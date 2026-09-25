"""UC2 Lastverteilung - Orchestrator fuer komplette Messreihen.

Misst, wie schnell N parallele Consumer-Instanzen einen festen Bestand von
Nachrichten abarbeiten, fuer N aus einer vorgegebenen Liste, je Technologie
mit mehreren Wiederholungen.

Ablauf pro Lauf:
  1. Queue leeren (RabbitMQ, IBM MQ) bzw. frisches Topic anlegen (Kafka)
  2. Vorbefuellung durch den Producer, danach Pruefung der Queue-Tiefe
  3. N Consumer starten, warten bis alle bereit sind (Kafka: bis die
     Partitionszuweisung vollstaendig und stabil ist)
  4. Gemeinsames Startsignal (Datei), Consumer arbeiten den Bestand ab
  5. Ergebnisse einsammeln: Durchsatz, Verteilung, Vollstaendigkeit

Nutzung:
    python run_uc2_measurement.py <kafka|rabbitmq|ibmmq|all> <nachrichten> <wiederholungen> <instanzen>

Beispiel:
    python run_uc2_measurement.py all 100000 5 1,2,4,8
    PROCESSING_MS=2 python run_uc2_measurement.py kafka 20000 3 1,2,4,8

Kafka-Topics werden mit KAFKA_PARTITIONS Partitionen angelegt. Instanz-
anzahlen, die KAFKA_PARTITIONS nicht teilen, fuehren bei Kafka zwangslaeufig
zu ungleicher Verteilung (z.B. 3 Instanzen auf 8 Partitionen: 3/3/2).
"""

import json
import os
import queue
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
def script_path(name):
    return os.path.join(SCRIPT_DIR, name)

VALID_TECHNOLOGIES = ["kafka", "rabbitmq", "ibmmq"]
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_PARTITIONS = 8

TECH_CONFIG = {
    "kafka": {
        "producer_script": script_path("lastverteilung_kafka_producer.py"),
        "consumer_script": script_path("lastverteilung_kafka_consumer.py"),
    },
    "rabbitmq": {
        "producer_script": script_path("lastverteilung_rabbitmq_producer.py"),
        "consumer_script": script_path("lastverteilung_rabbitmq_consumer.py"),
        "clear_cmd": ["podman", "exec", "rabbitmq", "rabbitmqctl",
                      "purge_queue", "loadbalance.test"],
    },
    "ibmmq": {
        "producer_script": script_path("lastverteilung_ibmmq_producer.py"),
        "consumer_script": script_path("lastverteilung_ibmmq_consumer.py"),
        "clear_cmd": ["podman", "exec", "ibmmq", "bash", "-c",
                      "echo 'CLEAR QLOCAL(DEV.QUEUE.2)' | runmqsc QM1"],
    },
}

READY_TIMEOUT_SECONDS = 60.0
KAFKA_STABLE_SECONDS = 2.0
DEPTH_CHECK_TIMEOUT_SECONDS = 20.0
SECONDS_PER_MESSAGE_TIMEOUT = 0.01
MIN_TIMEOUT_SECONDS = 120
RESULTS_JSONL = "results_uc2.jsonl"


# ---------------------------------------------------------------- Kafka-Admin

def kafka_create_topic(topic):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=KAFKA_PARTITIONS, replication_factor=1)]
    )
    futures[topic].result(timeout=30)
    deadline = time.time() + 30
    while time.time() < deadline:  # warten, bis Metadaten alle Partitionen kennen
        md = admin.list_topics(topic=topic, timeout=5)
        t = md.topics.get(topic)
        if t is not None and t.error is None and len(t.partitions) == KAFKA_PARTITIONS:
            return
        time.sleep(0.2)
    raise RuntimeError(f"Topic {topic} nicht rechtzeitig verfuegbar.")


def kafka_delete_topic(topic):
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    try:
        admin.delete_topics([topic])[topic].result(timeout=30)
    except Exception as e:
        print(f"  Hinweis: Topic {topic} konnte nicht geloescht werden: {e}")


def kafka_topic_depth(topic):
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "uc2-depthcheck"})
    try:
        total = 0
        for p in range(KAFKA_PARTITIONS):
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            total += high - low
        return total
    finally:
        c.close()


# ---------------------------------------------------------------- Queue-Tiefe

def queue_depth(technology, topic):
    if technology == "kafka":
        return kafka_topic_depth(topic)
    if technology == "rabbitmq":
        out = subprocess.run(
            ["podman", "exec", "rabbitmq", "rabbitmqctl", "list_queues", "name", "messages"],
            capture_output=True, text=True,
        ).stdout
        for line in out.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == "loadbalance.test":
                return int(parts[1])
        return None
    if technology == "ibmmq":
        out = subprocess.run(
            ["podman", "exec", "ibmmq", "bash", "-c",
             "echo 'DIS QL(DEV.QUEUE.2) CURDEPTH' | runmqsc QM1"],
            capture_output=True, text=True,
        ).stdout
        m = re.search(r"CURDEPTH\((\d+)\)", out)
        return int(m.group(1)) if m else None
    raise ValueError(technology)


def wait_for_depth(technology, topic, expected):
    deadline = time.time() + DEPTH_CHECK_TIMEOUT_SECONDS
    depth = None
    while time.time() < deadline:
        depth = queue_depth(technology, topic)
        if depth == expected:
            return
        time.sleep(0.5)
    raise RuntimeError(f"Queue-Tiefe nach Vorbefuellung {depth}, erwartet {expected}. "
                       "Queue nicht geleert oder Vorbefuellung unvollstaendig.")


# ---------------------------------------------------------------- Prozesse

def start_reader_thread(proc):
    q = queue.Queue()

    def _reader():
        for line in iter(proc.stdout.readline, ""):
            q.put(line)
        q.put(None)

    threading.Thread(target=_reader, daemon=True).start()
    return q


def wait_until_ready(technology, readers, instances):
    """Wartet, bis alle Instanzen 'Bereit' gemeldet haben. Bei Kafka zusaetzlich,
    bis alle Partitionen verteilt sind und die Zuweisung stabil ist."""
    ready = set()
    assignment = {}
    last_change = time.time()
    logs = {i: [] for i in readers}
    deadline = time.time() + READY_TIMEOUT_SECONDS

    while time.time() < deadline:
        for inst, q in readers.items():
            while True:
                try:
                    line = q.get_nowait()
                except queue.Empty:
                    break
                if line is None:
                    raise EOFError(f"Instanz {inst} beendet vor Start: {''.join(logs[inst])!r}")
                logs[inst].append(line)
                if "Bereit" in line:
                    ready.add(inst)
                m = re.match(r"Zuweisung:\s*(\d+)", line)
                if m:
                    assignment[inst] = int(m.group(1))
                    last_change = time.time()

        if len(ready) == instances:
            if technology != "kafka":
                return
            active = min(instances, KAFKA_PARTITIONS)
            complete = (
                sum(assignment.values()) == KAFKA_PARTITIONS
                and sum(1 for v in assignment.values() if v > 0) == active
            )
            if complete and time.time() - last_change >= KAFKA_STABLE_SECONDS:
                return
        time.sleep(0.05)

    raise TimeoutError(f"Nicht alle Instanzen bereit (bereit: {sorted(ready)}, "
                       f"Zuweisung: {assignment}).")


def run_single(technology, message_count, instances, rep):
    config = TECH_CONFIG[technology]
    run_id = f"{technology}-n{instances}-r{rep}-{int(time.time())}"
    topic = f"loadbalance.{run_id}" if technology == "kafka" else None
    workdir = tempfile.mkdtemp(prefix=f"uc2_{run_id}_")
    start_flag = os.path.join(workdir, "START")
    result_dir = os.path.join(workdir, "results")
    os.makedirs(result_dir)

    env = os.environ.copy()
    env.update({
        "PYTHONUNBUFFERED": "1",
        "MESSAGE_COUNT": str(message_count),
        "RUN_ID": run_id,
        "START_FLAG": start_flag,
        "RESULT_DIR": result_dir,
    })
    if topic:
        env["TOPIC"] = topic
        env["PARTITIONS"] = str(KAFKA_PARTITIONS)

    consumers = {}
    try:
        # 1. Ausgangszustand herstellen
        if technology == "kafka":
            kafka_create_topic(topic)
        else:
            subprocess.run(config["clear_cmd"], check=False, capture_output=True)
            # WICHTIG: check=False allein reicht nicht, ein fehlgeschlagenes
            # clear_cmd (z.B. haengendes Handle von einem abgestuerzten
            # Prozess) blieb sonst unbemerkt und Nachrichten sammelten sich
            # ueber viele Laeufe hinweg an (beobachtet: DEV.QUEUE.2 lief bis
            # MAXDEPTH voll). Vor der Vorbefuellung explizit auf 0 pruefen.
            wait_for_depth(technology, topic, 0)
            # Zusaetzlich: MAXDEPTH selbst kann unabhaengig von der Tiefe
            # zurueckfallen (beobachtet 24.09.2026, Ursache ungeklaert - auch
            # WAEHREND einer laufenden Isolation moeglich, nicht nur bei
            # einem Neustart). Eine leere Queue mit zu niedrigem MAXDEPTH
            # besteht obige Pruefung, crasht aber trotzdem mit MQRC_Q_FULL.
            if technology == "ibmmq":
                out = subprocess.run(
                    ["podman", "exec", "ibmmq", "bash", "-c",
                     "echo 'DIS QL(DEV.QUEUE.2) MAXDEPTH' | runmqsc QM1"],
                    capture_output=True, text=True,
                ).stdout
                m = re.search(r"MAXDEPTH\((\d+)\)", out)
                maxdepth = int(m.group(1)) if m else None
                if maxdepth is None or maxdepth < message_count:
                    raise RuntimeError(
                        f"ibmmq: MAXDEPTH({maxdepth}) reicht nicht fuer {message_count} "
                        f"Nachrichten. 'ALTER QLOCAL(DEV.QUEUE.2) MAXDEPTH(1200000)' "
                        f"erneut setzen."
                    )

        # 2. Vorbefuellung
        prefill_timeout = max(MIN_TIMEOUT_SECONDS, message_count * SECONDS_PER_MESSAGE_TIMEOUT)
        prod = subprocess.run(
            [sys.executable, "-u", config["producer_script"]],
            env=env, capture_output=True, text=True, timeout=prefill_timeout,
        )
        if prod.returncode != 0:
            raise RuntimeError(f"Producer fehlgeschlagen:\n{prod.stdout}{prod.stderr}")
        wait_for_depth(technology, topic, message_count)

        # 3. Consumer starten und auf Bereitschaft warten
        readers = {}
        for inst in range(1, instances + 1):
            inst_env = dict(env, INSTANCE_ID=str(inst))
            p = subprocess.Popen(
                [sys.executable, "-u", config["consumer_script"]],
                env=inst_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            consumers[inst] = p
            readers[inst] = start_reader_thread(p)
        wait_until_ready(technology, readers, instances)

        # 4. Startsignal, dann auf Ende warten
        open(start_flag, "w").close()
        run_timeout = max(MIN_TIMEOUT_SECONDS, message_count * SECONDS_PER_MESSAGE_TIMEOUT) + 30
        deadline = time.time() + run_timeout
        for inst, p in consumers.items():
            p.wait(timeout=max(1, deadline - time.time()))
            if p.returncode != 0:
                raise RuntimeError(f"Instanz {inst} mit Exit-Code {p.returncode} beendet.")

        # 5. Ergebnisse einsammeln
        rows = []
        for inst in range(1, instances + 1):
            path = os.path.join(result_dir, f"{inst}.json")
            with open(path) as f:
                rows.append(json.load(f))
    finally:
        for p in consumers.values():
            if p.poll() is None:
                p.kill()
                p.wait()
        if topic:
            kafka_delete_topic(topic)
        shutil.rmtree(workdir, ignore_errors=True)

    return evaluate(technology, run_id, message_count, instances, rows)


def evaluate(technology, run_id, message_count, instances, rows):
    counts = [r["processed_count"] for r in rows]
    firsts = [r["first_message_at"] for r in rows if r["first_message_at"] is not None]
    lasts = [r["last_message_at"] for r in rows if r["last_message_at"] is not None]
    total = sum(counts)
    window = max(lasts) - min(firsts) if firsts else 0.0
    mean_count = statistics.mean(counts)
    cv = (statistics.pstdev(counts) / mean_count) if instances > 1 and mean_count else 0.0

    return {
        "technology": technology,
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message_count": message_count,
        "instances": instances,
        "processing_ms": float(os.environ.get("PROCESSING_MS", 0)),
        "per_instance": counts,
        "total_processed": total,
        "complete": total == message_count,
        "window_seconds": window,
        "throughput": total / window if window > 0 else 0.0,
        "distribution_cv": cv,
    }


# ---------------------------------------------------------------- Bericht

def write_report(all_results, message_count, repetitions, instance_list):
    processing_ms = float(os.environ.get("PROCESSING_MS", 0))
    path = f"uc2_bericht_{message_count}n_{int(time.time())}.md"
    with open(path, "w") as f:
        f.write("# UC2 Lastverteilung - Messbericht\n\n")
        f.write(f"Erstellt: {datetime.now(timezone.utc).isoformat()}\n\n")
        f.write(f"Nachrichten pro Lauf: {message_count}, Wiederholungen: {repetitions}, "
                f"Instanzen: {', '.join(map(str, instance_list))}, "
                f"simulierte Verarbeitung: {processing_ms} ms/Nachricht, "
                f"Kafka-Partitionen: {KAFKA_PARTITIONS}\n\n")

        for technology, by_n in all_results.items():
            f.write(f"## {technology}\n\n")
            f.write("### Einzellaeufe\n\n")
            f.write("| Instanzen | Lauf | Durchsatz (msg/s) | Fenster (s) | Verteilung | VK | Vollstaendig |\n")
            f.write("|---|---|---|---|---|---|---|\n")
            for n, results in by_n.items():
                for i, r in enumerate(results, 1):
                    f.write(f"| {n} | {i} | {r['throughput']:.1f} | {r['window_seconds']:.2f} | "
                            f"{'/'.join(map(str, r['per_instance']))} | "
                            f"{r['distribution_cv']:.3f} | "
                            f"{'ja' if r['complete'] else 'NEIN (' + str(r['total_processed']) + ')'} |\n")

            f.write("\n### Zusammenfassung (nur vollstaendige Laeufe)\n\n")
            f.write("| Instanzen | Laeufe | Mittel (msg/s) | Stdabw (msg/s) | Speedup | Effizienz | mittl. VK |\n")
            f.write("|---|---|---|---|---|---|---|\n")
            baseline = None
            for n, results in by_n.items():
                valid = [r for r in results if r["complete"]]
                if not valid:
                    f.write(f"| {n} | 0 | - | - | - | - | - |\n")
                    continue
                tps = [r["throughput"] for r in valid]
                mean_tp = statistics.mean(tps)
                sd = statistics.stdev(tps) if len(tps) > 1 else 0.0
                if baseline is None and n == min(by_n):
                    baseline = (n, mean_tp)
                if baseline:
                    speedup = mean_tp / baseline[1]
                    eff = speedup / (n / baseline[0])
                    sp, ef = f"{speedup:.2f}", f"{eff:.2f}"
                else:
                    sp = ef = "-"
                mean_cv = statistics.mean(r["distribution_cv"] for r in valid)
                f.write(f"| {n} | {len(valid)} | {mean_tp:.1f} | {sd:.1f} | {sp} | {ef} | {mean_cv:.3f} |\n")
            f.write("\n")

        f.write("VK = Variationskoeffizient der Nachrichten pro Instanz (0 = perfekt gleichmaessig). "
                f"Speedup bezogen auf die kleinste Instanzanzahl, Effizienz = Speedup / Instanzverhaeltnis.\n")
    print(f"\nBericht geschrieben: {path}")


def main():
    if len(sys.argv) != 5 or sys.argv[1] not in VALID_TECHNOLOGIES + ["all"]:
        print("Nutzung: python run_uc2_measurement.py "
              f"<{'|'.join(VALID_TECHNOLOGIES)}|all> <nachrichten> <wiederholungen> <instanzen, z.B. 1,2,4,8>")
        sys.exit(1)

    technologies = VALID_TECHNOLOGIES if sys.argv[1] == "all" else [sys.argv[1]]
    message_count = int(sys.argv[2])
    repetitions = int(sys.argv[3])
    instance_list = sorted(int(x) for x in sys.argv[4].split(","))

    if "kafka" in technologies:
        uneven = [n for n in instance_list if KAFKA_PARTITIONS % n != 0]
        if uneven:
            print(f"WARNUNG: {uneven} teilt {KAFKA_PARTITIONS} Partitionen nicht, "
                  "Kafka-Verteilung ist dort zwangslaeufig ungleich.")

    all_results = {}
    for technology in technologies:
        all_results[technology] = {}
        for n in instance_list:
            all_results[technology][n] = []
            for rep in range(1, repetitions + 1):
                print(f"[{technology}] {n} Instanz(en), Lauf {rep}/{repetitions} ...")
                try:
                    r = run_single(technology, message_count, n, rep)
                except Exception as e:
                    print(f"[{technology}] Lauf abgebrochen: {type(e).__name__}: {e}")
                    continue
                with open(RESULTS_JSONL, "a") as f:
                    f.write(json.dumps(r) + "\n")
                flag = "" if r["complete"] else f"  UNVOLLSTAENDIG: {r['total_processed']}"
                print(f"[{technology}] {n} Instanz(en): {r['throughput']:.1f} msg/s, "
                      f"Verteilung {'/'.join(map(str, r['per_instance']))}{flag}")
                all_results[technology][n].append(r)

    write_report(all_results, message_count, repetitions, instance_list)


if __name__ == "__main__":
    main()