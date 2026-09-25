"""UC3 Event-basierte Benachrichtigung (Broadcast) - Orchestrator.

Misst fuer S unabhaengige Subscriber (S aus einer Liste), ob jeder
Subscriber JEDE Nachricht erhaelt und wie sich Latenz und Sende-
verhalten mit wachsendem S veraendern.

Ablauf pro Lauf:
  1. Kafka: frisches Topic (1 Partition) anlegen
  2. S Subscriber starten; jeder legt seine eigene dauerhafte Subscription
     an (Kafka: eigene Consumer Group, RabbitMQ: eigene durable Queue am
     Fanout-Exchange, IBM MQ: eigene durable Subscription) und meldet
     "Bereit"
  3. Getakteter Producer (500 Nachrichten/s, wie UC1)
  4. Subscriber beenden sich, sobald sie alle Sequenznummern haben
  5. Auswertung, danach Aufraeumen (Topic, Queues, Subscriptions)

Kennzahlen:
  - Vollstaendigkeit PRO Subscriber (eindeutige seq), Duplikate
  - Latenz ueber alle Subscriber (Mittel, Median, P95, P99)
  - Spreizung der Subscriber-Mittelwerte (benachteiligt die Verteilung
    einzelne Subscriber?)
  - Versatz: fuer jede Nachricht Abstand zwischen fruehestem und
    spaetestem Empfang ueber alle Subscriber
  - tatsaechliche Senderate und Intervalle im Rueckstand

Nutzung:
    python run_uc3_measurement.py <kafka|rabbitmq|ibmmq|all> <nachrichten> <wiederholungen> <subscriber>

Beispiel:
    python run_uc3_measurement.py all 10000 5 1,2,4,8
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

VALID_TECHNOLOGIES = ["kafka", "rabbitmq", "ibmmq"]
KAFKA_BOOTSTRAP = "localhost:9092"
WARMUP_COUNT = 10

TECH_CONFIG = {
    "kafka": ("broadcast_kafka_producer.py", "broadcast_kafka_consumer.py"),
    "rabbitmq": ("broadcast_rabbitmq_producer.py", "broadcast_rabbitmq_consumer.py"),
    "ibmmq": ("broadcast_ibmmq_producer.py", "broadcast_ibmmq_consumer.py"),
}

READY_TIMEOUT_SECONDS = 60.0
SECONDS_PER_MESSAGE_TIMEOUT = 0.01
MIN_TIMEOUT_SECONDS = 120
RESULTS_JSONL = "results_uc3.jsonl"
RATE_PATTERN = re.compile(
    r"Tatsaechliche Rate:\s*(?P<rate>[\d.]+) Nachrichten/s.*?Rueckstand:\s*(?P<behind>\d+)"
)


# ---------------------------------------------------------------- Infrastruktur

def runmqsc(command):
    return subprocess.run(
        ["podman", "exec", "ibmmq", "bash", "-c", f"echo \"{command}\" | runmqsc QM1"],
        capture_output=True, text=True,
    ).stdout


def rabbitmqctl(*args):
    return subprocess.run(["podman", "exec", "rabbitmq", "rabbitmqctl", *args],
                          capture_output=True, text=True).stdout


def cleanup_orphans(technology):
    """Entfernt verwaiste Subscriptions frueherer, abgebrochener Laeufe. Sonst
    sammeln sie weiter Kopien jeder Nachricht und verfaelschen die Last."""
    if technology == "rabbitmq":
        for line in rabbitmqctl("list_queues", "name").splitlines():
            name = line.strip()
            if name.startswith("broadcast."):
                rabbitmqctl("delete_queue", name)
                print(f"  verwaiste Queue geloescht: {name}")
    elif technology == "ibmmq":
        for name in re.findall(r"SUB\((uc3-[^)]*)\)", runmqsc("DIS SUB('uc3-*')")):
            runmqsc(f"DELETE SUB('{name}')")
            print(f"  verwaiste Subscription geloescht: {name}")


def cleanup_run(technology, run_id, subscribers, topic):
    if technology == "kafka":
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        try:
            admin.delete_topics([topic])[topic].result(timeout=30)
        except Exception as e:
            print(f"  Hinweis: Topic {topic} nicht geloescht: {e}")
    elif technology == "rabbitmq":
        for i in range(1, subscribers + 1):
            rabbitmqctl("delete_queue", f"broadcast.{run_id}.{i}")
    elif technology == "ibmmq":
        for i in range(1, subscribers + 1):
            runmqsc(f"DELETE SUB('uc3-{run_id}-{i}')")


def kafka_create_topic(topic):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(timeout=30)
    deadline = time.time() + 30
    while time.time() < deadline:
        t = admin.list_topics(topic=topic, timeout=5).topics.get(topic)
        if t is not None and t.error is None and len(t.partitions) == 1:
            return
        time.sleep(0.2)
    raise RuntimeError(f"Topic {topic} nicht rechtzeitig verfuegbar.")


# ---------------------------------------------------------------- Prozesse

def start_reader_thread(proc):
    q = queue.Queue()

    def _reader():
        for line in iter(proc.stdout.readline, ""):
            q.put(line)
        q.put(None)

    threading.Thread(target=_reader, daemon=True).start()
    return q


def wait_all_ready(readers):
    ready = set()
    logs = {i: [] for i in readers}
    deadline = time.time() + READY_TIMEOUT_SECONDS
    while len(ready) < len(readers):
        if time.time() > deadline:
            raise TimeoutError(f"Nicht alle Subscriber bereit: {sorted(ready)}")
        for inst, q in readers.items():
            try:
                line = q.get(timeout=0.05)
            except queue.Empty:
                continue
            if line is None:
                raise EOFError(f"Subscriber {inst} beendet vor Start: {''.join(logs[inst])!r}")
            logs[inst].append(line)
            if "Bereit" in line:
                ready.add(inst)


def run_single(technology, message_count, subscribers, rep):
    producer_script, consumer_script = TECH_CONFIG[technology]
    run_id = f"{technology}-s{subscribers}-r{rep}-{int(time.time())}"
    topic = f"broadcast.{run_id}" if technology == "kafka" else None
    workdir = tempfile.mkdtemp(prefix=f"uc3_{run_id}_")

    env = os.environ.copy()
    env.update({
        "PYTHONUNBUFFERED": "1",
        "MEASURE_COUNT": str(message_count),
        "RUN_ID": run_id,
        "RESULT_DIR": workdir,
    })
    if topic:
        env["TOPIC"] = topic

    procs = {}
    try:
        if topic:
            kafka_create_topic(topic)

        readers = {}
        for inst in range(1, subscribers + 1):
            p = subprocess.Popen(
                [sys.executable, "-u", consumer_script],
                env=dict(env, INSTANCE_ID=str(inst)),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
            )
            procs[inst] = p
            readers[inst] = start_reader_thread(p)
        wait_all_ready(readers)
        time.sleep(0.5)

        timeout = max(MIN_TIMEOUT_SECONDS, message_count * SECONDS_PER_MESSAGE_TIMEOUT)
        prod = subprocess.run([sys.executable, "-u", producer_script], env=env,
                              capture_output=True, text=True, timeout=timeout)
        if prod.returncode != 0:
            raise RuntimeError(f"Producer fehlgeschlagen:\n{prod.stdout}{prod.stderr}")
        rate_match = RATE_PATTERN.search(prod.stdout)

        deadline = time.time() + timeout + 30
        for inst, p in procs.items():
            p.wait(timeout=max(1, deadline - time.time()))
            if p.returncode != 0:
                raise RuntimeError(f"Subscriber {inst} mit Exit-Code {p.returncode} beendet.")

        rows = []
        for inst in range(1, subscribers + 1):
            with open(os.path.join(workdir, f"{inst}.json")) as f:
                rows.append(json.load(f))
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
                p.wait()
        cleanup_run(technology, run_id, subscribers, topic)
        shutil.rmtree(workdir, ignore_errors=True)

    return evaluate(technology, run_id, message_count, subscribers, rows, rate_match)


# ---------------------------------------------------------------- Auswertung

def percentile(sorted_values, p):
    idx = min(int(round(p / 100 * (len(sorted_values) - 1))), len(sorted_values) - 1)
    return sorted_values[idx]


def evaluate(technology, run_id, message_count, subscribers, rows, rate_match):
    total = WARMUP_COUNT + message_count
    all_lat = []
    sub_means = []
    recv_by_seq = {}
    per_sub = []

    for r in rows:
        lat = [(rv - s) * 1000 for seq, s, rv in zip(r["seq"], r["sent"], r["recv"])
               if seq > WARMUP_COUNT]
        all_lat.extend(lat)
        if lat:
            sub_means.append(statistics.mean(lat))
        for seq, rv in zip(r["seq"], r["recv"]):
            if seq > WARMUP_COUNT:
                recv_by_seq.setdefault(seq, []).append(rv)
        per_sub.append({"instance": r["instance_id"], "unique": r["unique"],
                        "duplicates": r["duplicates"], "missing": total - r["unique"]})

    all_lat.sort()
    skew = sorted((max(v) - min(v)) * 1000 for v in recv_by_seq.values()
                  if len(v) == subscribers) if subscribers > 1 else []

    return {
        "technology": technology,
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message_count": message_count,
        "subscribers": subscribers,
        "complete": all(s["missing"] == 0 for s in per_sub),
        "per_subscriber": per_sub,
        "duplicates": sum(s["duplicates"] for s in per_sub),
        "missing": sum(s["missing"] for s in per_sub),
        "mean": statistics.mean(all_lat) if all_lat else None,
        "median": statistics.median(all_lat) if all_lat else None,
        "p95": percentile(all_lat, 95) if all_lat else None,
        "p99": percentile(all_lat, 99) if all_lat else None,
        "max": all_lat[-1] if all_lat else None,
        "sub_mean_spread": (max(sub_means) - min(sub_means)) if len(sub_means) > 1 else 0.0,
        "skew_mean": statistics.mean(skew) if skew else None,
        "skew_p99": percentile(skew, 99) if skew else None,
        "rate": float(rate_match.group("rate")) if rate_match else None,
        "behind": int(rate_match.group("behind")) if rate_match else None,
    }


def fmt(v, digits=2):
    return "-" if v is None else f"{v:.{digits}f}"


def write_report(all_results, message_count, repetitions, sub_list):
    path = f"uc3_bericht_{message_count}n_{int(time.time())}.md"
    with open(path, "w") as f:
        f.write("# UC3 Event-basierte Benachrichtigung - Messbericht\n\n")
        f.write(f"Erstellt: {datetime.now(timezone.utc).isoformat()}\n\n")
        f.write(f"Nachrichten pro Lauf: {message_count} (+{WARMUP_COUNT} Warmup), "
                f"Wiederholungen: {repetitions}, Subscriber: {', '.join(map(str, sub_list))}, "
                "Soll-Last: 500 Nachrichten/s\n\n")

        for technology, by_s in all_results.items():
            f.write(f"## {technology}\n\n### Einzellaeufe\n\n")
            f.write("| Subscriber | Lauf | Vollstaendig | Fehlend | Duplikate | Mittel (ms) | Median (ms) "
                    "| P99 (ms) | Max (ms) | Spreizung Sub.-Mittel (ms) | Versatz P99 (ms) "
                    "| Rate (msg/s) | Rueckstand |\n")
            f.write("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
            for s, results in by_s.items():
                for i, r in enumerate(results, 1):
                    f.write(f"| {s} | {i} | {'ja' if r['complete'] else 'NEIN'} | {r['missing']} | "
                            f"{r['duplicates']} | {fmt(r['mean'])} | {fmt(r['median'])} | "
                            f"{fmt(r['p99'])} | {fmt(r['max'])} | {fmt(r['sub_mean_spread'])} | "
                            f"{fmt(r['skew_p99'])} | {fmt(r['rate'], 1)} | "
                            f"{r['behind'] if r['behind'] is not None else '-'} |\n")

            f.write("\n### Zusammenfassung (nur vollstaendige Laeufe)\n\n")
            f.write("| Subscriber | Laeufe | Mittel (ms) | Stdabw Mittel (ms) | Median (ms) | P99 (ms) "
                    "| Versatz Mittel (ms) | Versatz P99 (ms) | mittl. Rueckstand |\n")
            f.write("|---|---|---|---|---|---|---|---|---|\n")
            for s, results in by_s.items():
                valid = [r for r in results if r["complete"] and r["mean"] is not None]
                if not valid:
                    f.write(f"| {s} | 0 | - | - | - | - | - | - | - |\n")
                    continue
                means = [r["mean"] for r in valid]
                sd = statistics.stdev(means) if len(means) > 1 else 0.0

                def avg(key):
                    vals = [r[key] for r in valid if r[key] is not None]
                    return statistics.mean(vals) if vals else None

                f.write(f"| {s} | {len(valid)} | {fmt(statistics.mean(means))} | {fmt(sd)} | "
                        f"{fmt(avg('median'))} | {fmt(avg('p99'))} | {fmt(avg('skew_mean'))} | "
                        f"{fmt(avg('skew_p99'))} | {fmt(avg('behind'), 0)} |\n")
            f.write("\n")

        f.write("Spreizung Sub.-Mittel = groesster minus kleinster Latenz-Mittelwert der Subscriber "
                "eines Laufs. Versatz = je Nachricht Abstand zwischen fruehestem und spaetestem Empfang "
                "ueber alle Subscriber. Latenzkennzahlen ohne Warmup-Nachrichten.\n")
    print(f"\nBericht geschrieben: {path}")


def main():
    if len(sys.argv) != 5 or sys.argv[1] not in VALID_TECHNOLOGIES + ["all"]:
        print("Nutzung: python run_uc3_measurement.py "
              f"<{'|'.join(VALID_TECHNOLOGIES)}|all> <nachrichten> <wiederholungen> <subscriber, z.B. 1,2,4,8>")
        sys.exit(1)

    technologies = VALID_TECHNOLOGIES if sys.argv[1] == "all" else [sys.argv[1]]
    message_count = int(sys.argv[2])
    repetitions = int(sys.argv[3])
    sub_list = sorted(int(x) for x in sys.argv[4].split(","))

    all_results = {}
    for technology in technologies:
        cleanup_orphans(technology)
        all_results[technology] = {}
        for s in sub_list:
            all_results[technology][s] = []
            for rep in range(1, repetitions + 1):
                print(f"[{technology}] {s} Subscriber, Lauf {rep}/{repetitions} ...")
                try:
                    r = run_single(technology, message_count, s, rep)
                except Exception as e:
                    print(f"[{technology}] Lauf abgebrochen: {type(e).__name__}: {e}")
                    continue
                with open(RESULTS_JSONL, "a") as f:
                    f.write(json.dumps(r) + "\n")
                status = "vollstaendig" if r["complete"] else f"FEHLEND: {r['missing']}"
                print(f"[{technology}] {s} Subscriber: Mittel={fmt(r['mean'])} ms, "
                      f"P99={fmt(r['p99'])} ms, Versatz P99={fmt(r['skew_p99'])} ms, "
                      f"Rueckstand={r['behind']}, {status}, Duplikate={r['duplicates']}")
                all_results[technology][s].append(r)

    write_report(all_results, message_count, repetitions, sub_list)


if __name__ == "__main__":
    main()