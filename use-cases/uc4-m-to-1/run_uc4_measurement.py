"""UC4 m:1 - Orchestrator fuer komplette Messreihen.

M Producer senden gleichzeitig an EINEN Consumer. Geprueft werden
(Kapitel 4.2.3): kein Verlust, Reihenfolge JE Producer erhalten. Eine
globale Reihenfolge ueber alle Producer wird bewusst nicht bewertet.
Zusaetzlich: Latenz und Fairness zwischen den Producern.

Lastmodell (siehe m1_common.py): Gesamtlast konstant 500 Nachrichten/s,
unabhaengig von M, die Producer senden versetzt und lueckenlos
ineinandergreifend. <nachrichten> ist die GESAMTzahl pro Lauf und wird
gleichmaessig auf die M Producer verteilt, jeder Lauf dauert also gleich
lang.

Ablauf pro Lauf:
  1. Queue leeren (RabbitMQ, IBM MQ) bzw. frisches Topic (Kafka, 1 Partition)
  2. Consumer starten, auf Bereitschaft warten
  3. M Producer starten, auf Bereitschaft warten
  4. Gemeinsamer absoluter Startzeitpunkt, Producer senden nach Fahrplan
  5. Consumer endet, sobald alle (Producer, seq) eingetroffen sind

Nutzung:
    python run_uc4_measurement.py <kafka|rabbitmq|ibmmq|all> <nachrichten_gesamt> <wiederholungen> <producer>

Beispiel:
    python run_uc4_measurement.py all 10000 5 1,2,4,8
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
WARMUP_COUNT = 10

TECH_CONFIG = {
    "kafka": {"producer": script_path("m1_kafka_producer.py"), "consumer": script_path("m1_kafka_consumer.py")},
    "rabbitmq": {
        "producer": script_path("m1_rabbitmq_producer.py"), "consumer": script_path("m1_rabbitmq_consumer.py"),
        "clear_cmd": ["podman", "exec", "rabbitmq", "rabbitmqctl", "purge_queue", "aggregation.test"],
    },
    "ibmmq": {
        "producer": script_path("m1_ibmmq_producer.py"), "consumer": script_path("m1_ibmmq_consumer.py"),
        "clear_cmd": ["podman", "exec", "ibmmq", "bash", "-c",
                      "echo 'CLEAR QLOCAL(DEV.QUEUE.2)' | runmqsc QM1"],
    },
}

READY_TIMEOUT_SECONDS = 60.0
START_DELAY_SECONDS = 1.0
MIN_TIMEOUT_SECONDS = 120
RESULTS_JSONL = "results_uc4.jsonl"
RATE_PATTERN = re.compile(
    r"Tatsaechliche Rate:\s*(?P<rate>[\d.]+) Nachrichten/s.*?Rueckstand:\s*(?P<behind>\d+)"
)


# ---------------------------------------------------------------- Infrastruktur

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


def kafka_delete_topic(topic):
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    try:
        admin.delete_topics([topic])[topic].result(timeout=30)
    except Exception as e:
        print(f"  Hinweis: Topic {topic} nicht geloescht: {e}")


def start_reader_thread(proc):
    q = queue.Queue()

    def _reader():
        for line in iter(proc.stdout.readline, ""):
            q.put(line)
        q.put(None)

    threading.Thread(target=_reader, daemon=True).start()
    return q


def wait_all_ready(readers, label):
    ready = set()
    logs = {k: [] for k in readers}
    deadline = time.time() + READY_TIMEOUT_SECONDS
    while len(ready) < len(readers):
        if time.time() > deadline:
            raise TimeoutError(f"{label}: nicht alle bereit ({sorted(ready)})")
        for key, q in readers.items():
            if key in ready:
                continue
            try:
                line = q.get(timeout=0.02)
            except queue.Empty:
                continue
            if line is None:
                raise EOFError(f"{label} {key} beendet vor Start: {''.join(logs[key])!r}")
            logs[key].append(line)
            if "Bereit" in line:
                ready.add(key)


def spawn(script, env):
    p = subprocess.Popen([sys.executable, "-u", script], env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    return p, start_reader_thread(p)


def drain(q):
    lines = []
    while True:
        try:
            line = q.get_nowait()
        except queue.Empty:
            return "".join(lines)
        if line is None:
            return "".join(lines)
        lines.append(line)


# ---------------------------------------------------------------- Lauf

def verified_empty(technology):
    if technology == "rabbitmq":
        out = subprocess.run(
            ["podman", "exec", "rabbitmq", "rabbitmqctl", "list_queues", "name", "messages"],
            capture_output=True, text=True,
        ).stdout
        for line in out.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == "aggregation.test":
                return int(parts[1]) == 0
        return True
    if technology == "ibmmq":
        out = subprocess.run(
            ["podman", "exec", "ibmmq", "bash", "-c",
             "echo 'DIS QL(DEV.QUEUE.2) CURDEPTH' | runmqsc QM1"],
            capture_output=True, text=True,
        ).stdout
        m = re.search(r"CURDEPTH\((\d+)\)", out)
        return m is not None and int(m.group(1)) == 0
    return True


def run_single(technology, total_messages, producers, rep):
    config = TECH_CONFIG[technology]
    per_producer = total_messages // producers
    run_id = f"{technology}-m{producers}-r{rep}-{int(time.time())}"
    topic = f"aggregation.{run_id}" if technology == "kafka" else None
    workdir = tempfile.mkdtemp(prefix=f"uc4_{run_id}_")
    start_flag = os.path.join(workdir, "START")

    env = os.environ.copy()
    env.update({
        "PYTHONUNBUFFERED": "1",
        "RUN_ID": run_id,
        "RESULT_DIR": workdir,
        "START_FLAG": start_flag,
        "PRODUCER_COUNT": str(producers),
        "MEASURE_COUNT": str(per_producer),
    })
    if topic:
        env["TOPIC"] = topic

    procs = []
    try:
        if topic:
            kafka_create_topic(topic)
        else:
            subprocess.run(config["clear_cmd"], check=False, capture_output=True)
            # WICHTIG: check=False allein reicht nicht, ein fehlgeschlagenes
            # clear_cmd blieb sonst unbemerkt und Nachrichten sammelten sich
            # ueber viele Laeufe hinweg an (beobachtet bei UC2: DEV.QUEUE.2
            # lief bis MAXDEPTH voll). Explizit verifizieren statt vertrauen.
            if not verified_empty(technology):
                raise RuntimeError(
                    f"{technology}: Queue nach clear_cmd nicht leer. Vermutlich "
                    f"ein haengendes Handle von einem abgestuerzten Prozess. "
                    f"'podman restart {technology}' und erneut versuchen."
                )
            if technology == "ibmmq":
                # Zusaetzlich zur Leer-Pruefung: MAXDEPTH kann unabhaengig
                # davon zurueckfallen (beobachtet 24.09.2026, Ursache
                # ungeklaert). Leere Queue mit zu niedrigem MAXDEPTH besteht
                # die obige Pruefung, crasht aber trotzdem mit MQRC_Q_FULL.
                out = subprocess.run(
                    ["podman", "exec", "ibmmq", "bash", "-c",
                     "echo 'DIS QL(DEV.QUEUE.2) MAXDEPTH' | runmqsc QM1"],
                    capture_output=True, text=True,
                ).stdout
                m = re.search(r"MAXDEPTH\((\d+)\)", out)
                maxdepth = int(m.group(1)) if m else None
                needed = producers * (per_producer + WARMUP_COUNT)
                if maxdepth is None or maxdepth < needed:
                    raise RuntimeError(
                        f"ibmmq: MAXDEPTH({maxdepth}) reicht nicht fuer {needed} "
                        f"Nachrichten. 'ALTER QLOCAL(DEV.QUEUE.2) MAXDEPTH(1200000)' "
                        f"erneut setzen."
                    )

        consumer, consumer_q = spawn(config["consumer"], env)
        procs.append(consumer)
        wait_all_ready({"Consumer": consumer_q}, "Consumer")

        prod = {}
        for pid in range(1, producers + 1):
            p, q = spawn(config["producer"], dict(env, PRODUCER_ID=str(pid)))
            procs.append(p)
            prod[pid] = (p, q)
        wait_all_ready({pid: q for pid, (_, q) in prod.items()}, "Producer")

        # Absoluter Startzeitpunkt, atomar geschrieben (erst Datei, dann umbenennen)
        tmp = start_flag + ".tmp"
        with open(tmp, "w") as f:
            f.write(f"{time.time() + START_DELAY_SECONDS:.6f}")
        os.rename(tmp, start_flag)

        timeout = max(MIN_TIMEOUT_SECONDS, total_messages * 0.01)
        deadline = time.time() + timeout
        rates, behind = [], 0
        for pid, (p, q) in prod.items():
            p.wait(timeout=max(1, deadline - time.time()))
            out = drain(q)
            if p.returncode != 0:
                raise RuntimeError(f"Producer {pid} mit Exit-Code {p.returncode}:\n{out}")
            m = RATE_PATTERN.search(out)
            if m:
                rates.append(float(m.group("rate")))
                behind += int(m.group("behind"))

        consumer.wait(timeout=max(1, deadline + 30 - time.time()))
        if consumer.returncode != 0:
            raise RuntimeError(f"Consumer mit Exit-Code {consumer.returncode}:\n{drain(consumer_q)}")
        with open(os.path.join(workdir, "consumer.json")) as f:
            result = json.load(f)
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
                p.wait()
        if topic:
            kafka_delete_topic(topic)
        shutil.rmtree(workdir, ignore_errors=True)

    return evaluate(technology, run_id, total_messages, producers, per_producer,
                    result, rates, behind)


# ---------------------------------------------------------------- Auswertung

def percentile(sorted_values, p):
    idx = min(int(round(p / 100 * (len(sorted_values) - 1))), len(sorted_values) - 1)
    return sorted_values[idx]


def evaluate(technology, run_id, total_messages, producers, per_producer, result, rates, behind):
    expected = WARMUP_COUNT + per_producer
    by_pid = {pid: [] for pid in range(1, producers + 1)}
    for pid, seq, sent, recv in result["arrivals"]:
        by_pid.setdefault(int(pid), []).append((seq, sent, recv))

    per_prod = []
    all_lat = []
    prod_means = []
    for pid, items in sorted(by_pid.items()):
        # Reihenfolge je Producer: Ankunft mit kleinerer seq als eine zuvor
        # angekommene seq desselben Producers zaehlt als Verletzung
        violations, max_seq = 0, 0
        for seq, _, _ in items:
            if seq < max_seq:
                violations += 1
            max_seq = max(max_seq, seq)
        lat = [(recv - sent) * 1000 for seq, sent, recv in items if seq > WARMUP_COUNT]
        all_lat.extend(lat)
        if lat:
            prod_means.append(statistics.mean(lat))
        per_prod.append({
            "producer": pid,
            "unique": len(items),
            "missing": expected - len(items),
            "duplicates": result["duplicates"].get(str(pid), 0),
            "order_violations": violations,
        })

    all_lat.sort()
    return {
        "technology": technology,
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_messages": total_messages,
        "producers": producers,
        "per_producer": per_prod,
        "complete": all(p["missing"] == 0 for p in per_prod),
        "missing": sum(p["missing"] for p in per_prod),
        "duplicates": sum(p["duplicates"] for p in per_prod),
        "order_violations": sum(p["order_violations"] for p in per_prod),
        "mean": statistics.mean(all_lat) if all_lat else None,
        "median": statistics.median(all_lat) if all_lat else None,
        "p95": percentile(all_lat, 95) if all_lat else None,
        "p99": percentile(all_lat, 99) if all_lat else None,
        "max": all_lat[-1] if all_lat else None,
        "producer_mean_spread": (max(prod_means) - min(prod_means)) if len(prod_means) > 1 else 0.0,
        "total_rate": sum(rates) if rates else None,
        "behind": behind,
    }


def fmt(v, digits=2):
    return "-" if v is None else f"{v:.{digits}f}"


def write_report(all_results, total_messages, repetitions, prod_list):
    path = f"uc4_bericht_{total_messages}n_{int(time.time())}.md"
    with open(path, "w") as f:
        f.write("# UC4 m:1 - Messbericht\n\n")
        f.write(f"Erstellt: {datetime.now(timezone.utc).isoformat()}\n\n")
        f.write(f"Nachrichten gesamt pro Lauf: {total_messages} (verteilt auf die Producer, "
                f"je +{WARMUP_COUNT} Warmup), Wiederholungen: {repetitions}, "
                f"Producer: {', '.join(map(str, prod_list))}, "
                "Gesamtlast konstant 500 Nachrichten/s\n\n")

        for technology, by_m in all_results.items():
            f.write(f"## {technology}\n\n### Einzellaeufe\n\n")
            f.write("| Producer | Lauf | Vollstaendig | Fehlend | Duplikate | Reihenfolge-Verletzungen "
                    "| Mittel (ms) | Median (ms) | P99 (ms) | Max (ms) | Spreizung Prod.-Mittel (ms) "
                    "| Gesamtrate (msg/s) | Rueckstand |\n")
            f.write("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
            for m, results in by_m.items():
                for i, r in enumerate(results, 1):
                    f.write(f"| {m} | {i} | {'ja' if r['complete'] else 'NEIN'} | {r['missing']} | "
                            f"{r['duplicates']} | {r['order_violations']} | {fmt(r['mean'])} | "
                            f"{fmt(r['median'])} | {fmt(r['p99'])} | {fmt(r['max'])} | "
                            f"{fmt(r['producer_mean_spread'])} | {fmt(r['total_rate'], 1)} | "
                            f"{r['behind']} |\n")

            f.write("\n### Zusammenfassung (nur vollstaendige Laeufe)\n\n")
            f.write("| Producer | Laeufe | Mittel (ms) | Stdabw Mittel (ms) | Median (ms) | P99 (ms) "
                    "| Reihenfolge-Verletzungen gesamt | mittl. Rueckstand |\n")
            f.write("|---|---|---|---|---|---|---|---|\n")
            for m, results in by_m.items():
                valid = [r for r in results if r["complete"] and r["mean"] is not None]
                if not valid:
                    f.write(f"| {m} | 0 | - | - | - | - | - | - |\n")
                    continue
                means = [r["mean"] for r in valid]
                sd = statistics.stdev(means) if len(means) > 1 else 0.0
                f.write(f"| {m} | {len(valid)} | {fmt(statistics.mean(means))} | {fmt(sd)} | "
                        f"{fmt(statistics.mean(r['median'] for r in valid))} | "
                        f"{fmt(statistics.mean(r['p99'] for r in valid))} | "
                        f"{sum(r['order_violations'] for r in valid)} | "
                        f"{fmt(statistics.mean(r['behind'] for r in valid), 0)} |\n")
            f.write("\n")

        f.write("Reihenfolge-Verletzung = eine Nachricht kommt mit kleinerer Sequenznummer an als eine "
                "zuvor angekommene Nachricht DESSELBEN Producers. Globale Reihenfolge ueber Producer "
                "hinweg wird nicht bewertet. Spreizung Prod.-Mittel = groesster minus kleinster "
                "Latenz-Mittelwert der Producer eines Laufs (Fairness).\n")
    print(f"\nBericht geschrieben: {path}")


def main():
    if len(sys.argv) != 5 or sys.argv[1] not in VALID_TECHNOLOGIES + ["all"]:
        print("Nutzung: python run_uc4_measurement.py "
              f"<{'|'.join(VALID_TECHNOLOGIES)}|all> <nachrichten_gesamt> <wiederholungen> <producer, z.B. 1,2,4,8>")
        sys.exit(1)

    technologies = VALID_TECHNOLOGIES if sys.argv[1] == "all" else [sys.argv[1]]
    total_messages = int(sys.argv[2])
    repetitions = int(sys.argv[3])
    prod_list = sorted(int(x) for x in sys.argv[4].split(","))
    bad = [m for m in prod_list if total_messages % m]
    if bad:
        print(f"FEHLER: {total_messages} ist nicht durch {bad} teilbar.")
        sys.exit(1)

    all_results = {}
    for technology in technologies:
        all_results[technology] = {}
        for m in prod_list:
            all_results[technology][m] = []
            for rep in range(1, repetitions + 1):
                print(f"[{technology}] {m} Producer, Lauf {rep}/{repetitions} ...")
                try:
                    r = run_single(technology, total_messages, m, rep)
                except Exception as e:
                    print(f"[{technology}] Lauf abgebrochen: {type(e).__name__}: {e}")
                    continue
                with open(RESULTS_JSONL, "a") as f:
                    f.write(json.dumps(r) + "\n")
                status = "vollstaendig" if r["complete"] else f"FEHLEND: {r['missing']}"
                print(f"[{technology}] {m} Producer: Mittel={fmt(r['mean'])} ms, P99={fmt(r['p99'])} ms, "
                      f"Reihenfolge-Verletzungen={r['order_violations']}, Rate={fmt(r['total_rate'], 1)}, "
                      f"Rueckstand={r['behind']}, {status}")
                all_results[technology][m].append(r)

    write_report(all_results, total_messages, repetitions, prod_list)


if __name__ == "__main__":
    main()