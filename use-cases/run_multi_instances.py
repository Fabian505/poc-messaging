"""Generischer Launcher fuer mehrere Consumer-Instanzen (Lastverteilung
oder Event-basierte Benachrichtigung), startet danach automatisch den
Producer. Ersetzt das manuelle Oeffnen mehrerer Terminals.

Nutzung:
    python run_multi_instances.py <usecase> <technologie> <run_id> <anzahl_instanzen> [wartezeit_vor_producer]

<usecase>: lastverteilung | broadcast
<technologie>: kafka | rabbitmq | ibmmq
[wartezeit_vor_producer]: optional, Sekunden, Standard 3.0

Jede Consumer-Instanz schreibt ihre Konsolenausgabe in eine eigene Datei
(log_<usecase>_<technologie>_<run_id>_instanz<N>.txt), damit bei vielen
gleichzeitigen Instanzen nicht alles im selben Terminal vermischt wird.
Nach Abschluss aller Instanzen wird der passende Auswertungsbefehl
ausgegeben.

Beispiel, 8 Kafka-Consumer-Instanzen fuer Lastverteilung:
    python run_multi_instances.py lastverteilung kafka kafka-run-8 8
"""

import subprocess
import sys
import time

VALID_USECASES = {"lastverteilung", "broadcast"}
VALID_TECHNOLOGIES = {"kafka", "rabbitmq", "ibmmq"}

SCRIPT_NAMES = {
    "lastverteilung": {
        "kafka": ("lastverteilung_kafka_consumer.py", "lastverteilung_kafka_producer.py"),
        "rabbitmq": ("lastverteilung_rabbitmq_consumer.py", "lastverteilung_rabbitmq_producer.py"),
        "ibmmq": ("lastverteilung_ibmmq_consumer.py", "lastverteilung_ibmmq_producer.py"),
    },
    "broadcast": {
        "kafka": ("broadcast_kafka_consumer.py", "broadcast_kafka_producer.py"),
        "rabbitmq": ("broadcast_rabbitmq_consumer.py", "broadcast_rabbitmq_producer.py"),
        "ibmmq": ("broadcast_ibmmq_consumer.py", "broadcast_ibmmq_producer.py"),
    },
}

RESULTS_FILE = {
    "lastverteilung": "results_loadbalance.jsonl",
    "broadcast": "results_broadcast.jsonl",
}

ANALYZE_SCRIPT = {
    "lastverteilung": "analyze_loadbalance.py",
    "broadcast": "analyze_broadcast.py",
}


def main():
    if len(sys.argv) not in (5, 6):
        print(
            "Nutzung: python run_multi_instances.py <lastverteilung|broadcast> "
            "<kafka|rabbitmq|ibmmq> <run_id> <anzahl_instanzen> [wartezeit_vor_producer]"
        )
        sys.exit(1)

    usecase, technology, run_id = sys.argv[1], sys.argv[2], sys.argv[3]
    instance_count = int(sys.argv[4])
    wait_before_producer = float(sys.argv[5]) if len(sys.argv) == 6 else 3.0

    if usecase not in VALID_USECASES or technology not in VALID_TECHNOLOGIES:
        print(f"Ungueltige Kombination: {usecase} / {technology}")
        sys.exit(1)

    consumer_script, producer_script = SCRIPT_NAMES[usecase][technology]

    print(f"Starte {instance_count} Consumer-Instanzen ({consumer_script}), run_id={run_id} ...")
    consumer_procs = []
    log_files = []
    for instance_id in range(1, instance_count + 1):
        log_path = f"log_{usecase}_{technology}_{run_id}_instanz{instance_id}.txt"
        log_file = open(log_path, "w")
        log_files.append(log_file)
        proc = subprocess.Popen(
            ["python", consumer_script, run_id, str(instance_id)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        consumer_procs.append(proc)

    print(f"Warte {wait_before_producer}s, damit alle Instanzen verbunden/gebunden sind ...")
    time.sleep(wait_before_producer)

    print(f"Starte Producer ({producer_script}) ...")
    producer_proc = subprocess.Popen(["python", producer_script])
    producer_proc.wait()
    print("Producer fertig.")

    print("Warte auf Abschluss aller Consumer-Instanzen (Idle-Timeout) ...")
    for proc in consumer_procs:
        proc.wait()
    for log_file in log_files:
        log_file.close()

    print(f"\nAlle {instance_count} Instanzen abgeschlossen. Logs: log_{usecase}_{technology}_{run_id}_instanz*.txt")
    if usecase == "lastverteilung":
        print(f"Auswertung: python {ANALYZE_SCRIPT[usecase]} {RESULTS_FILE[usecase]} {run_id}")
    else:
        print(f"Auswertung: python {ANALYZE_SCRIPT[usecase]} {RESULTS_FILE[usecase]} {run_id} <erwartete_anzahl>")


if __name__ == "__main__":
    main()
