"""Generischer Launcher fuer den Use Case m:1: startet einen einzelnen
Consumer und danach mehrere Producer-Instanzen gleichzeitig. Ersetzt das
manuelle Oeffnen mehrerer Terminals.

Nutzung:
    python run_multi_producers.py <technologie> <run_id> <anzahl_producer>

<technologie>: kafka | rabbitmq | ibmmq

Jede Producer-Instanz schreibt ihre Konsolenausgabe in eine eigene Datei
(log_m1_<technologie>_<run_id>_producer<N>.txt).

Beispiel, 10 RabbitMQ-Producer:
    python run_multi_producers.py rabbitmq rabbitmq-run-10 10
"""

import subprocess
import sys
import time

VALID_TECHNOLOGIES = {"kafka", "rabbitmq", "ibmmq"}

SCRIPT_NAMES = {
    "kafka": ("m1_kafka_consumer.py", "m1_kafka_producer.py"),
    "rabbitmq": ("m1_rabbitmq_consumer.py", "m1_rabbitmq_producer.py"),
    "ibmmq": ("m1_ibmmq_consumer.py", "m1_ibmmq_producer.py"),
}

CONSUMER_STARTUP_WAIT_SECONDS = 2.0
CONSUMER_FINISH_GRACE_SECONDS = 12.0  # etwas ueber dem Idle-Timeout der Consumer


def main():
    if len(sys.argv) != 4:
        print("Nutzung: python run_multi_producers.py <kafka|rabbitmq|ibmmq> <run_id> <anzahl_producer>")
        sys.exit(1)

    technology, run_id = sys.argv[1], sys.argv[2]
    producer_count = int(sys.argv[3])

    if technology not in VALID_TECHNOLOGIES:
        print(f"Ungueltige Technologie: {technology}")
        sys.exit(1)

    consumer_script, producer_script = SCRIPT_NAMES[technology]

    print(f"Starte Consumer ({consumer_script}), run_id={run_id} ...")
    consumer_log = open(f"log_m1_{technology}_{run_id}_consumer.txt", "w")
    consumer_proc = subprocess.Popen(
        ["python", consumer_script, run_id],
        stdout=consumer_log,
        stderr=subprocess.STDOUT,
    )

    print(f"Warte {CONSUMER_STARTUP_WAIT_SECONDS}s, damit der Consumer bereit ist ...")
    time.sleep(CONSUMER_STARTUP_WAIT_SECONDS)

    print(f"Starte {producer_count} Producer-Instanzen ({producer_script}) gleichzeitig ...")
    producer_procs = []
    log_files = []
    for producer_id in range(1, producer_count + 1):
        log_path = f"log_m1_{technology}_{run_id}_producer{producer_id}.txt"
        log_file = open(log_path, "w")
        log_files.append(log_file)
        proc = subprocess.Popen(
            ["python", producer_script, run_id, str(producer_id)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        producer_procs.append(proc)

    print("Warte auf Abschluss aller Producer-Instanzen ...")
    for proc in producer_procs:
        proc.wait()
    for log_file in log_files:
        log_file.close()
    print("Alle Producer fertig.")

    print(f"Warte {CONSUMER_FINISH_GRACE_SECONDS}s, damit der Consumer per Idle-Timeout selbst beendet ...")
    time.sleep(CONSUMER_FINISH_GRACE_SECONDS)
    if consumer_proc.poll() is None:
        print("Consumer laeuft noch, wird beendet.")
        consumer_proc.terminate()
    consumer_proc.wait()
    consumer_log.close()

    print(f"\nLauf abgeschlossen. Logs: log_m1_{technology}_{run_id}_*.txt")
    print(f"Auswertung: python analyze_m1.py results_m1.jsonl {run_id} {producer_count} <nachrichten_pro_producer>")


if __name__ == "__main__":
    main()
