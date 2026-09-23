"""Use Case 4.2.4 - Orchestrator fuer die Fehlerinjektion "Consumer-Absturz"
mit RabbitMQ. Strukturell identisch zu run_fault_injection.py (Kafka),
nur mit den RabbitMQ-Skripten.

Nutzung:
    python run_fault_injection_rabbitmq.py <semantik>

WICHTIG: Vor jedem Lauf pruefen, dass die Queue 'semantics.test' leer ist:

    podman exec rabbitmq rabbitmqctl list_queues name messages
"""

import random
import signal
import subprocess
import sys
import time

VALID_SEMANTICS = {"at-most-once", "at-least-once", "exactly-once"}
KILL_DELAY_MIN_SECONDS = 0.5
KILL_DELAY_MAX_SECONDS = 2.0
RESTART_GRACE_SECONDS = 1.0
FINAL_WAIT_SECONDS = 15.0


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in VALID_SEMANTICS:
        print(f"Nutzung: python run_fault_injection_rabbitmq.py <{'|'.join(VALID_SEMANTICS)}>")
        sys.exit(1)

    semantics = sys.argv[1]
    run_id = f"semtest-rabbitmq-{semantics}-{int(time.time())}"
    print(f"run_id fuer diesen Lauf: {run_id}")

    consumer_proc = subprocess.Popen(
        ["python", "zustellsemantik_rabbitmq_consumer.py", run_id, semantics]
    )
    time.sleep(1.0)

    producer_proc = subprocess.Popen(
        ["python", "zustellsemantik_rabbitmq_producer.py"]
    )

    kill_delay = random.uniform(KILL_DELAY_MIN_SECONDS, KILL_DELAY_MAX_SECONDS)
    time.sleep(kill_delay)
    print(f"Sende SIGKILL an Consumer (PID {consumer_proc.pid}) nach {kill_delay:.2f}s")
    consumer_proc.send_signal(signal.SIGKILL)
    consumer_proc.wait()

    time.sleep(RESTART_GRACE_SECONDS)

    print("Starte Consumer neu, um verbleibende Nachrichten zu verarbeiten")
    restarted_consumer_proc = subprocess.Popen(
        ["python", "zustellsemantik_rabbitmq_consumer.py", run_id, semantics]
    )

    producer_proc.wait()
    print("Producer fertig, warte auf restlichen Consumer-Durchlauf...")
    time.sleep(FINAL_WAIT_SECONDS)
    restarted_consumer_proc.terminate()
    restarted_consumer_proc.wait()

    print(f"Fehlerinjektion abgeschlossen. run_id: {run_id}")
    print(f"Auswertung: python analyze_semantics.py results_semantics.jsonl {run_id} 200")


if __name__ == "__main__":
    main()
