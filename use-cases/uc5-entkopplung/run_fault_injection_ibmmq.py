"""Use Case 4.2.4 - Orchestrator fuer die Fehlerinjektion "Consumer-Absturz"
mit IBM MQ. Strukturell identisch zu run_fault_injection.py (Kafka), nur
mit den IBM-MQ-Skripten.

Nutzung:
    python run_fault_injection_ibmmq.py <semantik>

WICHTIG: Vor jedem Lauf pruefen, dass DEV.QUEUE.2 leer ist:

    podman exec ibmmq bash -c "echo 'DIS QL(DEV.QUEUE.2) CURDEPTH' | runmqsc QM1"

Bei at-most-once ist KEIN Consumer-Neustart-Verlustnachweis noetig, der
Verlust entsteht hier bereits durch den Absturz selbst (siehe Kommentar in
zustellsemantik_ibmmq_consumer.py), der Neustart-Teil unten deckt trotzdem
den allgemeinen Ablauf einheitlich fuer alle drei Semantiken ab.
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
        print(f"Nutzung: python run_fault_injection_ibmmq.py <{'|'.join(VALID_SEMANTICS)}>")
        sys.exit(1)

    semantics = sys.argv[1]
    run_id = f"semtest-ibmmq-{semantics}-{int(time.time())}"
    print(f"run_id fuer diesen Lauf: {run_id}")

    consumer_proc = subprocess.Popen(
        ["python", "zustellsemantik_ibmmq_consumer.py", run_id, semantics]
    )
    time.sleep(1.0)

    producer_proc = subprocess.Popen(
        ["python", "zustellsemantik_ibmmq_producer.py"]
    )

    kill_delay = random.uniform(KILL_DELAY_MIN_SECONDS, KILL_DELAY_MAX_SECONDS)
    time.sleep(kill_delay)
    print(f"Sende SIGKILL an Consumer (PID {consumer_proc.pid}) nach {kill_delay:.2f}s")
    consumer_proc.send_signal(signal.SIGKILL)
    consumer_proc.wait()

    time.sleep(RESTART_GRACE_SECONDS)

    print("Starte Consumer neu, um verbleibende Nachrichten zu verarbeiten")
    restarted_consumer_proc = subprocess.Popen(
        ["python", "zustellsemantik_ibmmq_consumer.py", run_id, semantics]
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
