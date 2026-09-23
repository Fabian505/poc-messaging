"""Use Case 4.2.4 - Orchestrator fuer die Fehlerinjektion "Consumer-Absturz".

Startet Consumer und Producer als Subprozesse, wartet eine kurze,
zufaellige Zeit und schickt dem Consumer dann SIGKILL, um einen harten
Absturz waehrend der Verarbeitung zu simulieren. Startet danach einen
frischen Consumer-Prozess (gleiche run_id, gleiche Semantik), der die
Verarbeitung fortsetzt, so wie es nach einem echten Absturz und Neustart
der Fall waere.

Nutzung:
    python run_fault_injection.py <semantik>

Nach dem Lauf mit analyze_semantics.py auswerten: bei at-most-once werden
fehlende seq-Werte erwartet (das ist das erwartete, nicht das fehlerhafte
Verhalten dieser Semantik), bei at-least-once werden Duplikate erwartet,
bei exactly-once sollte weder etwas fehlen noch dupliziert verarbeitet
werden (Duplikate koennen ankommen, muessen aber laut Log als
duplicate_skipped=true erkannt worden sein).

WICHTIG: Vor jedem Lauf frische run_id verwenden (wird hier automatisch aus
der Startzeit generiert).
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
        print(f"Nutzung: python run_fault_injection.py <{'|'.join(VALID_SEMANTICS)}>")
        sys.exit(1)

    semantics = sys.argv[1]
    run_id = f"semtest-{semantics}-{int(time.time())}"
    print(f"run_id fuer diesen Lauf: {run_id}")

    # Erster Consumer-Prozess starten
    consumer_proc = subprocess.Popen(
        ["python", "zustellsemantik_kafka_consumer.py", run_id, semantics]
    )
    time.sleep(2.0)  # Zeit zum Joinen der Consumer Group geben

    # Producer starten
    producer_proc = subprocess.Popen(
        ["python", "zustellsemantik_kafka_producer.py", semantics]
    )

    # Nach kurzer, zufaelliger Verzoegerung den Consumer hart beenden
    kill_delay = random.uniform(KILL_DELAY_MIN_SECONDS, KILL_DELAY_MAX_SECONDS)
    time.sleep(kill_delay)
    print(f"Sende SIGKILL an Consumer (PID {consumer_proc.pid}) nach {kill_delay:.2f}s")
    consumer_proc.send_signal(signal.SIGKILL)
    consumer_proc.wait()

    time.sleep(RESTART_GRACE_SECONDS)

    # Frischen Consumer-Prozess starten, der den Rest verarbeitet
    print("Starte Consumer neu, um verbleibende Nachrichten zu verarbeiten")
    restarted_consumer_proc = subprocess.Popen(
        ["python", "zustellsemantik_kafka_consumer.py", run_id, semantics]
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
