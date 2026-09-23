"""Use Case 4.2.2.1 (Lastverteilung) - Consumer (IBM MQ-Variante).

Fuer den Durchsatztest mehrfach parallel starten, z.B. fuer eine
Konfiguration mit 3 Instanzen in drei Terminals:

    python lastverteilung_ibmmq_consumer.py loadbalance-run-3 1
    python lastverteilung_ibmmq_consumer.py loadbalance-run-3 2
    python lastverteilung_ibmmq_consumer.py loadbalance-run-3 3

WICHTIG, Unterschied zu ibmmq_consumer.py aus Kapitel 6.1.1: dort wird ohne
Syncpoint gelesen, ein erfolgreicher get() entfernt die Nachricht sofort und
endgueltig aus der Queue. Das ist streng genommen at-most-once, stuerzt der
Consumer zwischen get() und Verarbeitung ab, ist die Nachricht verloren.

Fuer at-least-once wird hier stattdessen transaktional gelesen
(MQGMO_SYNCPOINT): eine Nachricht gilt erst nach explizitem qmgr.commit()
als final entnommen. Stuerzt der Consumer vorher ab, bleibt die Nachricht
in der Queue und wird bei einem Neustart oder von einer anderen Instanz
erneut zugestellt.

Erstes Argument: run_id nur zur Kennzeichnung in der Ergebnisdatei.
Zweites Argument: Instanz-ID, nur fuer die Log-Ausgabe.
"""

import json
import sys
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
QUEUE_NAME = "DEV.QUEUE.2"
USER = "app"
PASSWORD = "app12345"
RESULTS_FILE = "results_loadbalance.jsonl"
IDLE_TIMEOUT_MS = 10_000

conn_info = f"{HOST}({PORT})"


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python lastverteilung_ibmmq_consumer.py <run_id> <instanz_id>")
        sys.exit(1)

    run_id, instance_id = sys.argv[1], sys.argv[2]

    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    gmo = pymqi.GMO(
        Options=(
            pymqi.CMQC.MQGMO_WAIT
            | pymqi.CMQC.MQGMO_SYNCPOINT
            | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        ),
        WaitInterval=IDLE_TIMEOUT_MS,
    )

    processed_count = 0
    first_message_at = None

    print(f"[Instanz {instance_id}] Bereit, run_id '{run_id}'. Warte auf Nachrichten.")
    try:
        while True:
            try:
                message = queue.get(None, pymqi.md(), gmo)
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    break  # Idle-Timeout erreicht, Lauf gilt als beendet
                raise

            if first_message_at is None:
                first_message_at = datetime.now(timezone.utc)

            json.loads(message)  # Verarbeitung simuliert durch Parsen
            processed_count += 1

            # Erst nach erfolgreicher Verarbeitung committen (at-least-once)
            qmgr.commit()
    except KeyboardInterrupt:
        pass
    finally:
        queue.close()
        qmgr.disconnect()

    finished_at = datetime.now(timezone.utc)
    print(f"[Instanz {instance_id}] Fertig: {processed_count} Nachrichten verarbeitet.")

    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps({
            "technology": "ibmmq",
            "run_id": run_id,
            "instance_id": instance_id,
            "processed_count": processed_count,
            "first_message_at": first_message_at.isoformat() if first_message_at else None,
            "finished_at": finished_at.isoformat(),
        }) + "\n")


if __name__ == "__main__":
    main()
