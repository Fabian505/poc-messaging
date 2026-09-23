"""Use Case 4.2.3 (m:1) - Consumer (IBM MQ-Variante).

Nur EINE Instanz starten. at-least-once ueber MQGMO_SYNCPOINT und
qmgr.commit() nach Verarbeitung, analog zu lastverteilung_ibmmq_consumer.py.
Protokolliert jede Nachricht roh in results_m1.jsonl.
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
RESULTS_FILE = "results_m1.jsonl"
IDLE_TIMEOUT_MS = 10_000

conn_info = f"{HOST}({PORT})"


def main():
    if len(sys.argv) != 2:
        print("Nutzung: python m1_ibmmq_consumer.py <run_id>")
        sys.exit(1)

    run_id = sys.argv[1]

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

    received_count = 0
    print(f"Bereit, run_id '{run_id}'. Warte auf Nachrichten von allen Producern.")

    with open(RESULTS_FILE, "a") as f:
        try:
            while True:
                try:
                    message = queue.get(None, pymqi.md(), gmo)
                except pymqi.MQMIError as e:
                    if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        break
                    raise

                payload = json.loads(message)
                received_at = datetime.now(timezone.utc).isoformat()

                f.write(json.dumps({
                    "technology": "ibmmq",
                    "run_id": payload["run_id"],
                    "producer_id": payload["producer_id"],
                    "seq": payload["seq"],
                    "sent_at": payload["sent_at"],
                    "received_at": received_at,
                }) + "\n")
                received_count += 1

                qmgr.commit()
        except KeyboardInterrupt:
            pass
        finally:
            queue.close()
            qmgr.disconnect()

    print(f"Fertig: {received_count} Nachrichten insgesamt empfangen.")


if __name__ == "__main__":
    main()
