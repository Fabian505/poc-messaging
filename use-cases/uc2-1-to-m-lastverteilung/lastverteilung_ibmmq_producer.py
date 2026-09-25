"""Use Case 4.2.2.1 (Lastverteilung) - Producer (IBM MQ-Variante).

Befuellt DEV.QUEUE.2 VOR dem Start der Consumer (Vorbefuellung), Senderate
nicht Teil der Messung.

Persistenz explizit MQPER_PERSISTENT, konsistent zu RabbitMQ
delivery_mode=2. Ohne explizite Angabe gilt die Queue-Voreinstellung
(DEFPSIST), die bei den Entwickler-Queues typischerweise NO ist.
Put unter Syncpoint in Batches, damit die Vorbefuellung nicht pro
Nachricht einen Log-Schreibvorgang erzwingt.

Parameter per Umgebungsvariable: MESSAGE_COUNT.
"""

import json
import os
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
CONN_INFO = "localhost(1414)"
QUEUE_NAME = "DEV.QUEUE.2"
USER = "app"
PASSWORD = "app12345"
MESSAGE_COUNT = int(os.environ["MESSAGE_COUNT"])
COMMIT_BATCH = 500  # deutlich unter MAXUMSGS (Standard 10.000)


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, CONN_INFO, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)
    pmo = pymqi.PMO(Options=(
        pymqi.CMQC.MQPMO_SYNCPOINT
        | pymqi.CMQC.MQPMO_NEW_MSG_ID
        | pymqi.CMQC.MQPMO_FAIL_IF_QUIESCING
    ))

    for seq in range(1, MESSAGE_COUNT + 1):
        md = pymqi.MD()
        md.Persistence = pymqi.CMQC.MQPER_PERSISTENT
        queue.put(json.dumps({
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }).encode(), md, pmo)
        if seq % COMMIT_BATCH == 0:
            qmgr.commit()
    qmgr.commit()

    queue.close()
    qmgr.disconnect()
    print(f"Vorbefuellung fertig: {MESSAGE_COUNT} Nachrichten.")


if __name__ == "__main__":
    main()
