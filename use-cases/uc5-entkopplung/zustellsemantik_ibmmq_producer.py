"""Use Case 4.2.4 (Entkopplung bei Ausfall und Zustellsemantiken)
- Producer (IBM MQ-Variante).

Bleibt bewusst unveraendert ueber alle drei Semantiken hinweg, der
Unterschied liegt komplett auf Consumer-Seite. Nutzung:

    python zustellsemantik_ibmmq_producer.py
"""

import json
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
QUEUE_NAME = "DEV.QUEUE.2"
USER = "app"
PASSWORD = "app12345"
MESSAGE_COUNT = 200

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        queue.put(json.dumps(payload).encode())

    queue.close()
    qmgr.disconnect()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
