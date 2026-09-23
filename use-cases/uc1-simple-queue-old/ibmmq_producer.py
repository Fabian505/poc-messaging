"""Use Case 1: Einfache Warteschlange - Producer (IBM MQ-Variante)."""

import json
import time
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
QUEUE_NAME = "DEV.QUEUE.1"
USER = "app"
PASSWORD = "app12345"
MESSAGE_COUNT = 10
DELAY_SECONDS = 0.5

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        queue.put(json.dumps(payload).encode())
        print(f"Gesendet: seq={seq}")
        time.sleep(DELAY_SECONDS)

    queue.close()
    qmgr.disconnect()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
