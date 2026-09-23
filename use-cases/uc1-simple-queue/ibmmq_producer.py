"""Latenzmessung Kapitel 6.1.1 - Producer (IBM MQ-Variante).

WICHTIG: Consumer VORHER starten und aktiv warten lassen.
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
WARMUP_COUNT = 10
MEASURE_COUNT = 1_000_000
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, TOTAL_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        queue.put(json.dumps(payload).encode())

    queue.close()
    qmgr.disconnect()
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen).")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
