"""Latenzmessung Kapitel 6.1.1 - Producer (IBM MQ-Variante, mit Verzoegerung).

Gleiche Verzoegerung wie bei kafka_producer_delayed.py, damit alle drei
Technologien mit identischer Methodik gemessen werden. Consumer bleibt
unveraendert (ibmmq_consumer.py).
"""

import json
import time
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
MEASURE_COUNT = 100
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT
DELAY_SECONDS = 0.05

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    for seq in range(1, TOTAL_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        queue.put(json.dumps(payload).encode())
        time.sleep(DELAY_SECONDS)

    queue.close()
    qmgr.disconnect()
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen), "
          f"mit {DELAY_SECONDS * 1000:.0f} ms Abstand.")


if __name__ == "__main__":
    main()
