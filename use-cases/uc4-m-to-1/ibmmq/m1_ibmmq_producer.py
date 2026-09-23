"""Use Case 4.2.3 (m:1) - Producer (IBM MQ-Variante).

Mehrfach parallel mit unterschiedlicher producer_id starten, analog zu
m1_kafka_producer.py. Alle Instanzen senden an dieselbe Queue DEV.QUEUE.2.
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
MESSAGE_COUNT_PER_PRODUCER = 1_000

conn_info = f"{HOST}({PORT})"


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python m1_ibmmq_producer.py <run_id> <producer_id>")
        sys.exit(1)

    run_id, producer_id = sys.argv[1], sys.argv[2]

    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    print(f"[Producer {producer_id}] Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT_PER_PRODUCER + 1):
        payload = {
            "run_id": run_id,
            "producer_id": producer_id,
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        queue.put(json.dumps(payload).encode())

    queue.close()
    qmgr.disconnect()
    print(f"[Producer {producer_id}] Fertig: {MESSAGE_COUNT_PER_PRODUCER} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
