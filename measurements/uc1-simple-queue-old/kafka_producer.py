"""Use Case 1: Einfache Warteschlange - Producer (Kafka-Variante).

Kafka arbeitet intern mit einem Log statt einer klassischen Queue, aber
fuer einen einzelnen Consumer in einer Consumer Group verhaelt sich das
fuer diesen Use Case aequivalent zu einer Point-to-Point-Queue.
"""

import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "uc1.simple_queue"
MESSAGE_COUNT = 10
DELAY_SECONDS = 0.5


def delivery_report(err, msg):
    if err is not None:
        print(f"Zustellung fehlgeschlagen: {err}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(TOPIC, value=json.dumps(payload), callback=delivery_report)
        producer.poll(0)
        print(f"Gesendet: seq={seq}")
        time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
