"""Latenzmessung Kapitel 6.1.1 - Producer (Kafka-Variante).

WICHTIG: Consumer VORHER starten, damit die Consumer Group bereits gejoined
und der Rebalancing-Aufwand nicht in die Messwerte einfliesst.
"""

import json
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 10000
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, TOTAL_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(TOPIC, value=json.dumps(payload))
        producer.poll(0)

    producer.flush()
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen).")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
