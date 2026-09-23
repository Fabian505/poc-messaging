"""Latenzmessung Kapitel 6.1.1 - Consumer (Kafka-Variante).

WICHTIG: Diesen Consumer ZUERST starten und aktiv warten lassen, dann erst
den Producer starten. Nutzt eine neue, zeitstempelbasierte Consumer Group
und auto.offset.reset=latest, damit jeder Testlauf garantiert nur die
Nachrichten dieses Laufs zaehlt, unabhaengig von frueheren Testlaeufen.
"""

import json
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer

from latency_stats import print_latency_summary

TOPIC = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 1_000_000
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT


def main():
    group_id = f"latency-test-{int(time.time())}"
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([TOPIC])

    latencies = []
    received_count = 0

    print(f"Bereit. Warte auf {TOTAL_COUNT} Nachrichten "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen). "
          "Jetzt den Producer starten.")
    try:
        while received_count < TOTAL_COUNT:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Fehler: {msg.error()}")
                continue

            payload = json.loads(msg.value())
            seq = payload["seq"]
            sent_at = datetime.fromisoformat(payload["sent_at"])
            received_at = datetime.now(timezone.utc)
            latency = (received_at - sent_at).total_seconds()

            received_count += 1
            if seq > WARMUP_COUNT:
                latencies.append(latency)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print_latency_summary(latencies, "Kafka")


if __name__ == "__main__":
    main()
