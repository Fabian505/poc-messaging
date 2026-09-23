"""Latenzmessung Kapitel 6.1.1 - Producer (Kafka-Variante, mit Verzoegerung).

Testet den Verdacht aus der ersten Messung: ob die extrem niedrige
Standardabweichung dort ein Batch-Artefakt des Kafka-Clients war. Sendet
mit DELAY_SECONDS Abstand zwischen den Nachrichten, damit der Consumer
jede Nachricht einzeln abholen muss statt mehrere in einem Fetch zu
bekommen.

Consumer bleibt identisch (kafka_consumer.py), muss weiterhin VORHER
gestartet werden.
"""

import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 100
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT
DELAY_SECONDS = 0.05  # 50 ms, bewusst groesser als typische Batch-Fenster


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    for seq in range(1, TOTAL_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(TOPIC, value=json.dumps(payload))
        producer.flush()  # bewusst sofort senden, kein Client-seitiges Batching
        time.sleep(DELAY_SECONDS)

    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen), "
          f"mit {DELAY_SECONDS * 1000:.0f} ms Abstand.")


if __name__ == "__main__":
    main()
