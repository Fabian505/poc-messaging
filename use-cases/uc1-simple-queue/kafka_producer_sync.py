"""Latenzmessung Kapitel 6.1.1 - Producer (Kafka-Variante, synchron ohne Delay).

Isoliert die Frage, ob das Batching-Artefakt aus dem urspruenglichen
Burst-Test durch flush() pro Nachricht behoben wird, OHNE den kuenstlichen
Delay aus kafka_producer_delayed.py. Kein time.sleep() zwischen den
Nachrichten, nur explizites Warten auf die Broker-Bestaetigung pro
Nachricht (kein Client-seitiges Batching mehr moeglich).

Vergleichspunkte:
- kafka_producer.py            (Burst, kein flush pro Nachricht)
- kafka_producer_delayed.py    (Delay + flush pro Nachricht)
- dieses Skript                (kein Delay, aber flush pro Nachricht)

Consumer bleibt unveraendert (kafka_consumer.py), muss weiterhin VORHER
gestartet werden.
"""

import json
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 1_000_000
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
        producer.flush()  # erzwingt synchronen Versand, kein Batching

    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen), "
          "ohne Delay, mit flush() pro Nachricht.")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
