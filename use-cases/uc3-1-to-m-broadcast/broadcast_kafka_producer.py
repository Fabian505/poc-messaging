"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Producer (Kafka).

Synchron (flush pro Nachricht, acks=all) und fahrplanbasiert getaktet,
wie der UC1-Producer. Topic per Umgebungsvariable TOPIC (pro Lauf frisch
angelegt vom Orchestrator, 1 Partition).
"""

import os

from confluent_kafka import Producer

from uc3_pacing import run_paced

TOPIC = os.environ["TOPIC"]


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})

    def send(body):
        producer.produce(TOPIC, value=body)
        producer.flush()

    run_paced(send)


if __name__ == "__main__":
    main()
