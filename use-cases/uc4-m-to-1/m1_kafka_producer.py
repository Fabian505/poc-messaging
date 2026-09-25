"""Use Case 4.2.3 (m:1) - Producer (Kafka). Synchron (flush pro Nachricht,
acks=all), getaktet nach m1_common. Topic per Umgebungsvariable TOPIC
(1 Partition, pro Lauf frisch): ein total geordnetes Log, Reihenfolge je
Producer ist damit garantiert, solange synchron gesendet wird."""

import os

from confluent_kafka import Producer

import m1_common as common

TOPIC = os.environ["TOPIC"]


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})

    def send(body):
        producer.produce(TOPIC, value=body)
        producer.flush()

    common.run_paced_producer(send, idle_fn=lambda: producer.poll(0.01))


if __name__ == "__main__":
    main()
