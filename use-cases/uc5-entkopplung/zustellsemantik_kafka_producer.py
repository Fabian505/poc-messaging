"""UC5 - Producer (Kafka). acks=all, synchron (flush pro Nachricht).
delivery.timeout.ms deckt einen Broker-Neustart ab, librdkafka verbindet
selbst neu. Bei exactly-once zusaetzlich enable.idempotence (Broker
verwirft producerseitige Retry-Duplikate)."""

import os

from confluent_kafka import Producer

import uc5_common as c

TOPIC = os.environ["TOPIC"]


def main():
    config = {"bootstrap.servers": "localhost:9092", "acks": "all",
              "delivery.timeout.ms": 120000}
    if c.SEMANTICS == "exactly-once":
        config["enable.idempotence"] = True
    producer = Producer(config)

    def send(body):
        result = {}
        producer.produce(TOPIC, value=body,
                         on_delivery=lambda err, _m: result.__setitem__("err", err))
        remaining = producer.flush(130)
        if remaining or result.get("err") is not None:
            raise RuntimeError(f"Zustellung fehlgeschlagen: {result.get('err')}")

    c.run_producer(send, reconnect_fn=lambda: None)


if __name__ == "__main__":
    main()
