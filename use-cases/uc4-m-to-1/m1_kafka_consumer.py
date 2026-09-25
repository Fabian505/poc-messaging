"""Use Case 4.2.3 (m:1) - Consumer (Kafka). Eine Instanz, frisches Topic mit
earliest, "Bereit" erst nach Partitionszuweisung. at-least-once:
synchroner Commit nach der Verarbeitung."""

import os

from confluent_kafka import Consumer

import m1_common as common

TOPIC = os.environ["TOPIC"]


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"uc4-{common.RUN_ID}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    assigned = {"done": False}

    def on_assign(c, partitions):
        c.assign(partitions)
        assigned["done"] = True

    consumer.subscribe([TOPIC], on_assign=on_assign)
    while not assigned["done"]:
        consumer.poll(0.1)
    print("[Consumer] Bereit", flush=True)

    rec = common.Recorder()
    try:
        while not rec.complete:
            msg = consumer.poll(0.5)
            if msg is None:
                if rec.timed_out():
                    break
                continue
            if msg.error():
                print(f"Fehler: {msg.error()}", flush=True)
                continue
            rec.record(msg.value())
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
    rec.write("kafka")


if __name__ == "__main__":
    main()
