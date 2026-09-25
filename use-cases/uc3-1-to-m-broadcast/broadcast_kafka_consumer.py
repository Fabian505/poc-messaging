"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Subscriber (Kafka).

Jeder Subscriber hat eine EIGENE Consumer Group und liest dadurch das
komplette Topic. Das Topic wird pro Lauf frisch angelegt (TOPIC), daher
auto.offset.reset=earliest ohne Risiko alter Nachrichten.

"Bereit" erst nach abgeschlossener Partitionszuweisung, sonst verpasst der
Subscriber die ersten Nachrichten nicht (earliest), sie haetten aber eine
kuenstlich hohe Latenz.

at-least-once: synchroner Commit nach der Verarbeitung (wie UC1/UC2).
Eine Consumer Group ist implizit dauerhaft: die Offsets bleiben erhalten,
ein neu gestarteter Subscriber setzt dort fort, wo er aufgehoert hat.
"""

import os

from confluent_kafka import Consumer

import uc3_common as common

TOPIC = os.environ["TOPIC"]


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"uc3-{common.RUN_ID}-{common.INSTANCE_ID}",
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
    print(f"[Subscriber {common.INSTANCE_ID}] Bereit", flush=True)

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
