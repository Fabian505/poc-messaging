"""UC5 - Consumer (Kafka). Eigene Consumer Group pro Lauf, frisches Topic,
earliest. Ein neu gestarteter Consumer setzt am letzten Commit fort.

session.timeout.ms=6000 (Minimum des Brokers): Nach einem SIGKILL wartet
der Group Coordinator auf das Ablaufen der Session des toten Mitglieds,
bevor er die Partition neu zuweist. Mit dem Standard (45 s) stuende der
neu gestartete Consumer so lange still. Das ist ein eigener Befund zur
Wiederanlaufzeit von Kafka nach einem Consumer-Absturz."""

import os
import time

from confluent_kafka import Consumer

import uc5_common as c

TOPIC = os.environ["TOPIC"]


def main():
    c.install_sigterm()
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"uc5-{c.RUN_ID}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 2000,
    })
    assigned = {"done": False}

    def on_assign(cons, partitions):
        cons.assign(partitions)
        assigned["done"] = True

    consumer.subscribe([TOPIC], on_assign=on_assign)
    # ACHTUNG: Derselbe poll()-Aufruf, der die Zuweisung ausloest, kann bei
    # vorhandenem Rueckstand (Neustart nach Absturz) bereits die erste
    # Nachricht zurueckgeben. Die erste Fassung verwarf sie: sie wurde nie
    # verarbeitet, aber durch den Commit der Folgenachricht uebersprungen
    # (Verlust genau einer Nachricht in ALLEN Semantiken).
    pending = None
    deadline = time.time() + 120
    while not assigned["done"] and not c.STOP["flag"]:
        m = consumer.poll(0.2)
        if m is not None and not m.error():
            pending = m
        if time.time() > deadline:
            raise TimeoutError("Keine Partitionszuweisung")
    store = c.EffectStore()
    c.ready()

    try:
        while not c.STOP["flag"]:
            if pending is not None:
                msg, pending = pending, None
            else:
                msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                print(f"Fehler: {msg.error()}", flush=True)
                continue
            c.process(store, msg.value(),
                      lambda: consumer.commit(message=msg, asynchronous=False))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
