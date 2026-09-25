"""Use Case 4.2.2.1 (Lastverteilung) - Consumer (Kafka-Variante).

Wird vom Orchestrator run_uc2_measurement.py gestartet, Parameter siehe
uc2_common.py. Zusaetzlich: TOPIC (pro Lauf frisch angelegtes Topic).

Startbarriere bei Kafka: Zugewiesene Partitionen werden sofort pausiert,
bis das Startsignal kommt. Jede (Neu-)Zuweisung wird als "Zuweisung: <k>"
gemeldet; der Orchestrator gibt das Startsignal erst, wenn alle Partitionen
verteilt sind und sich die Zuweisung nicht mehr aendert. Damit findet kein
Rebalance waehrend der Messung statt (keine Pausen, keine Duplikate).

at-least-once: enable.auto.commit=False, synchroner Commit pro Nachricht
nach der Verarbeitung (identisch zu UC1).
"""

import os
import time

from confluent_kafka import Consumer

import uc2_common as common

TOPIC = os.environ["TOPIC"]


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"uc2-{common.RUN_ID}",
        "auto.offset.reset": "earliest",  # Topic ist frisch, enthaelt nur diesen Lauf
        "enable.auto.commit": False,
    })
    state = {"started": False}

    def on_assign(c, partitions):
        c.assign(partitions)
        if not state["started"] and partitions:
            c.pause(partitions)
        print(f"Zuweisung: {len(partitions)}", flush=True)

    def on_revoke(c, partitions):
        print("Zuweisung: 0", flush=True)

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    print(f"[Instanz {common.INSTANCE_ID}] Bereit", flush=True)

    common.wait_for_start(idle_fn=lambda: consumer.poll(0.05))
    state["started"] = True
    assignment = consumer.assignment()
    if assignment:
        consumer.resume(assignment)

    processed = 0
    first_at = last_at = None
    last_activity = time.monotonic()
    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                if time.monotonic() - last_activity > common.IDLE_TIMEOUT_SECONDS:
                    break
                continue
            if msg.error():
                print(f"[Instanz {common.INSTANCE_ID}] Fehler: {msg.error()}", flush=True)
                continue

            if first_at is None:
                first_at = time.time()
            common.simulate_processing(msg.value())
            consumer.commit(message=msg, asynchronous=False)  # at-least-once
            processed += 1
            last_at = time.time()
            last_activity = time.monotonic()
    finally:
        consumer.close()

    common.write_result("kafka", processed, first_at, last_at)


if __name__ == "__main__":
    main()
