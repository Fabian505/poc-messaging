"""Latenzmessung Kapitel 6.1.1 - Producer (Kafka-Variante, synchron UND getaktet).

flush() pro Nachricht verhindert Client-seitiges Batching. Zusaetzlich wird
mit fester Soll-Last gesendet, damit sich innerhalb des Laufs keine
Warteschlange aufbaut (Consumer mit explizitem Commit schafft auf dem
Laptop ca. 1400 Nachrichten/s, siehe kafka_consumer_seqdiagnose.py).

Taktung: fahrplanbasiert (absolute Sendezeitpunkte im Abstand
INTERVAL_SECONDS), NICHT sleep() nach dem Senden. Damit ist die angebotene
Last fuer alle drei Technologien identisch (500 Nachrichten/s) und haengt
nicht von der technologiespezifischen Sendedauer ab. Die tatsaechlich
erreichte Rate wird am Ende ausgegeben und gehoert in den Messbericht.
"""

import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = int(os.environ.get("MEASURE_COUNT", 10000))
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT
TARGET_RATE = float(os.environ.get("TARGET_RATE", 500))  # Soll-Last in Nachrichten/s
INTERVAL_SECONDS = 1 / TARGET_RATE  # Standard 500/s = 2 ms, fuer alle drei identisch


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    behind_count = 0
    t_start = time.perf_counter()
    next_send = t_start
    for seq in range(1, TOTAL_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(TOPIC, value=json.dumps(payload))
        producer.flush()  # weiterhin noetig, verhindert Client-Batching

        next_send += INTERVAL_SECONDS
        delay = next_send - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        else:
            # Im Rueckstand: nicht in Bursts nachholen, Fahrplan neu ansetzen
            next_send = time.perf_counter()
            behind_count += 1

    elapsed = time.perf_counter() - t_start
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen), "
          f"synchron, getaktet mit {INTERVAL_SECONDS * 1000:.1f} ms Intervall.")
    print(f"Tatsaechliche Rate: {TOTAL_COUNT / elapsed:.1f} Nachrichten/s "
          f"(Soll: {1 / INTERVAL_SECONDS:.0f}), Intervalle im Rueckstand: {behind_count}")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
