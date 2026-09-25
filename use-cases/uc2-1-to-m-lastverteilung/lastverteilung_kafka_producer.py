"""Use Case 4.2.2.1 (Lastverteilung) - Producer (Kafka-Variante).

Befuellt das Topic VOR dem Start der Consumer (Vorbefuellung). Die
Senderate ist hier bewusst nicht Teil der Messung: gemessen wird, wie
schnell N Consumer einen festen Bestand abarbeiten. Deshalb ungedrosselt
und asynchron (Batching erlaubt).

Parameter per Umgebungsvariable: TOPIC, PARTITIONS, MESSAGE_COUNT.

Explizite Partitionierung (seq mod PARTITIONS): exakt gleich viele
Nachrichten pro Partition. Ohne Key wuerde der Sticky Partitioner
ungleichmaessig verteilen und die Fairness-Auswertung verfaelschen.
acks=all explizit, damit die Bestaetigungssemantik dokumentiert ist.
"""

import json
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = os.environ["TOPIC"]
PARTITIONS = int(os.environ["PARTITIONS"])
MESSAGE_COUNT = int(os.environ["MESSAGE_COUNT"])


def main():
    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
        "linger.ms": 5,
    })
    errors = {"count": 0}

    def on_delivery(err, _msg):
        if err is not None:
            errors["count"] += 1

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = json.dumps({
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        })
        while True:
            try:
                producer.produce(TOPIC, value=payload,
                                 partition=(seq - 1) % PARTITIONS,
                                 on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)  # lokaler Puffer voll, kurz abarbeiten
        producer.poll(0)

    remaining = producer.flush(120)
    if remaining or errors["count"]:
        print(f"FEHLER: {remaining} nicht zugestellt, {errors['count']} Zustellfehler.")
        sys.exit(1)
    print(f"Vorbefuellung fertig: {MESSAGE_COUNT} Nachrichten.")


if __name__ == "__main__":
    main()
