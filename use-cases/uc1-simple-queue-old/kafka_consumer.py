"""Use Case 1: Einfache Warteschlange - Consumer (Kafka-Variante)."""

import json
from datetime import datetime, timezone

from confluent_kafka import Consumer

TOPIC = "uc1.simple_queue"
MESSAGE_COUNT = 10


def print_summary(received_seqs):
    expected = list(range(1, MESSAGE_COUNT + 1))
    in_order = received_seqs == sorted(received_seqs)
    missing = sorted(set(expected) - set(received_seqs))
    print("--- Zusammenfassung ---")
    print(f"Empfangen: {len(received_seqs)} von {MESSAGE_COUNT}")
    print(f"Reihenfolge korrekt: {in_order}")
    print(f"Fehlende Sequenznummern: {missing if missing else 'keine'}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "uc1-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    received_seqs = []
    print(f"Warte auf {MESSAGE_COUNT} Nachrichten. Strg+C zum vorzeitigen Beenden.")
    try:
        while len(received_seqs) < MESSAGE_COUNT:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Fehler: {msg.error()}")
                continue

            payload = json.loads(msg.value())
            seq = payload["seq"]
            sent_at = datetime.fromisoformat(payload["sent_at"])
            received_at = datetime.now(timezone.utc)
            latency = (received_at - sent_at).total_seconds()

            received_seqs.append(seq)
            print(f"Empfangen: seq={seq} latenz={latency:.3f}s")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print_summary(received_seqs)


if __name__ == "__main__":
    main()
