"""Use Case 1: Einfache Warteschlange - Consumer (RabbitMQ-Variante).

Liest Nachrichten von der Queue, bis MESSAGE_COUNT erreicht ist oder
Strg+C gedrueckt wird. Gibt am Ende eine Zusammenfassung aus.
"""

import json
from datetime import datetime, timezone

import pika

QUEUE_NAME = "uc1.simple_queue"
MESSAGE_COUNT = 10_000

received_seqs = []


def callback(ch, method, properties, body):
    payload = json.loads(body)
    seq = payload["seq"]
    sent_at = datetime.fromisoformat(payload["sent_at"])
    received_at = datetime.now(timezone.utc)
    latency = (received_at - sent_at).total_seconds()

    received_seqs.append(seq)
    print(f"Empfangen: seq={seq} latenz={latency:.3f}s")

    ch.basic_ack(delivery_tag=method.delivery_tag)

    if len(received_seqs) >= MESSAGE_COUNT:
        ch.stop_consuming()


def print_summary():
    expected = list(range(1, MESSAGE_COUNT + 1))
    in_order = received_seqs == sorted(received_seqs)
    missing = sorted(set(expected) - set(received_seqs))
    print("--- Zusammenfassung ---")
    print(f"Empfangen: {len(received_seqs)} von {MESSAGE_COUNT}")
    print(f"Reihenfolge korrekt: {in_order}")
    print(f"Fehlende Sequenznummern: {missing if missing else 'keine'}")


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print(f"Warte auf {MESSAGE_COUNT} Nachrichten. Strg+C zum vorzeitigen Beenden.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()

    print_summary()


if __name__ == "__main__":
    main()
