"""Latenzmessung Kapitel 6.1.1 - Consumer (RabbitMQ-Variante).

WICHTIG: Diesen Consumer ZUERST starten und aktiv warten lassen, dann erst
den Producer starten. Die ersten WARMUP_COUNT Nachrichten werden verworfen,
erst danach beginnt die eigentliche Messung.

at-least-once: manuelles Ack ERST NACH vollstaendiger Verarbeitung der
Nachricht, konsistent mit den uebrigen Use Cases (siehe Kapitel 4.2 / 5.2).
Zuvor erfolgte das Ack bereits vor dem Speichern des Messwerts in der
latencies-Liste, das war inkonsistent zu den anderen Use Cases.
"""

import json
from datetime import datetime, timezone

import pika

from latency_stats import print_latency_summary

QUEUE_NAME = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 100
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT

latencies = []


def callback(ch, method, properties, body):
    payload = json.loads(body)
    seq = payload["seq"]
    sent_at = datetime.fromisoformat(payload["sent_at"])
    received_at = datetime.now(timezone.utc)
    latency = (received_at - sent_at).total_seconds()

    if seq <= WARMUP_COUNT:
        pass  # Warmup, wird verworfen
    else:
        latencies.append(latency)

    # Erst nach erfolgreicher Verarbeitung acken (at-least-once)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    if seq >= TOTAL_COUNT:
        ch.stop_consuming()


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print(f"Bereit. Warte auf {TOTAL_COUNT} Nachrichten "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen). "
          "Jetzt den Producer starten.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()

    print_latency_summary(latencies, "RabbitMQ")


if __name__ == "__main__":
    main()
