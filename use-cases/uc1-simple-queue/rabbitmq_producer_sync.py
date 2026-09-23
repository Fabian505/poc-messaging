"""Latenzmessung Kapitel 6.1.1 - Producer (RabbitMQ-Variante, synchron ohne Delay).

RabbitMQ-Aequivalent zu kafka_producer_sync.py: kein time.sleep() zwischen
den Nachrichten, aber channel.confirm_delivery() aktiviert, damit
basic_publish() erst zurueckkehrt, wenn der Broker die Nachricht bestaetigt
hat. Das schliesst aus, dass der Producer selbst puffert, unabhaengig davon
bleibt aber ein moeglicher Effekt durch den Consumer-seitigen
prefetch_count=10 bestehen, das ist bewusst eine separate Fragestellung.

Vergleichspunkte:
- rabbitmq_producer.py            (Burst, keine Confirms)
- rabbitmq_producer_delayed.py    (Delay, keine Confirms)
- dieses Skript                   (kein Delay, mit Confirms)

Consumer bleibt unveraendert (rabbitmq_consumer.py), muss weiterhin VORHER
gestartet werden.
"""

import json
from datetime import datetime, timezone

import pika

QUEUE_NAME = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 1_000_000
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.confirm_delivery()  # macht basic_publish synchron (wartet auf Ack)

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, TOTAL_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=1),
        )

    connection.close()
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen), "
          "ohne Delay, mit Publisher Confirms.")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
