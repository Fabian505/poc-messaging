"""Latenzmessung Kapitel 6.1.1 - Producer (RabbitMQ-Variante, mit Verzoegerung).

Gleiche Verzoegerung wie bei kafka_producer_delayed.py, damit alle drei
Technologien mit identischer Methodik gemessen werden. Consumer bleibt
unveraendert (rabbitmq_consumer.py).
"""

import json
import time
from datetime import datetime, timezone

import pika

QUEUE_NAME = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 100
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT
DELAY_SECONDS = 0.05


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for seq in range(1, TOTAL_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        time.sleep(DELAY_SECONDS)

    connection.close()
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen), "
          f"mit {DELAY_SECONDS * 1000:.0f} ms Abstand.")


if __name__ == "__main__":
    main()
