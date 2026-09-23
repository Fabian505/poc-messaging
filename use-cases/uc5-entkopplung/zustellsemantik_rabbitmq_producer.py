"""Use Case 4.2.4 (Entkopplung bei Ausfall und Zustellsemantiken)
- Producer (RabbitMQ-Variante).

Bleibt bewusst unveraendert ueber alle drei Semantiken hinweg, der einzige
Unterschied zwischen at-most-once/at-least-once/exactly-once liegt auf
Consumer-Seite (siehe zustellsemantik_rabbitmq_consumer.py). Nutzung:

    python zustellsemantik_rabbitmq_producer.py
"""

import json
from datetime import datetime, timezone

import pika

QUEUE_NAME = "semantics.test"
MESSAGE_COUNT = 200


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT + 1):
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

    connection.close()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
