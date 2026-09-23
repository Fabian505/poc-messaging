"""Use Case 4.2.2.1 (Lastverteilung) - Producer (RabbitMQ-Variante).

Sendet an die Queue 'loadbalance.test'. Anders als bei Kafka ist keine
Partitionierung noetig, RabbitMQ verteilt Nachrichten von einer Queue aus
automatisch (round-robin, begrenzt durch prefetch_count) auf alle
gleichzeitig verbundenen Consumer.

WICHTIG: Vor jedem Testlauf pruefen, dass die Queue leer ist (siehe
Lessons Learned zum RabbitMQ-Backlog-Vorfall):

    podman exec rabbitmq rabbitmqctl list_queues name messages

Consumer-Instanzen VORHER starten.
"""

import json
from datetime import datetime, timezone

import pika

QUEUE_NAME = "loadbalance.test"
MESSAGE_COUNT = 10_000


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
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
