"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Producer (RabbitMQ-Variante).

Publiziert ueber einen Fanout-Exchange statt an eine direkte Queue wie bei
Lastverteilung. Ein Fanout-Exchange leitet jede Nachricht an ALLE daran
gebundenen Queues weiter, unabhaengig vom Routing-Key.

WICHTIG: Alle Consumer-Instanzen VORHER starten, damit ihre jeweils eigene
Queue bereits an den Exchange gebunden ist, bevor der Producer sendet.
Sonst gehen Nachrichten, die vor dem Binden gesendet werden, verloren
(das ist bei Fanout so gewollt, es gibt anders als bei einer Queue keinen
Puffer, der auf den ersten Consumer wartet).
"""

import json
from datetime import datetime, timezone

import pika

EXCHANGE_NAME = "broadcast.test"
MESSAGE_COUNT = 1_000


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True)

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key="",  # bei Fanout ohne Bedeutung
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2),
        )

    connection.close()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
