"""Use Case 1: Einfache Warteschlange - Producer (RabbitMQ-Variante).

Sendet eine Serie nummerierter Nachrichten in konstantem Abstand.
Testablauf: Consumer VOR dem Start dieses Producers stoppen, dann erst
nach Abschluss des Producers wieder starten, um die Pufferung waehrend
der Downtime zu demonstrieren.
"""

import json
import time
from datetime import datetime, timezone

import pika

QUEUE_NAME = "uc1.simple_queue"
MESSAGE_COUNT = 10
DELAY_SECONDS = 0.5


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

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
        print(f"Gesendet: seq={seq}")
        time.sleep(DELAY_SECONDS)

    connection.close()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
