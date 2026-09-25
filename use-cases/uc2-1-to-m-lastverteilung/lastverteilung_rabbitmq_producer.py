"""Use Case 4.2.2.1 (Lastverteilung) - Producer (RabbitMQ-Variante).

Befuellt die Queue VOR dem Start der Consumer (Vorbefuellung), Senderate
nicht Teil der Messung. delivery_mode=2 (persistent), konsistent zu UC1.
Ohne Publisher Confirms, weil die Vollstaendigkeit anschliessend vom
Orchestrator ueber die Queue-Tiefe geprueft wird.

Parameter per Umgebungsvariable: MESSAGE_COUNT.
"""

import json
import os
from datetime import datetime, timezone

import pika

QUEUE_NAME = "loadbalance.test"
MESSAGE_COUNT = int(os.environ["MESSAGE_COUNT"])


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    props = pika.BasicProperties(delivery_mode=2)

    for seq in range(1, MESSAGE_COUNT + 1):
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps({
                "seq": seq,
                "sent_at": datetime.now(timezone.utc).isoformat(),
            }),
            properties=props,
        )

    connection.close()
    print(f"Vorbefuellung fertig: {MESSAGE_COUNT} Nachrichten.")


if __name__ == "__main__":
    main()
