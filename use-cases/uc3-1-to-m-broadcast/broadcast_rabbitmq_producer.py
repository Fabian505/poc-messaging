"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Producer (RabbitMQ).

Publiziert an den Fanout-Exchange, persistent (delivery_mode=2) mit
Publisher Confirms, fahrplanbasiert getaktet wie UC1. Das Confirm kommt
erst, wenn die Nachricht in ALLEN gebundenen dauerhaften Queues gesichert
ist, die Kosten steigen also mit der Zahl der Subscriber.
"""

import pika

from uc3_pacing import run_paced

EXCHANGE_NAME = "broadcast.test"


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True)
    channel.confirm_delivery()
    props = pika.BasicProperties(delivery_mode=2)

    def send(body):
        channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="",
                              body=body, properties=props)

    run_paced(send)
    connection.close()


if __name__ == "__main__":
    main()
