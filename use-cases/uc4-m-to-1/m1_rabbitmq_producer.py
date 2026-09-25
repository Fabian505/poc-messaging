"""Use Case 4.2.3 (m:1) - Producer (RabbitMQ). Persistent (delivery_mode=2)
mit Publisher Confirms, getaktet nach m1_common."""

import pika

import m1_common as common

QUEUE_NAME = "aggregation.test"


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.confirm_delivery()
    props = pika.BasicProperties(delivery_mode=2)

    def send(body):
        channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=body, properties=props)

    common.run_paced_producer(send, idle_fn=lambda: connection.sleep(0.01))
    connection.close()


if __name__ == "__main__":
    main()
