"""Use Case 4.2.3 (m:1) - Consumer (RabbitMQ). Eine Instanz, manuelles Ack
nach der Verarbeitung, prefetch 10 wie UC1."""

import pika

import m1_common as common

QUEUE_NAME = "aggregation.test"


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=10)
    print("[Consumer] Bereit", flush=True)

    rec = common.Recorder()
    try:
        for method, _props, body in channel.consume(QUEUE_NAME, inactivity_timeout=0.5):
            if method is None:
                if rec.timed_out():
                    break
                continue
            rec.record(body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            if rec.complete:
                break
        channel.cancel()
    finally:
        connection.close()
    rec.write("rabbitmq")


if __name__ == "__main__":
    main()
