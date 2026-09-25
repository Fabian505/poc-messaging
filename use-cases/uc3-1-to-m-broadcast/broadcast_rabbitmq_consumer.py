"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Subscriber (RabbitMQ).

Jeder Subscriber bindet eine EIGENE, DAUERHAFTE Queue an den Fanout-
Exchange. Nicht mehr exclusive/auto-delete wie in der ersten Fassung:
  1. Eine exklusive Queue ist nicht durable, persistente Nachrichten
     (delivery_mode=2) werden darin NICHT auf Platte gesichert. Das waere
     inkonsistent zu UC1/UC2.
  2. Eine exklusive Queue verschwindet mit der Verbindung. Nachrichten,
     die waehrend eines Neustarts des Subscribers publiziert werden, gehen
     fuer ihn verloren, das widerspricht at-least-once.
Die Queue wird am Laufende geloescht (und vom Orchestrator zur Sicherheit
nochmals), damit keine verwaisten Queues weiter Kopien sammeln.

at-least-once: manuelles Ack nach der Verarbeitung, prefetch 10 wie UC1.
"""

import pika

import uc3_common as common

EXCHANGE_NAME = "broadcast.test"
QUEUE_NAME = f"broadcast.{common.RUN_ID}.{common.INSTANCE_ID}"


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME)
    channel.basic_qos(prefetch_count=10)
    print(f"[Subscriber {common.INSTANCE_ID}] Bereit", flush=True)

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
        channel.queue_delete(queue=QUEUE_NAME)
    finally:
        connection.close()
    rec.write("rabbitmq")


if __name__ == "__main__":
    main()
