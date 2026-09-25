"""UC5 - Consumer (RabbitMQ). Eigene dauerhafte Queue pro Lauf.
at-most-once: auto_ack=True. Achtung, charakteristisch: prefetch_count
wirkt bei auto_ack nicht, der Broker schiebt den gesamten Rueckstand
sofort in den Client-Puffer. Ein Absturz verliert dann ALLE gepufferten
Nachrichten, nicht nur die gerade verarbeitete.
at-least-once/exactly-once: manuelles Ack nach der Nacharbeit, prefetch 10."""

import os

import pika

import uc5_common as c

QUEUE_NAME = os.environ["QUEUE_NAME"]


def main():
    c.install_sigterm()
    conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    auto_ack = c.SEMANTICS == "at-most-once"
    if not auto_ack:
        ch.basic_qos(prefetch_count=10)
    store = c.EffectStore()
    c.ready()

    try:
        for method, _props, body in ch.consume(QUEUE_NAME, auto_ack=auto_ack,
                                               inactivity_timeout=0.5):
            if c.STOP["flag"]:
                break
            if method is None:
                continue
            ack = None if auto_ack else (lambda t=method.delivery_tag: ch.basic_ack(delivery_tag=t))
            c.process(store, body, ack)
        ch.cancel()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
