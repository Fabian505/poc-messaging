"""UC5 - Producer (RabbitMQ). Persistent (delivery_mode=2) MIT Publisher
Confirms: Erst das Confirm gilt als Bestaetigung. Ohne Confirms (erste
Fassung) konnten Nachrichten beim Broker-Neustart unbemerkt verloren gehen,
der Verlust waere faelschlich der Consumer-Semantik angelastet worden.
Wiederverbindung bei Verbindungsverlust (pika hat keine eingebaute)."""

import os

import pika

import uc5_common as c

QUEUE_NAME = os.environ["QUEUE_NAME"]
state = {}


def connect():
    try:
        state["conn"].close()
    except Exception:
        pass
    conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.confirm_delivery()
    state.update(conn=conn, ch=ch)


def main():
    connect()
    props = pika.BasicProperties(delivery_mode=2)

    def send(body):
        state["ch"].basic_publish(exchange="", routing_key=QUEUE_NAME, body=body,
                                  properties=props, mandatory=True)

    c.run_producer(send, reconnect_fn=connect)
    state["conn"].close()


if __name__ == "__main__":
    main()
