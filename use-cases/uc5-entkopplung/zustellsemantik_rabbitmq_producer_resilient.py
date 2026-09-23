"""Use Case 4.2.4 - Producer (RabbitMQ-Variante) mit Wiederverbindung.

Nur fuer den Fehlertyp "Broker-Neustart waehrend des Sendens" gedacht.
pika hat anders als confluent-kafka/librdkafka keine eingebaute
Wiederverbindungslogik, ohne diese wuerde das Skript bei einem
Verbindungsabbruch schlicht mit einer Exception abbrechen, was den Test
unbrauchbar macht.

Bei einem Verbindungsfehler wird kurz gewartet, neu verbunden, und ab der
Nachricht fortgefahren, bei der der Fehler auftrat (die letzte, evtl.
schon gesendete aber nicht bestaetigte Nachricht wird dabei erneut
gesendet, das ist bewusst in Kauf genommen und deckt sich mit den ohnehin
zu erwartenden Duplikaten bei at-least-once/exactly-once).
"""

import json
import sys
import time
from datetime import datetime, timezone

import pika

QUEUE_NAME = "semantics.test"
MESSAGE_COUNT = 200
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY_SECONDS = 1.0


def connect():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    return connection, channel


def main():
    connection, channel = connect()
    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    seq = 1
    reconnect_attempts = 0
    while seq <= MESSAGE_COUNT:
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            seq += 1
            reconnect_attempts = 0
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError, pika.exceptions.ChannelClosed):
            reconnect_attempts += 1
            if reconnect_attempts > MAX_RECONNECT_ATTEMPTS:
                print(f"Abbruch nach {MAX_RECONNECT_ATTEMPTS} erfolglosen Wiederverbindungsversuchen.")
                sys.exit(1)
            print(f"Verbindung verloren bei seq={seq}, Wiederverbindungsversuch {reconnect_attempts}...")
            time.sleep(RECONNECT_DELAY_SECONDS)
            try:
                connection.close()
            except Exception:
                pass
            connection, channel = connect()

    connection.close()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
