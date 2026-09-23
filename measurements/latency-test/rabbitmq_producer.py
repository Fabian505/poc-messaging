"""Latenzmessung Kapitel 6.1.1 - Producer (RabbitMQ-Variante).

WICHTIG: Der Consumer muss VOR diesem Producer gestartet werden und aktiv
warten. Nur dann misst der Consumer echte Systemlatenz statt Bedienzeit.

Sendet WARMUP_COUNT Nachrichten, die der Consumer verwirft (JIT/Verbindungs-
Aufwaermen), gefolgt von MEASURE_COUNT Nachrichten, die tatsaechlich in die
Statistik einfliessen.
"""

import json
from datetime import datetime, timezone

import pika

QUEUE_NAME = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 10_000
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, TOTAL_COUNT + 1):
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

    connection.close()
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen).")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
