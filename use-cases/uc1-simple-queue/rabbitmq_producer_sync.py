"""Latenzmessung Kapitel 6.1.1 - Producer (RabbitMQ-Variante, synchron und getaktet).

channel.confirm_delivery() macht basic_publish() synchron (kehrt erst nach
Broker-Bestaetigung zurueck), Aequivalent zu flush() bei Kafka.

Taktung: fahrplanbasiert (absolute Sendezeitpunkte im Abstand
INTERVAL_SECONDS), identisch zu Kafka und IBM MQ. Frueher lief RabbitMQ
ungedrosselt, weil sich durch die Confirms keine Warteschlange aufbaute;
die angebotene Last war dann aber hoeher als bei Kafka/IBM MQ und die
Latenzen damit nicht unter gleichen Bedingungen gemessen. Die tatsaechlich
erreichte Rate wird am Ende ausgegeben und gehoert in den Messbericht.

Persistenz: delivery_mode=2 (persistent) ist die fuer Kapitel 6.1.1
methodisch richtige Referenzkonfiguration, siehe Kapitel 5.3.1 und den
RabbitMQ-Persistenz-Befund in Kapitel 6 - Zustellgarantie ist die
Themenstellung der Arbeit, ein nicht-persistenter Lauf gibt diese
Eigenschaft gerade auf.

Nur fuer den Vorher-Nachher-Vergleich (Sensitivitaetsanalyse, analog zum
IBM-MQ-Persistenzbefund in Kapitel 6): RABBITMQ_PERSISTENT=0 sendet mit
delivery_mode=1 (nicht persistent). Fuer berichtete Hauptwerte NICHT
verwenden, nur fuer den expliziten Vergleich beider Modi.
"""

import json
import os
import time
from datetime import datetime, timezone

import pika

QUEUE_NAME = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = int(os.environ.get("MEASURE_COUNT", 10000))
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT
TARGET_RATE = float(os.environ.get("TARGET_RATE", 500))  # Soll-Last in Nachrichten/s
INTERVAL_SECONDS = 1 / TARGET_RATE  # Standard 500/s = 2 ms, fuer alle drei identisch
PERSISTENT = os.environ.get("RABBITMQ_PERSISTENT", "1") != "0"
DELIVERY_MODE = 2 if PERSISTENT else 1


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.confirm_delivery()  # macht basic_publish synchron (wartet auf Ack)

    print(f"Start: {datetime.now(timezone.utc).isoformat()} "
          f"(Persistenz: {'ja' if PERSISTENT else 'NEIN, nur Diagnose'})")

    behind_count = 0
    t_start = time.perf_counter()
    next_send = t_start
    for seq in range(1, TOTAL_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=DELIVERY_MODE),
        )

        next_send += INTERVAL_SECONDS
        delay = next_send - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        else:
            # Im Rueckstand: nicht in Bursts nachholen, Fahrplan neu ansetzen
            next_send = time.perf_counter()
            behind_count += 1

    elapsed = time.perf_counter() - t_start
    connection.close()
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen), "
          f"getaktet mit {INTERVAL_SECONDS * 1000:.1f} ms Intervall, mit Publisher Confirms.")
    print(f"Tatsaechliche Rate: {TOTAL_COUNT / elapsed:.1f} Nachrichten/s "
          f"(Soll: {1 / INTERVAL_SECONDS:.0f}), Intervalle im Rueckstand: {behind_count}")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()