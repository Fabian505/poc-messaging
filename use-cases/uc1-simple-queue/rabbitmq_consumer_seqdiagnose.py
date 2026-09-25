"""Diagnose-Variante von rabbitmq_consumer.py: protokolliert zusaetzlich
jede einzelne (seq, latency)-Paarung in eine CSV-Datei, um zu pruefen, ob
die Latenz mit der Sequenznummer ansteigt (Hinweis auf Producer/Consumer-
Geschwindigkeits-Mismatch, analog zum Kafka-/IBM-MQ-Befund).

Nutzung identisch zu rabbitmq_consumer.py, schreibt zusaetzlich
rabbitmq_diagnose_seq_latency.csv.
"""

import csv
import json
from datetime import datetime, timezone

import pika

from latency_stats import print_latency_summary

QUEUE_NAME = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 10000
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT

latencies = []
csv_writer = None


def callback(ch, method, properties, body):
    payload = json.loads(body)
    seq = payload["seq"]
    sent_at = datetime.fromisoformat(payload["sent_at"])
    received_at = datetime.now(timezone.utc)
    latency = (received_at - sent_at).total_seconds()

    if seq <= WARMUP_COUNT:
        pass  # Warmup, wird verworfen
    else:
        latencies.append(latency)
        csv_writer.writerow([seq, latency * 1000, received_at.isoformat()])

    # Erst nach erfolgreicher Verarbeitung acken (at-least-once)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    if seq >= TOTAL_COUNT:
        ch.stop_consuming()


def main():
    global csv_writer

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print(f"Bereit. Warte auf {TOTAL_COUNT} Nachrichten "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen). "
          "Jetzt den Producer starten.")

    with open("rabbitmq_diagnose_seq_latency.csv", "w", newline="") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["seq", "latency_ms", "received_at"])

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        finally:
            connection.close()

    print_latency_summary(latencies, "RabbitMQ")
    print("\nDetails in rabbitmq_diagnose_seq_latency.csv, seq gegen latency_ms "
          "pruefen: steigt die Latenz mit steigendem seq deutlich an?")


if __name__ == "__main__":
    main()
