"""Use Case 4.2.3 (m:1) - Consumer (RabbitMQ-Variante).

Nur EINE Instanz starten. Protokolliert jede Nachricht roh in
results_m1.jsonl fuer die spaetere Verlust- und Reihenfolge-Analyse mit
analyze_m1.py.
"""

import json
import sys
from datetime import datetime, timezone

import pika

QUEUE_NAME = "aggregation.test"
RESULTS_FILE = "results_m1.jsonl"
IDLE_TIMEOUT_SECONDS = 10.0
PREFETCH_COUNT = 50


def main():
    if len(sys.argv) != 2:
        print("Nutzung: python m1_rabbitmq_consumer.py <run_id>")
        sys.exit(1)

    run_id = sys.argv[1]

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    received_count = 0
    print(f"Bereit, run_id '{run_id}'. Warte auf Nachrichten von allen Producern.")

    consumer_gen = channel.consume(QUEUE_NAME, inactivity_timeout=IDLE_TIMEOUT_SECONDS)
    with open(RESULTS_FILE, "a") as f:
        try:
            for method, properties, body in consumer_gen:
                if method is None:
                    break

                payload = json.loads(body)
                received_at = datetime.now(timezone.utc).isoformat()

                f.write(json.dumps({
                    "technology": "rabbitmq",
                    "run_id": payload["run_id"],
                    "producer_id": payload["producer_id"],
                    "seq": payload["seq"],
                    "sent_at": payload["sent_at"],
                    "received_at": received_at,
                }) + "\n")
                received_count += 1

                channel.basic_ack(delivery_tag=method.delivery_tag)
        except KeyboardInterrupt:
            pass
        finally:
            channel.cancel()
            connection.close()

    print(f"Fertig: {received_count} Nachrichten insgesamt empfangen.")


if __name__ == "__main__":
    main()
