"""Use Case 4.2.3 (m:1) - Producer (RabbitMQ-Variante).

Mehrfach parallel mit unterschiedlicher producer_id starten, analog zu
m1_kafka_producer.py. Alle Instanzen senden an dieselbe Queue
'aggregation.test'.
"""

import json
import sys
from datetime import datetime, timezone

import pika

QUEUE_NAME = "aggregation.test"
MESSAGE_COUNT_PER_PRODUCER = 1_000


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python m1_rabbitmq_producer.py <run_id> <producer_id>")
        sys.exit(1)

    run_id, producer_id = sys.argv[1], sys.argv[2]

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"[Producer {producer_id}] Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT_PER_PRODUCER + 1):
        payload = {
            "run_id": run_id,
            "producer_id": producer_id,
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
    print(f"[Producer {producer_id}] Fertig: {MESSAGE_COUNT_PER_PRODUCER} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
