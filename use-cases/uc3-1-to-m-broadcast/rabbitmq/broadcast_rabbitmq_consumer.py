"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Consumer (RabbitMQ-Variante).

Fuer den Broadcast-Test mehrfach parallel starten, z.B. mit 3 unabhaengigen
Consumern in drei Terminals:

    python broadcast_rabbitmq_consumer.py broadcast-run-3 1
    python broadcast_rabbitmq_consumer.py broadcast-run-3 2
    python broadcast_rabbitmq_consumer.py broadcast-run-3 3

Jede Instanz deklariert ihre eigene, exklusive, automatisch benannte Queue
(queue='') und bindet sie an den Fanout-Exchange. Dadurch erhaelt jede
Instanz eine vollstaendige Kopie jeder Nachricht, nicht nur einen Teil wie
bei Lastverteilung. Die Queue wird beim Verbindungsende automatisch
geloescht (exclusive=True).
"""

import json
import sys
from datetime import datetime, timezone

import pika

EXCHANGE_NAME = "broadcast.test"
RESULTS_FILE = "results_broadcast.jsonl"
IDLE_TIMEOUT_SECONDS = 10.0


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python broadcast_rabbitmq_consumer.py <run_id> <instanz_id>")
        sys.exit(1)

    run_id, instance_id = sys.argv[1], sys.argv[2]

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True)

    result = channel.queue_declare(queue="", exclusive=True)
    own_queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=own_queue_name)

    processed_count = 0
    first_message_at = None

    print(f"[Instanz {instance_id}] Bereit, eigene Queue '{own_queue_name}'. Warte auf Nachrichten.")

    consumer_gen = channel.consume(
        own_queue_name, inactivity_timeout=IDLE_TIMEOUT_SECONDS
    )
    try:
        for method, properties, body in consumer_gen:
            if method is None:
                break

            if first_message_at is None:
                first_message_at = datetime.now(timezone.utc)

            json.loads(body)
            processed_count += 1
            channel.basic_ack(delivery_tag=method.delivery_tag)
    except KeyboardInterrupt:
        pass
    finally:
        channel.cancel()
        connection.close()

    finished_at = datetime.now(timezone.utc)
    print(f"[Instanz {instance_id}] Fertig: {processed_count} Nachrichten erhalten.")

    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps({
            "technology": "rabbitmq",
            "run_id": run_id,
            "instance_id": instance_id,
            "processed_count": processed_count,
            "first_message_at": first_message_at.isoformat() if first_message_at else None,
            "finished_at": finished_at.isoformat(),
        }) + "\n")


if __name__ == "__main__":
    main()
