"""Use Case 4.2.2.1 (Lastverteilung) - Consumer (RabbitMQ-Variante).

Fuer den Durchsatztest mehrfach parallel starten, z.B. fuer eine
Konfiguration mit 3 Instanzen in drei Terminals:

    python lastverteilung_rabbitmq_consumer.py loadbalance-run-3 1
    python lastverteilung_rabbitmq_consumer.py loadbalance-run-3 2
    python lastverteilung_rabbitmq_consumer.py loadbalance-run-3 3

Erstes Argument: run_id nur zur Kennzeichnung in der Ergebnisdatei (RabbitMQ
selbst braucht dafuer keinen eigenen Parameter, die Lastverteilung ergibt
sich automatisch daraus, dass mehrere Consumer an derselben Queue haengen).
Zweites Argument: Instanz-ID, nur fuer die Log-Ausgabe.

at-least-once: manuelles Ack nach erfolgreicher Verarbeitung (kein
auto_ack). Stuerzt die Instanz vor dem Ack ab, requeued RabbitMQ die
Nachricht automatisch fuer einen anderen Consumer.

Jede Instanz schreibt ihr Ergebnis als eine Zeile in results_loadbalance.jsonl.
"""

import json
import sys
from datetime import datetime, timezone

import pika

QUEUE_NAME = "loadbalance.test"
RESULTS_FILE = "results_loadbalance.jsonl"
IDLE_TIMEOUT_SECONDS = 10.0
PREFETCH_COUNT = 50


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python lastverteilung_rabbitmq_consumer.py <run_id> <instanz_id>")
        sys.exit(1)

    run_id, instance_id = sys.argv[1], sys.argv[2]

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    processed_count = 0
    first_message_at = None

    print(f"[Instanz {instance_id}] Bereit, run_id '{run_id}'. Warte auf Nachrichten.")

    consumer_gen = channel.consume(
        QUEUE_NAME, inactivity_timeout=IDLE_TIMEOUT_SECONDS
    )
    try:
        for method, properties, body in consumer_gen:
            if method is None:
                # Kein Ereignis innerhalb des Timeouts: Lauf gilt als beendet
                break

            if first_message_at is None:
                first_message_at = datetime.now(timezone.utc)

            json.loads(body)  # Verarbeitung simuliert durch Parsen
            processed_count += 1

            # Erst nach erfolgreicher Verarbeitung acken (at-least-once)
            channel.basic_ack(delivery_tag=method.delivery_tag)
    except KeyboardInterrupt:
        pass
    finally:
        channel.cancel()
        connection.close()

    finished_at = datetime.now(timezone.utc)
    print(f"[Instanz {instance_id}] Fertig: {processed_count} Nachrichten verarbeitet.")

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
