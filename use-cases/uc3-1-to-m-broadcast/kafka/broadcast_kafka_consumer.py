"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Consumer (Kafka-Variante).

Fuer den Broadcast-Test mehrfach parallel starten, z.B. mit 3 unabhaengigen
Consumern in drei Terminals:

    python broadcast_kafka_consumer.py broadcast-run-3 1
    python broadcast_kafka_consumer.py broadcast-run-3 2
    python broadcast_kafka_consumer.py broadcast-run-3 3

Anders als bei Lastverteilung bekommt hier JEDE Instanz eine EIGENE
Consumer-Group-ID (zusammengesetzt aus run_id und instanz_id), dadurch
liest jede Instanz das komplette Topic unabhaengig von den anderen, jede
sollte am Ende alle MESSAGE_COUNT Nachrichten erhalten haben, nicht nur
einen Teil davon.

at-least-once: enable.auto.commit ist deaktiviert, Offset wird erst nach
erfolgreicher Verarbeitung manuell committed, analog zu
lastverteilung_kafka_consumer.py.
"""

import json
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer

TOPIC = "broadcast.test"
RESULTS_FILE = "results_broadcast.jsonl"
IDLE_TIMEOUT_SECONDS = 10.0


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python broadcast_kafka_consumer.py <run_id> <instanz_id>")
        sys.exit(1)

    run_id, instance_id = sys.argv[1], sys.argv[2]
    unique_group_id = f"{run_id}-{instance_id}"

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": unique_group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    processed_count = 0
    first_message_at = None

    print(f"[Instanz {instance_id}] Bereit, eigene Group '{unique_group_id}'. Warte auf Nachrichten.")
    try:
        while True:
            msg = consumer.poll(IDLE_TIMEOUT_SECONDS)
            if msg is None:
                break
            if msg.error():
                print(f"[Instanz {instance_id}] Fehler: {msg.error()}")
                continue

            if first_message_at is None:
                first_message_at = datetime.now(timezone.utc)

            json.loads(msg.value())
            processed_count += 1
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    finished_at = datetime.now(timezone.utc)
    print(f"[Instanz {instance_id}] Fertig: {processed_count} Nachrichten erhalten.")

    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps({
            "technology": "kafka",
            "run_id": run_id,
            "instance_id": instance_id,
            "processed_count": processed_count,
            "first_message_at": first_message_at.isoformat() if first_message_at else None,
            "finished_at": finished_at.isoformat(),
        }) + "\n")


if __name__ == "__main__":
    main()
