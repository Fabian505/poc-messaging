"""Use Case 4.2.3 (m:1) - Consumer (Kafka-Variante).

Nur EINE Instanz starten, alle Producer schreiben in dasselbe Topic. Jede
empfangene Nachricht wird als eigene Zeile in results_m1.jsonl protokolliert
(nicht nur ein Zaehler), damit analyze_m1.py anschliessend pro Producer
pruefen kann, ob wirklich JEDE einzelne Sequenznummer angekommen ist, und
in welcher Reihenfolge die Nachrichten der verschiedenen Producer beim
Consumer eintrafen.

at-least-once ueber manuellen Commit nach Verarbeitung, analog zu den
bisherigen m:1-Geschwister-Use-Cases.
"""

import json
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer

TOPIC = "aggregation.test"
RESULTS_FILE = "results_m1.jsonl"
IDLE_TIMEOUT_SECONDS = 10.0


def main():
    if len(sys.argv) != 2:
        print("Nutzung: python m1_kafka_consumer.py <run_id>")
        sys.exit(1)

    run_id = sys.argv[1]
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": run_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    received_count = 0
    print(f"Bereit, run_id '{run_id}'. Warte auf Nachrichten von allen Producern.")

    with open(RESULTS_FILE, "a") as f:
        try:
            while True:
                msg = consumer.poll(IDLE_TIMEOUT_SECONDS)
                if msg is None:
                    break
                if msg.error():
                    print(f"Fehler: {msg.error()}")
                    continue

                payload = json.loads(msg.value())
                received_at = datetime.now(timezone.utc).isoformat()

                f.write(json.dumps({
                    "technology": "kafka",
                    "run_id": payload["run_id"],
                    "producer_id": payload["producer_id"],
                    "seq": payload["seq"],
                    "sent_at": payload["sent_at"],
                    "received_at": received_at,
                }) + "\n")
                received_count += 1

                consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    print(f"Fertig: {received_count} Nachrichten insgesamt empfangen.")


if __name__ == "__main__":
    main()
