"""Use Case 4.2.2.1 (Lastverteilung) - Consumer (Kafka-Variante).

Fuer den Durchsatztest mehrfach parallel starten, z.B. fuer eine
Konfiguration mit 3 Instanzen in drei Terminals:

    python lastverteilung_kafka_consumer.py loadbalance-test-run1 1
    python lastverteilung_kafka_consumer.py loadbalance-test-run1 2
    python lastverteilung_kafka_consumer.py loadbalance-test-run1 3

Erstes Argument: gemeinsame Consumer-Group-ID fuer alle Instanzen dieses
Testlaufs (bei jedem neuen Testlauf/jeder neuen Konfiguration einen neuen,
noch nicht verwendeten Namen waehlen, sonst werden alte Offsets fortgesetzt
statt bei 'latest' zu beginnen).
Zweites Argument: Instanz-ID, nur fuer die Log-Ausgabe, hat keine technische
Funktion fuer die Lastverteilung selbst (die uebernimmt Kafka ueber die
gemeinsame Group-ID automatisch).

at-least-once: enable.auto.commit ist deaktiviert, das Offset wird erst
nach erfolgreicher Verarbeitung der Nachricht manuell committed. Stuerzt
die Instanz zwischen Verarbeitung und Commit ab, wird die Nachricht beim
naechsten Start erneut zugestellt (moegliche Duplikate, aber kein Verlust).

Jede Instanz schreibt ihr Ergebnis als eine Zeile in results_loadbalance.jsonl
(gemeinsame Datei, ein Eintrag pro Instanz), damit der Gesamtdurchsatz ueber
alle Instanzen einer Konfiguration hinweg ausgewertet werden kann.
"""

import json
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer

TOPIC = "loadbalance.test"
RESULTS_FILE = "results_loadbalance.jsonl"
IDLE_TIMEOUT_SECONDS = 10.0


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python lastverteilung_kafka_consumer.py <group_id> <instanz_id>")
        sys.exit(1)

    group_id, instance_id = sys.argv[1], sys.argv[2]

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    processed_count = 0
    first_message_at = None

    print(f"[Instanz {instance_id}] Bereit, Group '{group_id}'. Warte auf Nachrichten.")
    try:
        while True:
            msg = consumer.poll(IDLE_TIMEOUT_SECONDS)
            if msg is None:
                # Keine Nachricht innerhalb des Timeouts: Lauf gilt als beendet
                break
            if msg.error():
                print(f"[Instanz {instance_id}] Fehler: {msg.error()}")
                continue

            if first_message_at is None:
                first_message_at = datetime.now(timezone.utc)

            json.loads(msg.value())  # Verarbeitung simuliert durch Parsen
            processed_count += 1

            # Erst nach erfolgreicher Verarbeitung committen (at-least-once)
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    finished_at = datetime.now(timezone.utc)
    print(f"[Instanz {instance_id}] Fertig: {processed_count} Nachrichten verarbeitet.")

    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps({
            "technology": "kafka",
            "group_id": group_id,
            "instance_id": instance_id,
            "processed_count": processed_count,
            "first_message_at": first_message_at.isoformat() if first_message_at else None,
            "finished_at": finished_at.isoformat(),
        }) + "\n")


if __name__ == "__main__":
    main()
