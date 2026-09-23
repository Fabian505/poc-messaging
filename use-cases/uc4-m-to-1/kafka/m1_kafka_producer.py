"""Use Case 4.2.3 (m:1) - Producer (Kafka-Variante).

Mehrfach parallel mit unterschiedlicher producer_id starten, z.B. fuer eine
Konfiguration mit 5 Producern in fuenf Terminals:

    python m1_kafka_producer.py m1-run-5 1
    python m1_kafka_producer.py m1-run-5 2
    ... bis producer_id 5

Sendet an das Topic 'aggregation.test' (einmalig mit 1 Partition anlegen,
siehe unten), damit alle Nachrichten in einem einzigen, total geordneten Log
landen, das vereinfacht die spaetere Reihenfolge-Auswertung:

    podman exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
        --topic aggregation.test \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1

WICHTIG: Consumer VORHER starten. Alle Producer-Instanzen moeglichst
gleichzeitig starten (z.B. per Skript oder kurz nacheinander in getrennten
Terminals), damit tatsaechlich nebenlaeufig gesendet wird.
"""

import json
import sys
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "aggregation.test"
MESSAGE_COUNT_PER_PRODUCER = 1_000


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python m1_kafka_producer.py <run_id> <producer_id>")
        sys.exit(1)

    run_id, producer_id = sys.argv[1], sys.argv[2]
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print(f"[Producer {producer_id}] Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT_PER_PRODUCER + 1):
        payload = {
            "run_id": run_id,
            "producer_id": producer_id,
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(TOPIC, value=json.dumps(payload))
        producer.poll(0)

    producer.flush()
    print(f"[Producer {producer_id}] Fertig: {MESSAGE_COUNT_PER_PRODUCER} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
