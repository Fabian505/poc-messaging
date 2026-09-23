"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Producer (Kafka-Variante).

Sendet an das Topic 'broadcast.test'. Im Gegensatz zu Lastverteilung ist die
Partitionsanzahl hier nicht entscheidend, da jede Consumer-Instanz mit einer
eigenen Consumer-Group-ID das komplette Topic unabhaengig liest, eine
Partition genuegt:

    podman exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
        --topic broadcast.test \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1

WICHTIG: Alle Consumer-Instanzen VORHER starten, damit jede ihre eigene
Consumer Group bereits gejoined hat, bevor der Producer sendet.
"""

import json
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "broadcast.test"
MESSAGE_COUNT = 1_000


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(TOPIC, value=json.dumps(payload))
        producer.poll(0)

    producer.flush()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
