"""Use Case 4.2.2.1 (Lastverteilung) - Producer (Kafka-Variante).

Sendet an das Topic 'loadbalance.test', das VOR dem ersten Lauf einmalig mit
mehreren Partitionen angelegt werden muss, sonst kann Kafka die Last nicht
auf mehrere gleichzeitige Consumer-Instanzen verteilen:

    podman exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
        --topic loadbalance.test \
        --bootstrap-server localhost:9092 \
        --partitions 8 \
        --replication-factor 1

8 Partitionen reichen fuer bis zu 8 gleichzeitig aktive Consumer-Instanzen
in derselben Consumer Group, mehr Instanzen als Partitionen blieben sonst
untaetig.

WICHTIG: Consumer-Instanzen VORHER starten, damit die Consumer Group
bereits gejoined ist, bevor der Producer sendet.
"""

import json
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "loadbalance.test"
MESSAGE_COUNT = 10_000


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
