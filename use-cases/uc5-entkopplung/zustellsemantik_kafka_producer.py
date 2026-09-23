"""Use Case 4.2.4 (Entkopplung bei Ausfall und Zustellsemantiken)
- Producer (Kafka-Variante).

Nutzung:
    python zustellsemantik_kafka_producer.py <semantik>

Fuer exactly-once wird enable.idempotence aktiviert, das dedupliziert
Producer-seitige Retries auf Broker-Ebene (z.B. bei einem kurzzeitigen
Verbindungsabbruch durch die Broker-Pause/Neustart-Fehlerinjektion) und ist
die Kafka-seitige Ergaenzung zur consumerseitigen Deduplizierung in
zustellsemantik_kafka_consumer.py.
"""

import json
import sys
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "semantics.test"
MESSAGE_COUNT = 200
VALID_SEMANTICS = {"at-most-once", "at-least-once", "exactly-once"}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in VALID_SEMANTICS:
        print(f"Nutzung: python zustellsemantik_kafka_producer.py <{'|'.join(VALID_SEMANTICS)}>")
        sys.exit(1)

    semantics = sys.argv[1]
    config = {"bootstrap.servers": "localhost:9092"}
    if semantics == "exactly-once":
        config["enable.idempotence"] = True

    producer = Producer(config)

    print(f"[{semantics}] Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(TOPIC, value=json.dumps(payload))
        producer.poll(0)

    producer.flush()
    print(f"[{semantics}] Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
