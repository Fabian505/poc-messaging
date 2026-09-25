"""Diagnose-Variante von kafka_consumer.py: protokolliert zusaetzlich jede
einzelne (seq, latency)-Paarung in eine CSV-Datei, um zu pruefen, ob die
Latenz mit der Sequenznummer ansteigt (Hinweis auf Producer/Consumer-
Geschwindigkeits-Mismatch, analog zum IBM-MQ-Befund).

Nutzung identisch zu kafka_consumer.py, schreibt zusaetzlich
kafka_diagnose_seq_latency.csv.
"""

import csv
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer

from latency_stats import print_latency_summary

TOPIC = "latency.test"
WARMUP_COUNT = 10
MEASURE_COUNT = 10000
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT


def main():
    group_id = f"latency-test-{int(time.time())}"
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    latencies = []
    received_count = 0

    print(f"Bereit. Warte auf {TOTAL_COUNT} Nachrichten "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen). "
          "Jetzt den Producer starten.")

    with open("kafka_diagnose_seq_latency.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["seq", "latency_ms", "received_at"])

        try:
            while received_count < TOTAL_COUNT:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Fehler: {msg.error()}")
                    continue

                payload = json.loads(msg.value())
                seq = payload["seq"]
                sent_at = datetime.fromisoformat(payload["sent_at"])
                received_at = datetime.now(timezone.utc)
                latency = (received_at - sent_at).total_seconds()

                received_count += 1
                if seq > WARMUP_COUNT:
                    latencies.append(latency)
                    writer.writerow([seq, latency * 1000, received_at.isoformat()])

                consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    print_latency_summary(latencies, "Kafka")
    print("\nDetails in kafka_diagnose_seq_latency.csv, seq gegen latency_ms "
          "pruefen: steigt die Latenz mit steigendem seq deutlich an?")


if __name__ == "__main__":
    main()
