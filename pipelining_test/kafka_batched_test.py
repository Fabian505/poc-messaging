"""Isolierter Test: Kafka mit gruppierten Sendungen (Fenster) und optional
fsync je Produce-Request. NICHT Teil der offiziellen Messreihe -- Zusatzbefund
fuer Kapitel 7, Gegenstueck zu rabbitmq_pipelined_test.py / ibmmq_batched_test.py.

Ein Fenster = WINDOW Nachrichten werden ohne Warten produziert, dann wartet
flush() auf alle Delivery Reports (acks=all). Mit FSYNC=1 hat das Test-Topic
flush.messages=1: der Broker fsynct nach jedem angehaengten Batch, also EINMAL
je Produce-Request statt je Nachricht -- das ist das Gruppen-Commit-Aequivalent.
WINDOW=1 entspricht dem bisherigen Verfahren (produce+flush je Nachricht).

Jeder Lauf legt ein eigenes Topic an (Name mit Zeitstempel) und loescht es am
Ende: keine globale Konfiguration wird veraendert, kein Zuruecksetzen noetig.

Env: MEASURE_COUNT (Default 2000), WINDOW (Default 50), FSYNC (Default 1)
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:9092"
MEASURE_COUNT = int(os.environ.get("MEASURE_COUNT", 2000))
WINDOW = int(os.environ.get("WINDOW", 50))
FSYNC = os.environ.get("FSYNC", "1") == "1"


def main():
    topic = f"pipeline_test_{int(time.time() * 1000)}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        admin.list_topics(timeout=10)
    except Exception as e:  # keine Verbindung
        sys.exit(f"FEHLER: Kafka unter {BOOTSTRAP} nicht erreichbar: {e}")

    config = {"flush.messages": "1"} if FSYNC else {}
    for name, fut in admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1, config=config)]).items():
        fut.result(30)

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "linger.ms": 5})
    # warten, bis das Topic einen Leader hat
    for _ in range(50):
        md = producer.list_topics(topic, timeout=5).topics.get(topic)
        if md is not None and md.partitions and md.partitions[0].leader >= 0:
            break
        time.sleep(0.2)

    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(err)

    print(f"Start: {MEASURE_COUNT} Nachrichten, Fenstergroesse {WINDOW}, "
          f"fsync je Produce-Request: {'ja (flush.messages=1)' if FSYNC else 'nein (Standard)'}")
    t0 = time.perf_counter()
    for seq in range(1, MEASURE_COUNT + 1):
        producer.produce(topic, value=json.dumps(
            {"seq": seq, "sent_at": datetime.now(timezone.utc).isoformat()}).encode(),
            on_delivery=on_delivery)
        if seq % WINDOW == 0:
            producer.flush()  # EIN Warten fuer WINDOW Nachrichten
    producer.flush()
    elapsed = time.perf_counter() - t0

    for name, fut in admin.delete_topics([topic], operation_timeout=30).items():
        try:
            fut.result(30)
        except Exception as e:
            print(f"WARNUNG: Topic {name} nicht geloescht: {e}", file=sys.stderr)

    print(f"Fertig in {elapsed:.2f}s -> {MEASURE_COUNT / elapsed:.1f} Nachrichten/s "
          f"(Zustellfehler: {len(errors)}; Vergleich: WINDOW=1 im selben Skript)")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
