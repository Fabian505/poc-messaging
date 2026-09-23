"""Use Case 4.2.4 (Entkopplung bei Ausfall und Zustellsemantiken)
- Consumer (Kafka-Variante), konfigurierbar fuer alle drei Zustellsemantiken.

Nutzung:
    python zustellsemantik_kafka_consumer.py <run_id> <semantik>

<semantik> ist eine von: at-most-once | at-least-once | exactly-once

Unterschied zwischen den drei Modi, technisch:

- at-most-once: Offset wird SOFORT nach dem Empfang committed, VOR der
  Verarbeitung. Stuerzt der Consumer waehrend der Verarbeitung ab, gilt die
  Nachricht aus Kafka-Sicht bereits als konsumiert und wird nach einem
  Neustart NICHT erneut zugestellt, sie ist dann fuer diesen Consumer
  verloren.
- at-least-once: Offset wird ERST NACH erfolgreicher Verarbeitung
  committed. Stuerzt der Consumer vorher ab, wird die Nachricht beim
  naechsten Start erneut zugestellt, moeglicherweise als Duplikat.
- exactly-once: wie at-least-once (Commit nach Verarbeitung), zusaetzlich
  wird auf Anwendungsebene ueber ein Set bereits verarbeiteter (producer_id,
  seq)-Paare dedupliziert. Das ist eine pragmatische Idempotent-Consumer-
  Loesung, kein Einsatz der Kafka-Transactions-API, da hier kein
  nachgelagertes Produce stattfindet, fuer das die Transactions-API
  eigentlich gedacht ist. Kombiniert mit einem idempotenten Producer
  (siehe zustellsemantik_kafka_producer.py) deckt das den in Kapitel 4.2.4
  beschriebenen Testfall ab.

Jede Instanz wird von run_fault_injection.py als Subprozess gestartet und
kann von dort aus gezielt mit SIGKILL beendet werden, um einen Absturz zu
simulieren. Bei manuellem Testen kann sie auch direkt gestartet und von
Hand abgebrochen werden.
"""

import json
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer

TOPIC = "semantics.test"
RESULTS_FILE = "results_semantics.jsonl"
IDLE_TIMEOUT_SECONDS = 10.0
VALID_SEMANTICS = {"at-most-once", "at-least-once", "exactly-once"}


def main():
    if len(sys.argv) != 3 or sys.argv[2] not in VALID_SEMANTICS:
        print(f"Nutzung: python zustellsemantik_kafka_consumer.py <run_id> <{'|'.join(VALID_SEMANTICS)}>")
        sys.exit(1)

    run_id, semantics = sys.argv[1], sys.argv[2]

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": run_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    seen_seqs = set()  # nur fuer exactly-once relevant
    print(f"[{semantics}] Bereit, run_id '{run_id}'. Warte auf Nachrichten.")

    with open(RESULTS_FILE, "a") as f:
        try:
            while True:
                msg = consumer.poll(IDLE_TIMEOUT_SECONDS)
                if msg is None:
                    break
                if msg.error():
                    print(f"Fehler: {msg.error()}")
                    continue

                if semantics == "at-most-once":
                    # Commit SOFORT, vor der Verarbeitung: bei einem Absturz
                    # danach ist die Nachricht fuer immer weg.
                    consumer.commit(message=msg, asynchronous=False)

                payload = json.loads(msg.value())
                seq = payload["seq"]

                if semantics == "exactly-once" and seq in seen_seqs:
                    # Duplikat durch Redelivery erkannt und uebersprungen,
                    # trotzdem protokollieren, um es in der Auswertung
                    # sichtbar zu machen.
                    f.write(json.dumps({
                        "technology": "kafka", "run_id": run_id,
                        "semantics": semantics, "seq": seq,
                        "received_at": datetime.now(timezone.utc).isoformat(),
                        "duplicate_skipped": True,
                    }) + "\n")
                    if semantics != "at-most-once":
                        consumer.commit(message=msg, asynchronous=False)
                    continue

                # "Verarbeitung" (hier: Parsen reicht als Stellvertreter)
                seen_seqs.add(seq)

                f.write(json.dumps({
                    "technology": "kafka", "run_id": run_id,
                    "semantics": semantics, "seq": seq,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "duplicate_skipped": False,
                }) + "\n")
                f.flush()

                if semantics != "at-most-once":
                    # at-least-once und exactly-once: Commit NACH Verarbeitung
                    consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
