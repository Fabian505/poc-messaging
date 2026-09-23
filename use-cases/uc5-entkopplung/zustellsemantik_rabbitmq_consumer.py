"""Use Case 4.2.4 (Entkopplung bei Ausfall und Zustellsemantiken)
- Consumer (RabbitMQ-Variante), konfigurierbar fuer alle drei Zustellsemantiken.

Nutzung:
    python zustellsemantik_rabbitmq_consumer.py <run_id> <semantik>

<semantik> ist eine von: at-most-once | at-least-once | exactly-once

- at-most-once: auto_ack=True, RabbitMQ markiert die Nachricht bereits beim
  Zustellen als erledigt, VOR der Verarbeitung. Stuerzt der Consumer waehrend
  der Verarbeitung ab, ist die Nachricht endgueltig weg.
- at-least-once: manuelles Ack ERST NACH erfolgreicher Verarbeitung. Stuerzt
  der Consumer vorher ab, requeued RabbitMQ die Nachricht automatisch.
- exactly-once: wie at-least-once, zusaetzlich Deduplizierung auf
  Anwendungsebene ueber ein Set bereits verarbeiteter seq-Werte, da
  RabbitMQ selbst keine exactly-once-Garantie bietet.
"""

import json
import sys
from datetime import datetime, timezone

import pika

QUEUE_NAME = "semantics.test"
RESULTS_FILE = "results_semantics.jsonl"
IDLE_TIMEOUT_SECONDS = 10.0
VALID_SEMANTICS = {"at-most-once", "at-least-once", "exactly-once"}


def main():
    if len(sys.argv) != 3 or sys.argv[2] not in VALID_SEMANTICS:
        print(f"Nutzung: python zustellsemantik_rabbitmq_consumer.py <run_id> <{'|'.join(VALID_SEMANTICS)}>")
        sys.exit(1)

    run_id, semantics = sys.argv[1], sys.argv[2]

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=10)

    seen_seqs = set()  # nur fuer exactly-once relevant
    auto_ack = semantics == "at-most-once"

    print(f"[{semantics}] Bereit, run_id '{run_id}'. Warte auf Nachrichten.")

    consumer_gen = channel.consume(
        QUEUE_NAME, auto_ack=auto_ack, inactivity_timeout=IDLE_TIMEOUT_SECONDS
    )
    with open(RESULTS_FILE, "a") as f:
        try:
            for method, properties, body in consumer_gen:
                if method is None:
                    break

                payload = json.loads(body)
                seq = payload["seq"]

                if semantics == "exactly-once" and seq in seen_seqs:
                    f.write(json.dumps({
                        "technology": "rabbitmq", "run_id": run_id,
                        "semantics": semantics, "seq": seq,
                        "received_at": datetime.now(timezone.utc).isoformat(),
                        "duplicate_skipped": True,
                    }) + "\n")
                    if not auto_ack:
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                    continue

                seen_seqs.add(seq)

                f.write(json.dumps({
                    "technology": "rabbitmq", "run_id": run_id,
                    "semantics": semantics, "seq": seq,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "duplicate_skipped": False,
                }) + "\n")
                f.flush()

                if not auto_ack:
                    # at-least-once und exactly-once: Ack NACH Verarbeitung
                    channel.basic_ack(delivery_tag=method.delivery_tag)
        except KeyboardInterrupt:
            pass
        finally:
            channel.cancel()
            connection.close()


if __name__ == "__main__":
    main()
