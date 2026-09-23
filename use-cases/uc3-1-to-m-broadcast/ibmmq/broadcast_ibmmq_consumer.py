"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Subscriber (IBM MQ-Variante).

Fuer den Broadcast-Test mehrfach parallel starten, z.B. mit 3 unabhaengigen
Subscribern in drei Terminals:

    python broadcast_ibmmq_consumer.py broadcast-run-3 1
    python broadcast_ibmmq_consumer.py broadcast-run-3 2
    python broadcast_ibmmq_consumer.py broadcast-run-3 3

Jede Instanz erstellt eine eigene, nicht-dauerhafte (non-durable), von MQ
verwaltete (managed) Subscription auf das Topic. MQ legt dafuer intern eine
temporaere Queue an, aus der ganz normal mit get() gelesen werden kann.

Achtung, unbedingt zuerst im Kleinen testen: die genaue pymqi-API fuer
Subscriptions (Klassen-/Methodennamen, Options-Konstanten) variiert je nach
installierter pymqi-Version staerker als bei der einfachen Queue-API. Der
folgende Code zeigt das uebliche Muster, muss aber ggf. an eure konkrete
pymqi-Version angepasst werden (siehe pymqi-Dokumentation/Beispiele zu
'Subscription' und 'MQSO_MANAGED').

at-least-once ueber MQGMO_SYNCPOINT und explizites qmgr.commit() nach
erfolgreicher Verarbeitung, analog zu lastverteilung_ibmmq_consumer.py.
"""

import json
import sys
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
USER = "app"
PASSWORD = "app12345"
TOPIC_STRING = "dev/broadcast"
RESULTS_FILE = "results_broadcast.jsonl"
IDLE_TIMEOUT_MS = 10_000

conn_info = f"{HOST}({PORT})"


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python broadcast_ibmmq_consumer.py <run_id> <instanz_id>")
        sys.exit(1)

    run_id, instance_id = sys.argv[1], sys.argv[2]

    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)

    sub_desc = pymqi.SD()
    sub_desc.Options = (
        pymqi.CMQC.MQSO_CREATE
        | pymqi.CMQC.MQSO_NON_DURABLE
        | pymqi.CMQC.MQSO_MANAGED
    )
    sub_desc.set_vs("ObjectString", TOPIC_STRING)

    subscription = pymqi.Subscription(qmgr)
    subscription.sub(sub_desc=sub_desc)
    managed_queue = subscription.sub_queue

    gmo = pymqi.GMO(
        Options=(
            pymqi.CMQC.MQGMO_WAIT
            | pymqi.CMQC.MQGMO_SYNCPOINT
            | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        ),
        WaitInterval=IDLE_TIMEOUT_MS,
    )

    processed_count = 0
    first_message_at = None

    print(f"[Instanz {instance_id}] Abonniert '{TOPIC_STRING}'. Warte auf Nachrichten.")
    try:
        while True:
            try:
                message = managed_queue.get(None, pymqi.md(), gmo)
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    break
                raise

            if first_message_at is None:
                first_message_at = datetime.now(timezone.utc)

            json.loads(message)
            processed_count += 1
            qmgr.commit()
    except KeyboardInterrupt:
        pass
    finally:
        managed_queue.close()
        subscription.close(sub_close_options=0, close_sub_queue=False)
        qmgr.disconnect()

    finished_at = datetime.now(timezone.utc)
    print(f"[Instanz {instance_id}] Fertig: {processed_count} Nachrichten erhalten.")

    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps({
            "technology": "ibmmq",
            "run_id": run_id,
            "instance_id": instance_id,
            "processed_count": processed_count,
            "first_message_at": first_message_at.isoformat() if first_message_at else None,
            "finished_at": finished_at.isoformat(),
        }) + "\n")

if __name__ == "__main__":
    main()