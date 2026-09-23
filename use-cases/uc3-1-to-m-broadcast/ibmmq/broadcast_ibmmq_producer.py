"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Producer (IBM MQ-Variante).

Nutzt das Publish/Subscribe-Modell ueber ein Topic statt einer Queue. Das
ist ein anderer Teil der pymqi-API als in allen bisherigen IBM-MQ-Skripten,
bitte im Kleinen (z.B. mit MESSAGE_COUNT = 10) zuerst gegen einen einzelnen
Subscriber testen, bevor grosse Laeufe gefahren werden.

WICHTIG: Alle Subscriber-Instanzen (broadcast_ibmmq_consumer.py) MUESSEN
bereits eine aktive Subscription auf das Topic haben, BEVOR hier publiziert
wird. Anders als bei einer Queue existiert bei einem nicht-dauerhaften
(non-durable) Abonnement kein Puffer, Nachrichten, die vor dem Abonnieren
gesendet werden, gehen fuer diesen Subscriber verloren.
"""

import json
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
USER = "app"
PASSWORD = "app12345"
TOPIC_STRING = "dev/broadcast"
MESSAGE_COUNT = 1_000

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)

    topic = pymqi.Topic(qmgr, topic_string=TOPIC_STRING)
    topic.open(open_opts=pymqi.CMQC.MQOO_OUTPUT)

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        topic.pub(json.dumps(payload).encode())

    topic.close()
    qmgr.disconnect()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten publiziert.")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
