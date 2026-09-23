"""Use Case 4.2.2.1 (Lastverteilung) - Producer (IBM MQ-Variante).

Sendet an DEV.QUEUE.2, dieselbe Queue wie bei der Latenzmessung. Wie bei
RabbitMQ verteilt sich die Last automatisch auf alle Consumer-Instanzen,
die gleichzeitig von derselben Queue lesen, keine besondere Konfiguration
noetig.

WICHTIG: Vor jedem Testlauf pruefen, dass die Queue leer ist, analog zur
RabbitMQ-Praxis:

    podman exec ibmmq bash -c "echo 'DIS QL(DEV.QUEUE.2) CURDEPTH' | runmqsc QM1"

Consumer-Instanzen VORHER starten.
"""

import json
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
QUEUE_NAME = "DEV.QUEUE.2"
USER = "app"
PASSWORD = "app12345"
MESSAGE_COUNT = 10_000

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    for seq in range(1, MESSAGE_COUNT + 1):
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        queue.put(json.dumps(payload).encode())

    queue.close()
    qmgr.disconnect()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
