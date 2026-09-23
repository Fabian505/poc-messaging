"""Use Case 4.2.4 - Producer (IBM MQ-Variante) mit Wiederverbindung.

Nur fuer den Fehlertyp "Broker-Neustart waehrend des Sendens" gedacht,
analog zu zustellsemantik_rabbitmq_producer_resilient.py. pymqi hat keine
eingebaute Wiederverbindungslogik, bei einem MQMIError durch den
Broker-Neustart wird neu verbunden und die aktuelle Nachricht erneut
versucht.
"""

import json
import sys
import time
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
QUEUE_NAME = "DEV.QUEUE.2"
USER = "app"
PASSWORD = "app12345"
MESSAGE_COUNT = 200
MAX_RECONNECT_ATTEMPTS = 15
RECONNECT_DELAY_SECONDS = 1.0

conn_info = f"{HOST}({PORT})"


def connect():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)
    return qmgr, queue


def main():
    qmgr, queue = connect()
    print(f"Start: {datetime.now(timezone.utc).isoformat()}")

    seq = 1
    reconnect_attempts = 0
    while seq <= MESSAGE_COUNT:
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            queue.put(json.dumps(payload).encode())
            seq += 1
            reconnect_attempts = 0
        except pymqi.MQMIError:
            reconnect_attempts += 1
            if reconnect_attempts > MAX_RECONNECT_ATTEMPTS:
                print(f"Abbruch nach {MAX_RECONNECT_ATTEMPTS} erfolglosen Wiederverbindungsversuchen.")
                sys.exit(1)
            print(f"Verbindung verloren bei seq={seq}, Wiederverbindungsversuch {reconnect_attempts}...")
            time.sleep(RECONNECT_DELAY_SECONDS)
            try:
                queue.close()
                qmgr.disconnect()
            except Exception:
                pass
            qmgr, queue = connect()

    queue.close()
    qmgr.disconnect()
    print(f"Fertig: {MESSAGE_COUNT} Nachrichten gesendet.")


if __name__ == "__main__":
    main()
