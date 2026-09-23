"""Use Case 1: Einfache Warteschlange - Consumer (IBM MQ-Variante)."""

import json
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
QUEUE_NAME = "DEV.QUEUE.1"
USER = "app"
PASSWORD = "app12345"
MESSAGE_COUNT = 10

conn_info = f"{HOST}({PORT})"


def print_summary(received_seqs):
    expected = list(range(1, MESSAGE_COUNT + 1))
    in_order = received_seqs == sorted(received_seqs)
    missing = sorted(set(expected) - set(received_seqs))
    print("--- Zusammenfassung ---")
    print(f"Empfangen: {len(received_seqs)} von {MESSAGE_COUNT}")
    print(f"Reihenfolge korrekt: {in_order}")
    print(f"Fehlende Sequenznummern: {missing if missing else 'keine'}")


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    gmo = pymqi.GMO(
        Options=pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING,
        WaitInterval=5000,
    )

    received_seqs = []
    print(f"Warte auf {MESSAGE_COUNT} Nachrichten (5s Timeout pro Nachricht).")
    while len(received_seqs) < MESSAGE_COUNT:
        try:
            message = queue.get(None, pymqi.md(), gmo)
        except pymqi.MQMIError as e:
            if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                print("Keine weitere Nachricht verfuegbar, breche ab.")
                break
            raise

        payload = json.loads(message)
        seq = payload["seq"]
        sent_at = datetime.fromisoformat(payload["sent_at"])
        received_at = datetime.now(timezone.utc)
        latency = (received_at - sent_at).total_seconds()

        received_seqs.append(seq)
        print(f"Empfangen: seq={seq} latenz={latency:.3f}s")

    queue.close()
    qmgr.disconnect()

    print_summary(received_seqs)


if __name__ == "__main__":
    main()
