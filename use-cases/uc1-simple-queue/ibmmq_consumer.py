"""Latenzmessung Kapitel 6.1.1 - Consumer (IBM MQ-Variante).

WICHTIG: Diesen Consumer ZUERST starten und aktiv warten lassen, dann erst
den Producer starten.

at-least-once: MQGMO_SYNCPOINT plus explizitem qmgr.commit() nach
erfolgreicher Verarbeitung, konsistent mit den uebrigen Use Cases (siehe
Kapitel 4.2 / 5.2). Die urspruengliche Fassung ohne Syncpoint entsprach
unbeabsichtigt einer at-most-once-Semantik (siehe Kapitel 5.2), das war
inkonsistent zu den anderen Use Cases.

ACHTUNG: Diese Aenderung fuegt pro Nachricht einen zusaetzlichen Commit
hinzu, das kann die gemessene Latenz gegenueber vorherigen Laeufen mit der
alten Fassung veraendern. Bereits erhobene IBM-MQ-Latenzwerte aus Kapitel
6.1.1 mit der alten, nicht-transaktionalen Fassung sind mit dieser Version
NICHT direkt vergleichbar und sollten mit dieser Fassung neu erhoben
werden, damit alle drei Technologien unter derselben Zustellsemantik
gemessen sind.
"""

import json
from datetime import datetime, timezone

import pymqi

from latency_stats import print_latency_summary

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
QUEUE_NAME = "DEV.QUEUE.2"
USER = "app"
PASSWORD = "app12345"
WARMUP_COUNT = 10
MEASURE_COUNT = 100
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    gmo = pymqi.GMO(
        Options=(
            pymqi.CMQC.MQGMO_WAIT
            | pymqi.CMQC.MQGMO_SYNCPOINT
            | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        ),
        WaitInterval=30000,
    )

    latencies = []
    received_count = 0

    print(f"Bereit. Warte auf {TOTAL_COUNT} Nachrichten "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen). "
          "Jetzt den Producer starten.")
    while received_count < TOTAL_COUNT:
        try:
            message = queue.get(None, pymqi.md(), gmo)
        except pymqi.MQMIError as e:
            if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                print("Timeout ohne neue Nachricht, breche ab.")
                break
            raise

        payload = json.loads(message)
        seq = payload["seq"]
        sent_at = datetime.fromisoformat(payload["sent_at"])
        received_at = datetime.now(timezone.utc)
        latency = (received_at - sent_at).total_seconds()

        received_count += 1
        if seq > WARMUP_COUNT:
            latencies.append(latency)

        # Erst nach erfolgreicher Verarbeitung committen (at-least-once)
        qmgr.commit()

    queue.close()
    qmgr.disconnect()

    print_latency_summary(latencies, "IBM MQ")


if __name__ == "__main__":
    main()
