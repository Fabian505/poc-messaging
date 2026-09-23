"""Latenzmessung Kapitel 6.1.1 - Consumer (IBM MQ-Variante).

WICHTIG: Diesen Consumer ZUERST starten und aktiv warten lassen, dann erst
den Producer starten.
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
MEASURE_COUNT = 1_000_000
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    gmo = pymqi.GMO(
        Options=pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING,
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

    queue.close()
    qmgr.disconnect()

    print_latency_summary(latencies, "IBM MQ")


if __name__ == "__main__":
    main()
