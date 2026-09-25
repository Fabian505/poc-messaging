"""Diagnose-Variante von ibmmq_consumer.py: protokolliert zusaetzlich jede
einzelne (seq, latency)-Paarung in eine CSV-Datei, um zu pruefen, ob die
Latenz mit der Sequenznummer ansteigt (Hinweis auf ein
Geschwindigkeits-Mismatch zwischen Producer und Consumer innerhalb des
Laufs, nicht auf Backlog aus einem frueheren Lauf).

Nutzung identisch zu ibmmq_consumer.py, schreibt zusaetzlich
diagnose_seq_latency.csv.
"""

import csv
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
MEASURE_COUNT = 10000
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

    with open("diagnose_seq_latency.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["seq", "latency_ms", "received_at"])

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
                writer.writerow([seq, latency * 1000, received_at.isoformat()])

            qmgr.commit()

    queue.close()
    qmgr.disconnect()

    print_latency_summary(latencies, "IBM MQ")
    print("\nDetails in diagnose_seq_latency.csv, seq gegen latency_ms plotten "
          "oder mit Excel/Tabellenkalkulation ansehen: steigt die Latenz mit "
          "steigendem seq deutlich an?")


if __name__ == "__main__":
    main()
