"""Isolierter Test: IBM MQ mit mehreren put() unter einem gemeinsamen
Syncpoint (ein Commit pro Batch statt pro Nachricht). NICHT Teil der
offiziellen Messreihe -- eigenstaendig ausfuehren, Ergebnis nur als
Zusatzbefund fuer Kapitel 7 verwenden.

Env: MEASURE_COUNT (Default 2000), WINDOW (Default 50, Batchgroesse pro Commit)
"""
import json
import os
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
MEASURE_COUNT = int(os.environ.get("MEASURE_COUNT", 2000))
WINDOW = int(os.environ.get("WINDOW", 50))

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)
    pmo = pymqi.PMO(Options=pymqi.CMQC.MQPMO_SYNCPOINT)

    print(f"Start: {MEASURE_COUNT} Nachrichten, Fenstergroesse {WINDOW}")
    t0 = time.perf_counter()
    for seq in range(1, MEASURE_COUNT + 1):
        md = pymqi.MD()
        md.Persistence = pymqi.CMQC.MQPER_PERSISTENT
        queue.put(json.dumps({"seq": seq, "sent_at": datetime.now(timezone.utc).isoformat()}).encode(),
                   md, pmo)
        if seq % WINDOW == 0:
            qmgr.commit()  # EIN Commit fuer WINDOW Nachrichten
    qmgr.commit()  # Rest des letzten, unvollstaendigen Fensters
    elapsed = time.perf_counter() - t0
    queue.close()
    qmgr.disconnect()

    print(f"Fertig in {elapsed:.2f}s -> {MEASURE_COUNT / elapsed:.1f} Nachrichten/s "
          f"(zum Vergleich: CAP_RATE ohne Batching = 130/s)")


if __name__ == "__main__":
    main()
