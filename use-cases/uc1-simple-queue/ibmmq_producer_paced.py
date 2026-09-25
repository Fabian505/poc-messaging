"""Latenzmessung Kapitel 6.1.1 - Producer (IBM MQ-Variante, getaktet).

Sendet mit fester Soll-Last, damit sich innerhalb des Laufs keine
Warteschlange aufbaut (Consumer mit MQGMO_SYNCPOINT + Commit schafft auf
dem Laptop ca. 1430 Nachrichten/s, siehe ibmmq_consumer_seqdiagnose.py).

Taktung: fahrplanbasiert (absolute Sendezeitpunkte im Abstand
INTERVAL_SECONDS), NICHT sleep() nach dem Senden. Damit ist die angebotene
Last fuer alle drei Technologien identisch (500 Nachrichten/s) und haengt
nicht von der technologiespezifischen Sendedauer ab. Die tatsaechlich
erreichte Rate wird am Ende ausgegeben und gehoert in den Messbericht.

Persistenz: MQPER_PERSISTENT explizit, konsistent zu RabbitMQ
delivery_mode=2. Das MD MUSS an put() uebergeben werden, sonst gilt die
Queue-Voreinstellung DEFPSIST (bei DEV.QUEUE.2: nicht persistent). Genau
dieser Fehler steckte in der ersten Korrektur (md erzeugt, aber nicht
uebergeben), nachgewiesen mit amqsbcg (Persistence : 0).

Nur fuer den Vorher-Nachher-Vergleich: MQ_PERSISTENT=0 sendet nicht
persistent. Fuer berichtete Werte NICHT verwenden.
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
WARMUP_COUNT = 10
MEASURE_COUNT = int(os.environ.get("MEASURE_COUNT", 10000))
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT
TARGET_RATE = float(os.environ.get("TARGET_RATE", 500))  # Soll-Last in Nachrichten/s
INTERVAL_SECONDS = 1 / TARGET_RATE  # Standard 500/s = 2 ms, fuer alle drei identisch
PERSISTENT = os.environ.get("MQ_PERSISTENT", "1") != "0"
PERSISTENCE = pymqi.CMQC.MQPER_PERSISTENT if PERSISTENT else pymqi.CMQC.MQPER_NOT_PERSISTENT

conn_info = f"{HOST}({PORT})"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    print(f"Start: {datetime.now(timezone.utc).isoformat()} "
          f"(Persistenz: {'ja' if PERSISTENT else 'NEIN, nur Diagnose'})")

    behind_count = 0
    t_start = time.perf_counter()
    next_send = t_start
    for seq in range(1, TOTAL_COUNT + 1):
        md = pymqi.MD()
        md.Persistence = PERSISTENCE
        payload = {
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        queue.put(json.dumps(payload).encode(), md)

        next_send += INTERVAL_SECONDS
        delay = next_send - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        else:
            # Im Rueckstand: nicht in Bursts nachholen, Fahrplan neu ansetzen
            next_send = time.perf_counter()
            behind_count += 1

    elapsed = time.perf_counter() - t_start
    queue.close()
    qmgr.disconnect()
    print(f"Fertig: {TOTAL_COUNT} Nachrichten gesendet "
          f"({WARMUP_COUNT} Warmup, {MEASURE_COUNT} gemessen), "
          f"getaktet mit {INTERVAL_SECONDS * 1000:.1f} ms Intervall.")
    print(f"Tatsaechliche Rate: {TOTAL_COUNT / elapsed:.1f} Nachrichten/s "
          f"(Soll: {1 / INTERVAL_SECONDS:.0f}), Intervalle im Rueckstand: {behind_count}")
    print(f"End: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
