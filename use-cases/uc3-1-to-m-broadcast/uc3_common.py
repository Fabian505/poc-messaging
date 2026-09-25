"""Gemeinsame Hilfsfunktionen fuer die UC3-Subscriber (Event-basierte
Benachrichtigung / Broadcast).

Parameter per Umgebungsvariable (setzt run_uc3_measurement.py):
    RUN_ID, INSTANCE_ID, RESULT_DIR, MEASURE_COUNT
    IDLE_TIMEOUT_SECONDS   Ende, wenn nach der ersten Nachricht so lange
                           nichts mehr kommt (Standard 10)
    START_TIMEOUT_SECONDS  maximale Wartezeit auf die erste Nachricht
                           (Standard 120)

Jeder Subscriber zeichnet pro Sequenznummer den Sende- und den ERSTEN
Empfangszeitpunkt auf. Damit lassen sich getrennt pruefen:
  - Vollstaendigkeit pro Subscriber (eindeutige seq, nicht bloss Anzahl)
  - Duplikate (unter at-least-once zulaessig, werden aber ausgewiesen)
  - Latenz pro Subscriber
  - zeitlicher Versatz derselben Nachricht zwischen den Subscribern
Der Subscriber beendet sich, sobald er alle seq erhalten hat.
"""

import json
import os
import time
from datetime import datetime

RUN_ID = os.environ["RUN_ID"]
INSTANCE_ID = os.environ["INSTANCE_ID"]
RESULT_DIR = os.environ["RESULT_DIR"]
WARMUP_COUNT = 10
MEASURE_COUNT = int(os.environ["MEASURE_COUNT"])
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT
IDLE_TIMEOUT_SECONDS = float(os.environ.get("IDLE_TIMEOUT_SECONDS", 10))
START_TIMEOUT_SECONDS = float(os.environ.get("START_TIMEOUT_SECONDS", 120))


class Recorder:
    def __init__(self):
        self.first = {}  # seq -> (sent_ts, recv_ts)
        self.received = 0
        self.duplicates = 0
        self._last_activity = time.monotonic()

    def record(self, body):
        payload = json.loads(body)
        recv_ts = time.time()
        seq = payload["seq"]
        self.received += 1
        self._last_activity = time.monotonic()
        if seq in self.first:
            self.duplicates += 1
        else:
            sent_ts = datetime.fromisoformat(payload["sent_at"]).timestamp()
            self.first[seq] = (sent_ts, recv_ts)

    @property
    def complete(self):
        return len(self.first) >= TOTAL_COUNT

    def timed_out(self):
        limit = IDLE_TIMEOUT_SECONDS if self.received else START_TIMEOUT_SECONDS
        return time.monotonic() - self._last_activity > limit

    def write(self, technology):
        seqs = sorted(self.first)
        path = os.path.join(RESULT_DIR, f"{INSTANCE_ID}.json")
        with open(path, "w") as f:
            json.dump({
                "technology": technology,
                "run_id": RUN_ID,
                "instance_id": INSTANCE_ID,
                "received": self.received,
                "unique": len(seqs),
                "duplicates": self.duplicates,
                "seq": seqs,
                "sent": [self.first[s][0] for s in seqs],
                "recv": [self.first[s][1] for s in seqs],
            }, f)
        print(f"[Subscriber {INSTANCE_ID}] Fertig: {len(seqs)}/{TOTAL_COUNT} eindeutig, "
              f"{self.duplicates} Duplikate.", flush=True)
