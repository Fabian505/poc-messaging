"""Gemeinsame Hilfsfunktionen fuer UC4 (m:1).

Parameter per Umgebungsvariable (setzt run_uc4_measurement.py):
    RUN_ID, RESULT_DIR, START_FLAG
    PRODUCER_COUNT      Anzahl gleichzeitiger Producer M
    MEASURE_COUNT       gemessene Nachrichten PRO Producer (+ WARMUP_COUNT)
    PRODUCER_ID         nur bei Producern

Lastmodell: Die GESAMTLAST ist unabhaengig von M konstant 500 Nachrichten/s
(wie UC1). Jeder der M Producer sendet im Abstand M x 2 ms, versetzt um
(PRODUCER_ID-1) x 2 ms. Die Sendezeitpunkte aller Producer greifen dadurch
lueckenlos ineinander, das Lastprofil beim Consumer entspricht UC1. Ein
Unterschied zu UC1 kann damit nur aus der Nebenlaeufigkeit mehrerer
Producer entstehen, nicht aus hoeherer Last.

Gemeinsamer Start: Der Orchestrator schreibt einen absoluten Startzeitpunkt
(Unix-Zeit) in START_FLAG, alle Producer richten ihren Fahrplan daran aus.
"""

import json
import os
import time
from datetime import datetime, timezone

RUN_ID = os.environ["RUN_ID"]
RESULT_DIR = os.environ["RESULT_DIR"]
START_FLAG = os.environ["START_FLAG"]
PRODUCER_COUNT = int(os.environ["PRODUCER_COUNT"])
WARMUP_COUNT = 10
MEASURE_COUNT = int(os.environ["MEASURE_COUNT"])
TOTAL_PER_PRODUCER = WARMUP_COUNT + MEASURE_COUNT
BASE_INTERVAL = 0.002
IDLE_TIMEOUT_SECONDS = float(os.environ.get("IDLE_TIMEOUT_SECONDS", 10))
START_TIMEOUT_SECONDS = float(os.environ.get("START_TIMEOUT_SECONDS", 120))


# ---------------------------------------------------------------- Producer

def run_paced_producer(send_fn, idle_fn=None):
    producer_id = int(os.environ["PRODUCER_ID"])
    interval = BASE_INTERVAL * PRODUCER_COUNT
    offset = BASE_INTERVAL * (producer_id - 1)

    print(f"[Producer {producer_id}] Bereit", flush=True)
    while not os.path.exists(START_FLAG):
        idle_fn() if idle_fn else time.sleep(0.01)
    with open(START_FLAG) as f:
        t0 = float(f.read().strip())

    next_send = t0 + offset
    behind = 0
    first_send = None
    for seq in range(1, TOTAL_PER_PRODUCER + 1):
        delay = next_send - time.time()
        if delay > 0:
            time.sleep(delay)
        if first_send is None:
            first_send = time.time()
        send_fn(json.dumps({
            "run_id": RUN_ID,
            "producer_id": producer_id,
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }))
        next_send += interval
        if next_send < time.time():
            # Senden dauerte laenger als das Intervall: nicht in Bursts nachholen
            next_send = time.time()
            behind += 1

    elapsed = time.time() - first_send
    print(f"Tatsaechliche Rate: {TOTAL_PER_PRODUCER / elapsed:.1f} Nachrichten/s "
          f"(Soll: {1 / interval:.0f}), Intervalle im Rueckstand: {behind}", flush=True)


# ---------------------------------------------------------------- Consumer

class Recorder:
    """Speichert Nachrichten in Ankunftsreihenfolge (nur Erstempfang je
    Producer und seq), Duplikate werden je Producer gezaehlt."""

    def __init__(self):
        self.seen = set()
        self.arrivals = []  # [producer_id, seq, sent_ts, recv_ts]
        self.duplicates = {}
        self.received = 0
        self._last_activity = time.monotonic()

    def record(self, body):
        payload = json.loads(body)
        recv_ts = time.time()
        key = (payload["producer_id"], payload["seq"])
        self.received += 1
        self._last_activity = time.monotonic()
        if key in self.seen:
            pid = str(payload["producer_id"])
            self.duplicates[pid] = self.duplicates.get(pid, 0) + 1
            return
        self.seen.add(key)
        sent_ts = datetime.fromisoformat(payload["sent_at"]).timestamp()
        self.arrivals.append([payload["producer_id"], payload["seq"], sent_ts, recv_ts])

    @property
    def complete(self):
        return len(self.seen) >= PRODUCER_COUNT * TOTAL_PER_PRODUCER

    def timed_out(self):
        limit = IDLE_TIMEOUT_SECONDS if self.received else START_TIMEOUT_SECONDS
        return time.monotonic() - self._last_activity > limit

    def write(self, technology):
        with open(os.path.join(RESULT_DIR, "consumer.json"), "w") as f:
            json.dump({
                "technology": technology,
                "run_id": RUN_ID,
                "received": self.received,
                "duplicates": self.duplicates,
                "arrivals": self.arrivals,
            }, f)
        print(f"[Consumer] Fertig: {len(self.seen)}/{PRODUCER_COUNT * TOTAL_PER_PRODUCER} "
              f"eindeutig, {sum(self.duplicates.values())} Duplikate.", flush=True)
