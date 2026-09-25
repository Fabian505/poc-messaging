"""Fahrplanbasierte Taktung fuer die UC3-Producer, identisch zu UC1:
feste Soll-Last von 500 Nachrichten/s fuer alle drei Technologien,
unabhaengig von der Sendedauer. Ausgabe der tatsaechlichen Rate und der
Intervalle im Rueckstand (Sendedauer > Intervall). Bei Broadcast ist der
Rueckstand ein eigener Befund: mehr Subscriber = mehr Kopien pro Publish,
das kann den synchronen Sendeaufruf verlaengern.
"""

import json
import os
import time
from datetime import datetime, timezone

WARMUP_COUNT = 10
MEASURE_COUNT = int(os.environ["MEASURE_COUNT"])
TOTAL_COUNT = WARMUP_COUNT + MEASURE_COUNT
INTERVAL_SECONDS = 0.002


def run_paced(send_fn):
    behind = 0
    t_start = time.perf_counter()
    next_send = t_start
    for seq in range(1, TOTAL_COUNT + 1):
        send_fn(json.dumps({
            "seq": seq,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }))
        next_send += INTERVAL_SECONDS
        delay = next_send - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        else:
            next_send = time.perf_counter()
            behind += 1
    elapsed = time.perf_counter() - t_start
    print(f"Tatsaechliche Rate: {TOTAL_COUNT / elapsed:.1f} Nachrichten/s "
          f"(Soll: {1 / INTERVAL_SECONDS:.0f}), Intervalle im Rueckstand: {behind}",
          flush=True)
