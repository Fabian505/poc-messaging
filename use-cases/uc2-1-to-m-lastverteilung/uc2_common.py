"""Gemeinsame Hilfsfunktionen fuer die UC2-Consumer (Lastverteilung).

Alle Parameter kommen aus Umgebungsvariablen, die der Orchestrator
(run_uc2_measurement.py) setzt:

    RUN_ID                eindeutige Kennung des Laufs
    INSTANCE_ID           Nummer dieser Consumer-Instanz
    START_FLAG            Datei, deren Existenz das gemeinsame Startsignal ist
    RESULT_DIR            Verzeichnis, in das jede Instanz ihr Ergebnis schreibt
    IDLE_TIMEOUT_SECONDS  Laufende, wenn so lange keine Nachricht kam (Standard 5)
    PROCESSING_MS         simulierte Verarbeitungszeit pro Nachricht (Standard 0)

Startbarriere: Alle Instanzen verbinden sich zuerst, melden "Bereit" und
warten, bis der Orchestrator START_FLAG anlegt. Erst dann beginnt der
Verbrauch. So startet keine Instanz mit Vorsprung, und bei Kafka ist der
Consumer-Group-Rebalance vor Messbeginn abgeschlossen.

Zeitmessung: Das Messfenster endet mit der LETZTEN verarbeiteten Nachricht
(last_message_at), nicht mit dem Prozessende. Das Prozessende enthaelt den
Idle-Timeout und wuerde den Durchsatz massiv verfaelschen.
"""

import json
import os
import time

RUN_ID = os.environ["RUN_ID"]
INSTANCE_ID = os.environ["INSTANCE_ID"]
START_FLAG = os.environ["START_FLAG"]
RESULT_DIR = os.environ["RESULT_DIR"]
IDLE_TIMEOUT_SECONDS = float(os.environ.get("IDLE_TIMEOUT_SECONDS", 5))
PROCESSING_SECONDS = float(os.environ.get("PROCESSING_MS", 0)) / 1000


def wait_for_start(idle_fn=None):
    """Blockiert bis START_FLAG existiert. idle_fn haelt waehrenddessen die
    Verbindung am Leben (Kafka-poll, pika-Heartbeat)."""
    while not os.path.exists(START_FLAG):
        if idle_fn is not None:
            idle_fn()
        else:
            time.sleep(0.02)


def simulate_processing(body):
    json.loads(body)
    if PROCESSING_SECONDS > 0:
        time.sleep(PROCESSING_SECONDS)


def write_result(technology, processed_count, first_message_at, last_message_at):
    path = os.path.join(RESULT_DIR, f"{INSTANCE_ID}.json")
    with open(path, "w") as f:
        json.dump({
            "technology": technology,
            "run_id": RUN_ID,
            "instance_id": INSTANCE_ID,
            "processed_count": processed_count,
            "first_message_at": first_message_at,
            "last_message_at": last_message_at,
        }, f)
    print(f"[Instanz {INSTANCE_ID}] Fertig: {processed_count} Nachrichten.", flush=True)
