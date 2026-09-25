"""Gemeinsamer Kern fuer UC5 (Entkopplung bei Ausfall und Zustellsemantiken).

Parameter per Umgebungsvariable (setzt run_uc5_measurement.py):
    RUN_ID, RESULT_DIR, SEMANTICS (at-most-once|at-least-once|exactly-once)
    MESSAGE_COUNT       Nachrichten pro Lauf (Standard 200)
    SEND_INTERVAL_MS    Abstand zwischen zwei Sendungen (Standard 40 -> 25/s)
    PRE_MS, POST_MS     simulierte Arbeit vor und nach dem Seiteneffekt
                        (Standard je 10)
    CRASH_AT_SEQ        nur erste Consumer-Instanz: bei dieser seq abstuerzen
    CRASH_PHASE         pre  = waehrend der Arbeit VOR dem Seiteneffekt
                        post = NACH dem Seiteneffekt, vor der Bestaetigung

VERARBEITUNGSMODELL
Jede Nachricht durchlaeuft: Zustellung -> Arbeit (PRE_MS) -> Seiteneffekt
-> Nacharbeit (POST_MS). Der Zeitpunkt der Bestaetigung an den Broker
(Commit/Ack) bestimmt die Semantik:

    at-most-once   Bestaetigung SOFORT nach Zustellung, vor der Arbeit.
                   Absturz waehrend der Verarbeitung -> Nachricht verloren.
    at-least-once  Bestaetigung NACH der Nacharbeit.
                   Absturz vor dem Seiteneffekt -> erneute Zustellung,
                   Absturz nach dem Seiteneffekt -> Duplikat.
    exactly-once   wie at-least-once, der Seiteneffekt ist aber idempotent
                   (Idempotent Receiver): Seiteneffekt und Duplikatpruefung
                   liegen in EINER Transaktion eines dauerhaften Speichers.

Der Seiteneffekt ist ein Eintrag in einer SQLite-Datenbank (RESULT_DIR/
store.sqlite). Anders als die erste Fassung (Set im Arbeitsspeicher)
ueberlebt die Duplikaterkennung damit den Absturz des Consumers. Das Set
im Speicher war nach einem Neustart leer, die exactly-once-Pruefung haette
Duplikate nach einem Absturz nie erkannt.

Erfasst werden: jede Zustellung (deliveries), jeder ausgefuehrte
Seiteneffekt (effects) und jedes erkannte Duplikat (skipped).

GEZIELTE FEHLERINJEKTION
Der Consumer beendet sich bei CRASH_AT_SEQ selbst per SIGKILL (harter
Absturz, kein Aufraeumen, keine Bestaetigung), und zwar deterministisch in
der gewaehlten Phase. Ein Kill von aussen durch den Orchestrator traf den
Verarbeitungszeitpunkt nur zufaellig (der Consumer ist etwa die Haelfte der
Zeit untaetig), die Ergebnisse waren dadurch nicht eindeutig.
Erwartete Ergebnisse je Phase:
                    pre                          post
    at-most-once    Verlust                      kein Effekt sichtbar
    at-least-once   erneute Zustellung           Duplikat verarbeitet
    exactly-once    erneute Zustellung           Duplikat erkannt
"""

import json
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone

RUN_ID = os.environ["RUN_ID"]
RESULT_DIR = os.environ["RESULT_DIR"]
SEMANTICS = os.environ["SEMANTICS"]
MESSAGE_COUNT = int(os.environ.get("MESSAGE_COUNT", 200))
SEND_INTERVAL = float(os.environ.get("SEND_INTERVAL_MS", 40)) / 1000
PRE = float(os.environ.get("PRE_MS", 10)) / 1000
POST = float(os.environ.get("POST_MS", 10)) / 1000
DB_PATH = os.path.join(RESULT_DIR, "store.sqlite")

assert SEMANTICS in {"at-most-once", "at-least-once", "exactly-once"}, SEMANTICS
ACK_BEFORE = SEMANTICS == "at-most-once"
CRASH_AT_SEQ = int(os.environ.get("CRASH_AT_SEQ", 0))
CRASH_PHASE = os.environ.get("CRASH_PHASE", "")


def crash(seq):
    print(f"CRASH seq={seq} phase={CRASH_PHASE}", flush=True)
    os.kill(os.getpid(), signal.SIGKILL)


# ---------------------------------------------------------------- Consumer

STOP = {"flag": False}


def install_sigterm():
    """SIGTERM = geordnetes Ende nach der aktuellen Nachricht (vom
    Orchestrator, wenn alles verarbeitet ist). SIGKILL = Absturz."""
    signal.signal(signal.SIGTERM, lambda *_: STOP.__setitem__("flag", True))


class EffectStore:
    def __init__(self):
        self.db = sqlite3.connect(DB_PATH, isolation_level=None, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        pk = "PRIMARY KEY" if SEMANTICS == "exactly-once" else ""
        self.db.execute(f"CREATE TABLE IF NOT EXISTS effects (seq INTEGER {pk}, ts REAL, pid INTEGER)")
        self.db.execute("CREATE TABLE IF NOT EXISTS skipped (seq INTEGER, ts REAL, pid INTEGER)")
        self.db.execute("CREATE TABLE IF NOT EXISTS deliveries (seq INTEGER, ts REAL, pid INTEGER)")
        self.pid = os.getpid()

    def delivery(self, seq):
        self.db.execute("INSERT INTO deliveries VALUES (?, ?, ?)", (seq, time.time(), self.pid))

    def effect(self, seq):
        """Seiteneffekt atomar ausfuehren. Bei exactly-once verhindert der
        Primaerschluessel die doppelte Ausfuehrung, das Duplikat wird in
        derselben Transaktion als 'skipped' vermerkt."""
        self.db.execute("BEGIN IMMEDIATE")
        cur = self.db.execute("INSERT OR IGNORE INTO effects VALUES (?, ?, ?)",
                              (seq, time.time(), self.pid))
        if cur.rowcount == 0:
            self.db.execute("INSERT INTO skipped VALUES (?, ?, ?)", (seq, time.time(), self.pid))
        self.db.execute("COMMIT")


def process(store, body, ack_fn):
    """Verarbeitet eine zugestellte Nachricht nach dem Modell oben.
    ack_fn bestaetigt beim Broker (bei RabbitMQ auto_ack: None)."""
    seq = json.loads(body)["seq"]
    store.delivery(seq)
    print(f"D {seq}", flush=True)
    if ACK_BEFORE and ack_fn:
        ack_fn()
    time.sleep(PRE)
    if seq == CRASH_AT_SEQ and CRASH_PHASE == "pre":
        crash(seq)
    store.effect(seq)
    time.sleep(POST)
    if seq == CRASH_AT_SEQ and CRASH_PHASE == "post":
        crash(seq)
    if not ACK_BEFORE and ack_fn:
        ack_fn()


def ready():
    print(f"Bereit (pid {os.getpid()}, {SEMANTICS})", flush=True)


# ---------------------------------------------------------------- Producer

def run_producer(send_fn, reconnect_fn, max_seconds=300):
    """Getaktetes Senden. Jede Nachricht gilt erst als gesendet, wenn der
    Broker sie bestaetigt hat (send_fn kehrt ohne Exception zurueck).
    Bei Fehlern (z.B. Broker-Neustart) Wiederverbindung und erneutes Senden
    DERSELBEN Nachricht: producerseitig at-least-once, fuer alle drei
    Consumer-Semantiken gleich. Moegliche Duplikate daraus erkennt der
    exactly-once-Consumer ueber die seq."""
    confirmed = {}
    resends = 0
    t_begin = time.time()
    next_send = time.time()
    for seq in range(1, MESSAGE_COUNT + 1):
        delay = next_send - time.time()
        if delay > 0:
            time.sleep(delay)
        body = json.dumps({"seq": seq, "sent_at": datetime.now(timezone.utc).isoformat()})
        while True:
            try:
                send_fn(body)
                break
            except Exception as e:
                resends += 1
                print(f"RESEND seq={seq}: {type(e).__name__}", flush=True)
                if time.time() - t_begin > max_seconds:
                    print("Abbruch: Zeitlimit beim Wiederverbinden.", flush=True)
                    sys.exit(1)
                while True:
                    time.sleep(1.0)
                    try:
                        reconnect_fn()
                        break
                    except Exception as e2:
                        print(f"RECONNECT fehlgeschlagen: {type(e2).__name__}", flush=True)
                        if time.time() - t_begin > max_seconds:
                            sys.exit(1)
        confirmed[seq] = time.time()
        print(f"S {seq}", flush=True)
        next_send += SEND_INTERVAL
        if next_send < time.time():
            next_send = time.time()

    with open(os.path.join(RESULT_DIR, "producer.json"), "w") as f:
        json.dump({"confirmed": confirmed, "resends": resends}, f)
    print(f"Fertig: {MESSAGE_COUNT} bestaetigt, {resends} Wiederholungen.", flush=True)
