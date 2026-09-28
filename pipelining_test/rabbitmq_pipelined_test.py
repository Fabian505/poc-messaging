"""Isolierter Test: RabbitMQ mit gruppierten Publisher Confirms (Fenster) statt
einem Confirm pro Nachricht. NICHT Teil der offiziellen Messreihe -- Zusatzbefund
fuer Kapitel 7.

WICHTIG (Korrektur zur ersten Fassung): pika.BlockingConnection kann NICHT
pipelinen. Im Confirm-Modus blockiert BlockingChannel.basic_publish je Nachricht
bis zum Confirm, und BlockingChannel hat kein wait_for_confirms() (pika 1.4.4).
Diese Fassung nutzt deshalb die asynchrone SelectConnection: ein Fenster von
WINDOW Nachrichten wird ohne Warten gesendet, erst wenn alle bestaetigt sind,
folgt das naechste Fenster. WINDOW=1 entspricht dem bisherigen Verfahren
(ein Confirm pro Nachricht) und ist die Vergleichsbasis im selben Skript.

Env: MEASURE_COUNT (Default 2000), WINDOW (Default 50)
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import pika

QUEUE_NAME = "pipeline_test"
MEASURE_COUNT = int(os.environ.get("MEASURE_COUNT", 2000))
WINDOW = int(os.environ.get("WINDOW", 50))
TIMEOUT_S = 300  # Haenger-Schutz


class WindowedPublisher:
    """Fensterlogik ohne pika-Abhaengigkeit (dadurch getrennt testbar).
    Lieferschein-Nummer (delivery tag) = laufende Nummer der Nachricht auf dem
    Kanal (1-basiert), das vergibt der Broker im Confirm-Modus so."""

    def __init__(self, total, window, publish_one, on_done):
        self.total, self.window = total, window
        self.publish_one, self.on_done = publish_one, on_done
        self.sent = 0
        self.pending = set()
        self.acked = 0
        self.nacked = 0
        self.finished = False

    def start(self):
        self._send_window()

    def _send_window(self):
        target = min(self.sent + self.window, self.total)
        while self.sent < target:
            self.sent += 1
            self.pending.add(self.sent)
            self.publish_one(self.sent)

    def on_confirm(self, tag, multiple, nack=False):
        done = {t for t in self.pending if t <= tag} if multiple else ({tag} & self.pending)
        self.pending -= done
        if nack:
            self.nacked += len(done)
        else:
            self.acked += len(done)
        if not self.pending and not self.finished:
            if self.sent < self.total:
                self._send_window()
            else:
                self.finished = True
                self.on_done()


def main():
    state = {"t0": None, "t1": None, "error": None}
    conn_holder = {}

    def fail(msg):
        state["error"] = msg
        print(f"FEHLER: {msg}", file=sys.stderr)
        try:
            conn_holder["conn"].ioloop.stop()
        except Exception:
            pass

    def publish_one(seq):
        state["ch"].basic_publish(
            exchange="", routing_key=QUEUE_NAME,
            body=json.dumps({"seq": seq, "sent_at": datetime.now(timezone.utc).isoformat()}).encode(),
            properties=pika.BasicProperties(delivery_mode=2))

    def on_done():
        state["t1"] = time.perf_counter()
        state["ch"].queue_delete(QUEUE_NAME, callback=lambda _f: conn_holder["conn"].close())

    pub = WindowedPublisher(MEASURE_COUNT, WINDOW, publish_one, on_done)

    def on_ack_nack(frame):
        m = frame.method
        pub.on_confirm(m.delivery_tag, m.multiple, nack=isinstance(m, pika.spec.Basic.Nack))

    def on_confirm_ok(_frame):
        print(f"Start: {MEASURE_COUNT} Nachrichten, Fenstergroesse {WINDOW}")
        state["t0"] = time.perf_counter()
        pub.start()

    def on_purged(_frame):
        state["ch"].confirm_delivery(ack_nack_callback=on_ack_nack, callback=on_confirm_ok)

    def on_declared(_frame):
        state["ch"].queue_purge(QUEUE_NAME, callback=on_purged)

    def on_channel(ch):
        state["ch"] = ch
        ch.queue_declare(queue=QUEUE_NAME, durable=True, callback=on_declared)

    def on_open(conn):
        conn.channel(on_open_callback=on_channel)

    def on_open_error(_conn, err):
        fail(f"Verbindung zu RabbitMQ (localhost:5672) fehlgeschlagen: {err}")

    def on_close(conn, _reason):
        conn.ioloop.stop()

    conn = pika.SelectConnection(pika.ConnectionParameters(host="localhost", port=5672),
                                 on_open_callback=on_open, on_open_error_callback=on_open_error,
                                 on_close_callback=on_close)
    conn_holder["conn"] = conn
    conn.ioloop.call_later(TIMEOUT_S, lambda: fail(f"Timeout nach {TIMEOUT_S}s "
                                                    f"(gesendet {pub.sent}, bestaetigt {pub.acked})"))
    conn.ioloop.start()

    if state["error"] or state["t1"] is None:
        sys.exit(1)
    elapsed = state["t1"] - state["t0"]
    print(f"Fertig in {elapsed:.2f}s -> {MEASURE_COUNT / elapsed:.1f} Nachrichten/s "
          f"(bestaetigt {pub.acked}, nack {pub.nacked}; Vergleich: WINDOW=1 im selben Skript)")


if __name__ == "__main__":
    main()
