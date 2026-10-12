#!/usr/bin/env python3
"""kafka_fsync_iostat_auswertung.py <iostat.log> <rate>

Wertet die Schreiblast eines Kafka-fsync-Referenzberichts aus (5 Wiederholungen
je Datei, kurze Pausen dazwischen). Verwirft den ersten Block (Mittelwert seit
Systemstart, wie in allen anderen iostat-Auswertungen dieser Arbeit) UND
trennt aktive Sekunden von den Pausen zwischen den 5 Wiederholungen, damit die
Pausen die Schreiblast je Nachricht nicht verwaessern. "Aktiv" = w/s mindestens
50% des Sekunden-Medians der ganzen Datei.

<rate> = Median-Rate (msg/s) aus dem zugehoerigen Bericht, NICHT geschaetzt.
"""
import statistics
import sys


def parse_iostat(path):
    lines = open(path, errors="replace").read().splitlines()
    recs, block, i = [], 0, 0
    while i < len(lines):
        l = lines[i].strip()
        if l.startswith("Device"):
            hdr = l.split()
            i += 1
            while i < len(lines) and lines[i].strip():
                parts = lines[i].replace(",", ".").split()
                block += 1
                if block > 1:  # 1. Block = Mittel seit Systemstart -> verwerfen
                    try:
                        recs.append(dict(zip(hdr[1:], [float(x) for x in parts[1:]])))
                    except ValueError:
                        pass
                i += 1
            continue
        i += 1
    if not recs:
        sys.exit(f"FEHLER: keine iostat-Datenzeilen in {path}.")
    return recs


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    path, rate = sys.argv[1], float(sys.argv[2])
    recs = parse_iostat(path)
    med_ws = statistics.median(r["w/s"] for r in recs)
    active = [r for r in recs if r["w/s"] >= 0.5 * med_ws]

    print(f"{path}: {len(recs)} Sekunden gesamt, {len(active)} aktiv "
          f"(w/s >= 50% des Medians {med_ws:.0f}), {len(recs)-len(active)} Pause/Uebergang")
    ws = statistics.mean(r["w/s"] for r in active)
    wkb = statistics.mean(r["wkB/s"] for r in active)
    fs = statistics.mean(r["f/s"] for r in active)
    wa = statistics.mean(r["w_await"] for r in active)
    print(f"aktiv: w/s={ws:.0f}  wkB/s={wkb:.0f}  f/s={fs:.0f}  w_await={wa:.2f}ms")
    print(f"je Nachricht (Rate {rate:.1f}/s): {ws/rate:.2f} Schreib-IOs, "
          f"{wkb/rate:.1f} kB, {fs/rate:.3f} Flushes")


if __name__ == "__main__":
    main()
