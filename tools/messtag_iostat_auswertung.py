#!/usr/bin/env python3
"""messtag_iostat_auswertung.py <ratentabelle.csv> <iostat1.log> [iostat2.log ...]

Wertet die iostat-Rohdaten der sieben UC1-Messtag-Ressourcenlaeufe
(Referenz/Kapazitaet/nicht-persistent) aus. Verwirft den ersten Block
(Mittelwert seit Systemstart), genau wie auswerten_dauerlauf.py und
auswerten_uc2_ressourcen.py. Rate wird NICHT geraten, sondern aus einer
mitgelieferten CSV (Spalten: datei,rate) gelesen, damit keine falsche
Zuordnung wie bei den fruehreren Sendedauer-Werten in der Datengrundlage
passieren kann.
"""
import argparse
import csv
import os
import sys


def parse_iostat(path):
    lines = open(path, errors="replace").read().splitlines()
    recs, block, cpu_idle, i = [], 0, None, 0
    while i < len(lines):
        l = lines[i].strip()
        if l.startswith("avg-cpu:") and i + 1 < len(lines):
            vals = lines[i + 1].replace(",", ".").split()
            try:
                cpu_idle = float(vals[-1])
            except (ValueError, IndexError):
                cpu_idle = None
            i += 2
            continue
        if l.startswith("Device"):
            hdr = l.split()
            i += 1
            while i < len(lines) and lines[i].strip():
                parts = lines[i].replace(",", ".").split()
                block += 1
                if block > 1:  # 1. Block = Mittel seit Systemstart -> verwerfen
                    try:
                        d = dict(zip(hdr[1:], [float(x) for x in parts[1:]]))
                        d["cpu_idle"] = cpu_idle
                        recs.append(d)
                    except ValueError:
                        pass
                i += 1
            continue
        i += 1
    if not recs:
        sys.exit(f"FEHLER: keine iostat-Datenzeilen in {path}.")
    return recs


def mean(v):
    return sum(v) / len(v) if v else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ratentabelle", help="CSV mit Spalten: datei,rate (rate = "
                    "Nachrichten/s, aus dem jeweiligen Bericht, NICHT geschaetzt)")
    ap.add_argument("iostat_logs", nargs="+")
    a = ap.parse_args()

    rates = {}
    with open(a.ratentabelle, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rates[row["datei"]] = float(row["rate"])

    print(f"{'Datei':45s} {'Sek':>5s} {'CPU-idle Ø':>10s} {'w/s Ø':>8s} {'w_await Ø':>10s} "
          f"{'w_await Max':>12s} {'%util Ø':>8s} {'%util Max':>10s} {'IOs/Nachr.':>10s} {'kB/Nachr.':>10s}")
    for path in a.iostat_logs:
        name = os.path.basename(path)
        if name not in rates:
            sys.exit(f"FEHLER: keine Rate fuer {name} in {a.ratentabelle} hinterlegt "
                     "-- Zuordnung nicht raten, sondern Ratentabelle ergaenzen.")
        rate = rates[name]
        rows = parse_iostat(path)
        n = len(rows)
        cpu = [r["cpu_idle"] for r in rows if r["cpu_idle"] is not None]
        ws = [r["w/s"] for r in rows]
        wkb = [r["wkB/s"] for r in rows]
        wa = [r["w_await"] for r in rows]
        ut = [r["%util"] for r in rows]
        print(f"{name:45s} {n:5d} {mean(cpu):9.1f}% {mean(ws):8.1f} {mean(wa):10.2f} "
              f"{max(wa):12.2f} {mean(ut):8.1f} {max(ut):10.1f} {mean(ws)/rate:10.2f} {mean(wkb)/rate:10.1f}")


if __name__ == "__main__":
    main()
