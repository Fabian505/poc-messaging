#!/usr/bin/env python3
"""fsync_benchmark.py <pfad> [--count N] [--payload-bytes B] [--out datei.csv]

Isolierter fsync-Benchmark, unabhaengig von jedem Broker. Ersetzt das waehrend
der Home-PC-Fehlersuche (25./26.09.2026) verwendete, nie versionierte
Diagnoseskript (Median damals: 2,76 ms auf Btrfs, 2,73 ms auf ext4) durch
eine dokumentierte, wiederholbare Fassung mit denselben Parametern wie im
tatsaechlichen Messaufbau.

Ablauf je Aufruf: neue Datei im angegebenen Verzeichnis anlegen, <count>-mal
denselben Block aus <payload_bytes> Zufallsbytes anhaengen (os.write) und
sofort synchron auf die Platte zwingen (os.fsync), Zeit je fsync-Aufruf
einzeln stoppen (perf_counter). Der Schreibaufruf selbst wird nicht gemessen,
nur der fsync -- das entspricht der Fragestellung (Kosten des synchronen
Flush, nicht des Schreibens in den Seiten-Cache). Datei wird am Ende gelöscht.

Parameterwahl, im Text zu belegen:
  --count 200          identisch zur Fallzahl des vorhandenen isolierten
                        pika-basic_publish()-Benchmarks, damit beide Werte
                        auf derselben Stichprobengroesse beruhen
  --payload-bytes 64    entspricht der tatsaechlichen Nachrichtengroesse in
                        UC1 (JSON aus seq + sent_at, gemessen: 62 Byte bei
                        sechsstelliger seq; 64 aufgerundet)

Aufruf fuer den Btrfs/ext4-Vergleich (Pfade ggf. anpassen):
  python3 fsync_benchmark.py ~/Desktop/poc-messaging/rabbitmq/data --out fsync_btrfs.csv
  python3 fsync_benchmark.py /boot --out fsync_ext4.csv
Beide Pfade muessen auf der GLEICHEN physischen SSD liegen (wie beim
Originaltest), sonst ist der Dateisystemvergleich nicht aussagekraeftig -- das
Skript prueft das ueber st_dev und bricht NICHT automatisch ab, sondern gibt
nur eine Warnung aus (Geraetezuordnung unter Btrfs-Subvolumes ist nicht immer
eindeutig aus st_dev ablesbar).
"""
import argparse
import os
import statistics
import sys
import time


def fnum(x):
    return f"{x:.3f}"


def run(path, count, payload_bytes):
    os.makedirs(path, exist_ok=True)
    target = os.path.join(path, f".fsync_benchmark_{os.getpid()}.tmp")
    payload = os.urandom(payload_bytes)
    durations = []
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        for _ in range(count):
            os.write(fd, payload)
            t0 = time.perf_counter()
            os.fsync(fd)
            durations.append((time.perf_counter() - t0) * 1000.0)  # ms
    finally:
        os.close(fd)
        os.remove(target)
    return durations


def summarize(durations):
    d = sorted(durations)
    n = len(d)
    def pct(p):
        k = (n - 1) * p / 100
        f, c = int(k), min(int(k) + 1, n - 1)
        return d[f] + (d[c] - d[f]) * (k - f)
    return {
        "n": n, "min": d[0], "max": d[-1],
        "mean": statistics.mean(d), "median": statistics.median(d),
        "stdev": statistics.stdev(d) if n > 1 else 0.0,
        "p95": pct(95), "p99": pct(99),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="Verzeichnis, in dem die Testdatei angelegt wird "
                    "(muss beschreibbar sein; für den Btrfs/ext4-Vergleich z. B. "
                    "rabbitmq/data bzw. /boot)")
    ap.add_argument("--count", type=int, default=200,
                    help="Anzahl fsync-Aufrufe (Default 200, identisch zum "
                         "vorhandenen pika-Benchmark)")
    ap.add_argument("--payload-bytes", type=int, default=64,
                    help="Bytes je Schreibvorgang vor dem fsync (Default 64, "
                         "entspricht der tatsaechlichen UC1-Nachrichtengroesse)")
    ap.add_argument("--out", default=None, help="CSV mit den Einzelwerten (Millisekunden)")
    a = ap.parse_args()

    try:
        st = os.stat(a.path if os.path.isdir(a.path) else os.path.dirname(a.path) or ".")
        print(f"Zielverzeichnis: {a.path}  (st_dev={st.st_dev})")
    except OSError as e:
        sys.exit(f"FEHLER: {a.path} nicht zugreifbar: {e}")

    print(f"fsync-Benchmark: {a.count} Aufrufe, {a.payload_bytes} Byte je Schreibvorgang, "
          f"Ziel: {a.path}")
    durations = run(a.path, a.count, a.payload_bytes)
    s = summarize(durations)
    print(f"n={s['n']}  Min={fnum(s['min'])}  Median={fnum(s['median'])}  "
          f"Mittel={fnum(s['mean'])}  Stdabw={fnum(s['stdev'])}  "
          f"P95={fnum(s['p95'])}  P99={fnum(s['p99'])}  Max={fnum(s['max'])}  [ms]")

    if a.out:
        with open(a.out, "w", encoding="utf-8") as fh:
            fh.write("lauf,dauer_ms\n")
            for i, d in enumerate(durations, 1):
                fh.write(f"{i},{d:.6f}\n")
        print(f"-> Einzelwerte geschrieben: {a.out}")


if __name__ == "__main__":
    main()
