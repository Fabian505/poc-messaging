"""Wertet contention_log.csv aus: vergleicht CPU-Auslastung waehrend des
Testlauf-Fensters (Producer-Start bis Producer-Ende) gegen die Ruhephase
unmittelbar davor.

Nutzung:
    python analyze_contention.py contention_log.csv <start_iso> <end_iso>

<start_iso> und <end_iso> sind die Zeitstempel, die der Producer nach
Ergaenzung des Start-Prints ausgibt (Format wie in der CSV, z. B.
2026-09-14T10:15:32.123456+00:00).
"""

import csv
import sys
from datetime import datetime


def parse_ts(s):
    return datetime.fromisoformat(s)


def main():
    if len(sys.argv) != 4:
        print("Nutzung: python analyze_contention.py <csv> <start_iso> <end_iso>")
        sys.exit(1)

    csv_path, start_iso, end_iso = sys.argv[1:4]
    start = parse_ts(start_iso)
    end = parse_ts(end_iso)

    with open(csv_path) as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("CSV ist leer.")
        return

    metric_columns = [k for k in rows[0].keys() if k != "timestamp"]

    baseline_rows = []
    window_rows = []
    for row in rows:
        ts = parse_ts(row["timestamp"])
        if ts < start:
            baseline_rows.append(row)
        elif start <= ts <= end:
            window_rows.append(row)

    if not window_rows:
        print(
            "Keine Messpunkte im angegebenen Zeitfenster gefunden.\n"
            "Pruefen: stimmen Zeitstempel-Format und Zeitzone (UTC) mit "
            "der CSV ueberein? Liegt das Fenster ueberhaupt innerhalb der "
            "Monitoring-Laufzeit?"
        )
        return

    # Nur die letzten Messpunkte der Ruhephase als Baseline verwenden,
    # nicht den kompletten Zeitraum vor dem Test (der ggf. Aufwaermeffekte
    # oder fruehere Aktivitaet enthaelt)
    baseline_rows = baseline_rows[-10:]

    def summarize(rows, label):
        print(f"\n--- {label} ({len(rows)} Messpunkte) ---")
        for col in metric_columns:
            values = []
            for r in rows:
                raw = r[col].replace("%", "").strip()
                try:
                    values.append(float(raw))
                except ValueError:
                    continue  # z. B. MemUsage-Spalten wie "349.7MB / 32.7GB"
            if values:
                avg = sum(values) / len(values)
                print(f"{col:25s} avg={avg:6.2f}  max={max(values):6.2f}")

    summarize(baseline_rows, "Baseline (Ruhephase vor dem Test)")
    summarize(window_rows, "Waehrend des Testlaufs")

    print(
        "\nHinweis: Nur CPU-Prozentwerte werden hier automatisch verglichen. "
        "MemUsage-Spalten (Format 'X MB / Y GB') muesst ihr bei Bedarf "
        "manuell aus der CSV ablesen."
    )


if __name__ == "__main__":
    main()
