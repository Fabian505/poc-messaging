"""Wertet results_loadbalance.jsonl aus: Gesamtdurchsatz (Nachrichten/Sekunde)
ueber alle Consumer-Instanzen einer gemeinsamen Konfiguration (run_id).
Funktioniert unveraendert fuer Kafka-, RabbitMQ- und IBM-MQ-Ergebnisse.

Nutzung:
    python analyze_loadbalance.py results_loadbalance.jsonl <run_id>
"""

import json
import sys
from datetime import datetime


def parse_ts(s):
    return datetime.fromisoformat(s)


def main():
    if len(sys.argv) != 3:
        print("Nutzung: python analyze_loadbalance.py <results_datei> <run_id>")
        sys.exit(1)

    results_file, run_id = sys.argv[1], sys.argv[2]

    with open(results_file) as f:
        rows = [json.loads(line) for line in f if line.strip()]

    rows = [r for r in rows if r["run_id"] == run_id]
    if not rows:
        print(f"Keine Eintraege fuer run_id '{run_id}' gefunden.")
        return

    technologies = {r["technology"] for r in rows}
    if len(technologies) > 1:
        print(
            f"FEHLER: run_id '{run_id}' wurde fuer mehrere Technologien "
            f"verwendet ({', '.join(sorted(technologies))}). Das mischt "
            "unvergleichbare Ergebnisse. Bitte je Technologie eine eigene, "
            "eindeutige run_id verwenden (z.B. 'kafka-run-3', 'rabbitmq-run-3')."
        )
        sys.exit(1)

    total_processed = sum(r["processed_count"] for r in rows)
    first_starts = [parse_ts(r["first_message_at"]) for r in rows if r["first_message_at"]]
    finishes = [parse_ts(r["finished_at"]) for r in rows]

    if not first_starts:
        print("Keine Instanz hat Nachrichten verarbeitet.")
        return

    window_start = min(first_starts)
    window_end = max(finishes)
    duration_seconds = (window_end - window_start).total_seconds()

    print(f"--- Lastverteilung: {run_id} ---")
    print(f"Anzahl Instanzen: {len(rows)}")
    for r in sorted(rows, key=lambda r: r["instance_id"]):
        print(f"  Instanz {r['instance_id']}: {r['processed_count']} Nachrichten")
    print(f"Gesamt verarbeitet: {total_processed}")
    print(f"Zeitfenster: {duration_seconds:.2f} s")
    if duration_seconds > 0:
        print(f"Durchsatz: {total_processed / duration_seconds:.1f} Nachrichten/s")


if __name__ == "__main__":
    main()