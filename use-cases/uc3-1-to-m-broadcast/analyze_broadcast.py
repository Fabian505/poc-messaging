"""Wertet results_broadcast.jsonl aus: prueft, ob jede Consumer-Instanz einer
Konfiguration (run_id) tatsaechlich eine VOLLSTAENDIGE Kopie aller
gesendeten Nachrichten erhalten hat. Anders als bei Lastverteilung wird hier
nicht aufsummiert, sondern jede Instanz einzeln gegen die erwartete
Gesamtzahl geprueft.

Nutzung:
    python analyze_broadcast.py results_broadcast.jsonl <run_id> <erwartete_anzahl>
"""

import json
import sys
from datetime import datetime


def parse_ts(s):
    return datetime.fromisoformat(s)


def main():
    if len(sys.argv) != 4:
        print("Nutzung: python analyze_broadcast.py <results_datei> <run_id> <erwartete_anzahl>")
        sys.exit(1)

    results_file, run_id, expected_count = sys.argv[1], sys.argv[2], int(sys.argv[3])

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
            f"verwendet ({', '.join(sorted(technologies))}). Bitte je "
            "Technologie eine eigene run_id verwenden."
        )
        sys.exit(1)

    print(f"--- Event-basierte Benachrichtigung: {run_id} ---")
    print(f"Erwartete Nachrichtenanzahl pro Instanz: {expected_count}")
    print(f"Anzahl Instanzen: {len(rows)}\n")

    all_complete = True
    for r in sorted(rows, key=lambda r: r["instance_id"]):
        status = "OK" if r["processed_count"] == expected_count else "UNVOLLSTAENDIG"
        if status != "OK":
            all_complete = False
        print(f"  Instanz {r['instance_id']}: {r['processed_count']} / {expected_count}  [{status}]")

    print()
    if all_complete:
        print("Broadcast-Verhalten bestaetigt: alle Instanzen haben die vollstaendige Nachrichtenmenge erhalten.")
    else:
        print("ACHTUNG: mindestens eine Instanz hat nicht alle Nachrichten erhalten, siehe Markierung oben.")


if __name__ == "__main__":
    main()
