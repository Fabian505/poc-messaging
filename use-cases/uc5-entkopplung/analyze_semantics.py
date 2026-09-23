"""Wertet results_semantics.jsonl aus: prueft Verlust (fehlende seq) und
tatsaechlich verarbeitete Duplikate (nicht als solche erkannte) je nach
Semantik.

Nutzung:
    python analyze_semantics.py results_semantics.jsonl <run_id> <erwartete_anzahl>
"""

import json
import sys
from collections import Counter


def main():
    if len(sys.argv) != 4:
        print("Nutzung: python analyze_semantics.py <results_datei> <run_id> <erwartete_anzahl>")
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

    semantics = rows[0]["semantics"]

    processed_rows = [r for r in rows if not r.get("duplicate_skipped", False)]
    skipped_duplicates = [r for r in rows if r.get("duplicate_skipped", False)]

    seq_counts = Counter(r["seq"] for r in processed_rows)
    expected_set = set(range(1, expected_count + 1))
    received_set = set(seq_counts.keys())
    missing = expected_set - received_set
    unrecognized_duplicates = {seq: n for seq, n in seq_counts.items() if n > 1}

    print(f"--- Zustellsemantik: {semantics} (run_id: {run_id}) ---")
    print(f"Erwartet: {expected_count}, tatsaechlich verarbeitet (ohne erkannte Duplikate): {len(processed_rows)}")
    print(f"Fehlende Nachrichten: {len(missing)}")
    if missing:
        print(f"  seq-Werte: {sorted(missing)[:20]}{' ...' if len(missing) > 20 else ''}")
    print(f"Als Duplikat erkannt und uebersprungen: {len(skipped_duplicates)}")
    print(f"NICHT erkannte, tatsaechlich mehrfach verarbeitete Duplikate: {len(unrecognized_duplicates)}")
    if unrecognized_duplicates:
        print(f"  betroffene seq-Werte: {list(unrecognized_duplicates.items())[:10]}")

    print()
    print("Einordnung:")
    if semantics == "at-most-once":
        print("  Fehlende Nachrichten sind hier das ERWARTETE Verhalten dieser Semantik, kein Fehler.")
    elif semantics == "at-least-once":
        print("  Erkannte oder unerkannte Duplikate sind hier das ERWARTETE Verhalten, kein Fehler.")
        if missing:
            print("  ACHTUNG: fehlende Nachrichten waeren hier ein echter Regelverstoss.")
    elif semantics == "exactly-once":
        if missing or unrecognized_duplicates:
            print("  ACHTUNG: sowohl Verlust als auch unerkannte Duplikate waeren hier ein Regelverstoss.")
        else:
            print("  Kein Verlust, keine unerkannten Duplikate: Semantik eingehalten.")


if __name__ == "__main__":
    main()
