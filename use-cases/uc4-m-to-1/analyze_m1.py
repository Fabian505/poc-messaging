"""Wertet results_m1.jsonl aus: prueft pro Producer, ob JEDE einzelne
Sequenznummer beim Consumer angekommen ist (Verlustnachweis, nicht nur ein
Zaehlervergleich), sowie ob die Nachrichten eines einzelnen Producers in
aufsteigender Reihenfolge eintrafen. Eine GLOBALE Reihenfolge ueber alle
Producer hinweg wird bewusst nicht bewertet, da bei mehreren gleichzeitig
sendenden Producern kein eindeutiges Sollverhalten existiert, siehe Kapitel
4.2.3.

Nutzung:
    python analyze_m1.py results_m1.jsonl <run_id> <producer_anzahl> <nachrichten_pro_producer>
"""

import json
import sys
from collections import defaultdict


def main():
    if len(sys.argv) != 5:
        print(
            "Nutzung: python analyze_m1.py <results_datei> <run_id> "
            "<producer_anzahl> <nachrichten_pro_producer>"
        )
        sys.exit(1)

    results_file, run_id = sys.argv[1], sys.argv[2]
    expected_producer_count = int(sys.argv[3])
    expected_per_producer = int(sys.argv[4])

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

    by_producer = defaultdict(list)
    for r in rows:
        by_producer[r["producer_id"]].append(r["seq"])

    print(f"--- m:1: {run_id} ---")
    print(f"Erwartete Producer: {expected_producer_count}, "
          f"tatsaechlich beobachtet: {len(by_producer)}\n")

    any_loss = False
    any_disorder = False
    for producer_id in sorted(by_producer.keys(), key=lambda x: str(x)):
        seqs = by_producer[producer_id]
        seq_set = set(seqs)
        expected_set = set(range(1, expected_per_producer + 1))
        missing = expected_set - seq_set
        duplicates = len(seqs) - len(seq_set)

        # Reihenfolge: kamen die Nachrichten DIESES Producers aufsteigend an?
        in_order = all(a <= b for a, b in zip(seqs, seqs[1:]))
        if not in_order:
            any_disorder = True

        status = "OK" if not missing else f"VERLUST ({len(missing)} fehlend)"
        if missing:
            any_loss = True

        print(
            f"  Producer {producer_id}: {len(seqs)} empfangen "
            f"({len(seq_set)} eindeutig, {duplicates} Duplikate), "
            f"Reihenfolge {'erhalten' if in_order else 'DURCHBROCHEN'}  [{status}]"
        )
        if missing and len(missing) <= 20:
            print(f"    Fehlende Sequenznummern: {sorted(missing)}")
        elif missing:
            print(f"    Fehlende Sequenznummern (Auszug): {sorted(missing)[:20]} ...")

    print()
    print("Verlust ueber alle Producer: " + ("JA" if any_loss else "NEIN, keine Nachricht verloren"))
    print("Reihenfolge pro Producer erhalten: " + ("NEIN, mindestens ein Producer betroffen" if any_disorder else "JA, bei allen Producern"))


if __name__ == "__main__":
    main()
