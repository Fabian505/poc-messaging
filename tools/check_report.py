#!/usr/bin/env python3
"""check_report.py <bericht.md> <soll_rate> [--max-drift-pct N] [--max-behind-pct N]

Prueft automatisiert, was bisher per Augenschein geprueft wurde: Rate nahe
Soll-Last, Rueckstand unter Schwelle. Liest die Markdown-Tabelle, wie sie
run_uc1_measurement.py (und baugleich uc2-4) schreibt:

| Lauf | Min | Max | Mittel | Median | P95 | P99 | Stdabw | Drift | Rate (msg/s) | Rueckstand | Sendedauer Mittel (ms) | Sendedauer Median (ms) | Sendedauer P99 (ms) |

(14 Spalten seit der Sendedauer-Erweiterung vom 27.09.2026; Rate/Rueckstand
bleiben an Position 10/11, die drei Sendedauer-Spalten sind hinten angehaengt
und werden von diesem Skript nicht ausgewertet, nur fuer die Spaltenzahl-
Pruefung gebraucht.)

Exit 0 = ok, 1 = Schwelle verletzt (Block abbrechen), 2 = Bericht nicht
parsebar (ebenfalls abbrechen, z.B. weil ALLE Laeufe fehlgeschlagen sind
und die Tabelle leer/"Keine erfolgreichen Laeufe." ist).
"""
import re
import sys

ROW_RE = re.compile(r"^\|\s*\d+\s*\|(.+)\|\s*$")

def parse_message_count(text):
    m = re.search(r"Nachrichten pro Lauf:\s*(\d+)", text)
    return int(m.group(1)) if m else None

def parse_rows(text):
    rows = []
    for line in text.splitlines():
        m = ROW_RE.match(line.strip())
        if not m:
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 14:
            continue
        try:
            rate = float(cells[9])
            behind = int(cells[10])
        except ValueError:
            continue
        rows.append({"rate": rate, "behind": behind})
    return rows

def main():
    if len(sys.argv) < 3:
        print("Usage: check_report.py <bericht.md> <soll_rate> [--max-drift-pct N] [--max-behind-pct N]")
        sys.exit(2)

    path, soll = sys.argv[1], float(sys.argv[2])
    max_drift_pct = 10.0
    max_behind_pct = 5.0
    if "--max-drift-pct" in sys.argv:
        max_drift_pct = float(sys.argv[sys.argv.index("--max-drift-pct") + 1])
    if "--max-behind-pct" in sys.argv:
        max_behind_pct = float(sys.argv[sys.argv.index("--max-behind-pct") + 1])

    try:
        text = open(path, encoding="utf-8").read()
    except OSError as e:
        print(f"FAIL: Bericht nicht lesbar: {e}")
        sys.exit(2)

    rows = parse_rows(text)
    if not rows:
        print(f"FAIL: keine Messwert-Zeilen in {path} gefunden (alle Laeufe fehlgeschlagen?)")
        sys.exit(2)

    msg_count = parse_message_count(text)

    problems = []
    for i, r in enumerate(rows, 1):
        rate_dev_pct = abs(r["rate"] - soll) / soll * 100
        if rate_dev_pct > max_drift_pct:
            problems.append(f"Lauf {i}: Rate {r['rate']:.1f} weicht {rate_dev_pct:.1f}% vom Soll {soll} ab")
        if msg_count:
            behind_pct = r["behind"] / msg_count * 100
            if behind_pct > max_behind_pct:
                problems.append(f"Lauf {i}: Rueckstand {r['behind']} ({behind_pct:.1f}% von {msg_count})")

    if problems:
        print("FAIL: " + "; ".join(problems))
        sys.exit(1)

    print(f"OK: {len(rows)} Lauf/Laeufe geprueft, Rate und Rueckstand innerhalb der Schwellen")
    sys.exit(0)

if __name__ == "__main__":
    main()