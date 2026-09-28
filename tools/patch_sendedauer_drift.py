#!/usr/bin/env python3
"""patch_sendedauer_drift.py <pfad/zu/run_uc1_measurement.py>

Stellt die 15. Berichtsspalte "Sendedauer Drift (ms)" wieder her (14 -> 15 Spalten).
Der Wert stammt aus derselben Zusammenfassung wie die uebrigen Sendedauer-Werte
(Producer-Ausgabe, Zeile "Drift:"); die Regex dafuer ist in der Datei bereits vorhanden.
Nur vier Stellen werden ergaenzt. Legt <datei>.bak an, bricht ohne Aenderung ab, wenn ein
Anker nicht exakt einmal vorkommt, ist wiederholt ausfuehrbar.
"""
import py_compile
import shutil
import sys

path = sys.argv[1] if len(sys.argv) > 1 else sys.exit(__doc__)
src = open(path, encoding="utf-8").read()

if "Sendedauer Drift (ms)" in src:
    sys.exit("Datei hat die Spalte 'Sendedauer Drift (ms)' schon - nichts zu tun.")

edits = [
    # 1) Ergebnis-Dict
    ('        "send_p99": float(send_match.group("p99")) if send_match else None,\n',
     '        "send_p99": float(send_match.group("p99")) if send_match else None,\n'
     '        "send_drift": (float(send_match.group("drift"))\n'
     '                       if send_match and send_match.group("drift") else None),\n'),
    # 2) Kopfzeile der Tabelle
    ('| Sendedauer P99 (ms) |\\n")',
     '| Sendedauer P99 (ms) | Sendedauer Drift (ms) |\\n")'),
    # 3) Trennzeile (14 -> 15 Spalten)
    ('f.write("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\\n")',
     'f.write("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\\n")'),
    # 4a) Formatierung
    ('                send_p99 = f"{r[\'send_p99\']:.2f}" if r.get("send_p99") is not None else "-"\n',
     '                send_p99 = f"{r[\'send_p99\']:.2f}" if r.get("send_p99") is not None else "-"\n'
     '                send_drift = f"{r[\'send_drift\']:.2f}" if r.get("send_drift") is not None else "-"\n'),
    # 4b) Zeile
    ('f"{send_mean} | {send_median} | {send_p99} |\\n")',
     'f"{send_mean} | {send_median} | {send_p99} | {send_drift} |\\n")'),
]
for old, _ in edits:
    if src.count(old) != 1:
        sys.exit(f"ABBRUCH: Anker kommt {src.count(old)}x vor (erwartet 1x): {old.strip()[:70]!r}. "
                 "Datei unveraendert.")
new = src
for old, rep in edits:
    new = new.replace(old, rep)

shutil.copyfile(path, path + ".bak")
open(path, "w", encoding="utf-8").write(new)
try:
    py_compile.compile(path, doraise=True)
except py_compile.PyCompileError as e:
    shutil.copyfile(path + ".bak", path)
    sys.exit(f"ABBRUCH: Syntaxfehler nach Patch, Original wiederhergestellt: {e}")
print(f"OK: {path} hat jetzt 15 Berichtsspalten (Sicherung: {path}.bak). Kontrolle: diff {path}.bak {path}")
