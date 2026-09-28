#!/usr/bin/env python3
"""patch_persistenz.py <pfad/zu/run_uc1_measurement.py>

Fuegt einer BESTEHENDEN run_uc1_measurement.py (egal ob 14 oder 15 Berichtsspalten)
die Kopfzeile "Persistenz: <label>" hinzu. Es werden nur zwei Stellen ergaenzt,
sonst nichts veraendert. Legt <datei>.bak an, bricht ohne Aenderung ab, wenn die
erwarteten Anker nicht exakt einmal vorkommen, und ist wiederholt ausfuehrbar.
"""
import py_compile
import shutil
import sys

path = sys.argv[1] if len(sys.argv) > 1 else sys.exit(__doc__)
src = open(path, encoding="utf-8").read()

if "def persistence_label" in src:
    sys.exit("Schon gepatcht - nichts zu tun.")

FUNC = '''def persistence_label(technology):
    """Persistenz-Etikett fuer den Berichtskopf (wird von auswerten.py gelesen).
    PERSISTENZ_LABEL ueberschreibt alles (z.B. 'kafka-fsync'). Sonst aus den
    Producer-Schaltern abgeleitet (Default = persistent). ACHTUNG: das ist die
    ANGEFORDERTE Konfiguration, kein Broker-seitiger Nachweis."""
    override = os.environ.get("PERSISTENZ_LABEL")
    if override:
        return override
    if technology == "rabbitmq":
        var = "RABBITMQ_PERSISTENT"
    elif technology == "ibmmq":
        var = "MQ_PERSISTENT"
    else:
        return "kafka-standard"
    return "nein" if os.environ.get(var, "1") == "0" else "ja"


'''
HEADER = '''        # Persistenz nur bei Einzeltechnologie eindeutig (Orchestrator ruft
        # immer einzeln auf); bei "all" entfaellt die Zeile.
        if len(all_results) == 1:
            f.write(f"Persistenz: {persistence_label(next(iter(all_results)))}\\n\\n")
'''

anchor1 = "def verified_depth(technology):"
anchor2 = 'f"Soll-Last: {target_rate:.0f} Nachrichten/s\\n\\n")\n'
for a in (anchor1, anchor2):
    if src.count(a) != 1:
        sys.exit(f"ABBRUCH: Anker {a!r} kommt {src.count(a)}x vor (erwartet 1x). Datei unveraendert.")

new = src.replace(anchor1, FUNC + anchor1).replace(anchor2, anchor2 + HEADER)
shutil.copyfile(path, path + ".bak")
open(path, "w", encoding="utf-8").write(new)
try:
    py_compile.compile(path, doraise=True)
except py_compile.PyCompileError as e:
    shutil.copyfile(path + ".bak", path)
    sys.exit(f"ABBRUCH: Syntaxfehler nach Patch, Original wiederhergestellt: {e}")
print(f"OK: {path} gepatcht (Sicherung: {path}.bak). Kontrolle: diff {path}.bak {path}")
