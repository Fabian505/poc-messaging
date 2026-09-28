#!/usr/bin/env python3
"""auswerten.py <messungen-ordner> [--out-dir auswertung]

Liest alle uc*_bericht_*.md-Dateien (rekursiv) ein und erzeugt PRO UC eine
oder zwei CSV-Dateien, mit den tatsaechlichen Spaltenueberschriften aus den
Berichten (nicht erraten, sondern anhand echter Beispieldateien gebaut):

  uc1.csv                        -- ein Wert pro Einzellauf, Technologie als Spalte
  uc2_einzellaeufe.csv           -- Instanzen-Ebene je Lauf
  uc2_zusammenfassung.csv        -- Instanzen-Ebene, gemittelt ueber Wiederholungen
  uc3_einzellaeufe.csv / uc3_zusammenfassung.csv   -- analog, Subscriber-Ebene
  uc4_einzellaeufe.csv / uc4_zusammenfassung.csv   -- analog, Producer-Ebene
  uc5_zusammenfassung.csv / uc5_einzellaeufe.csv   -- Technologie ist hier
                                   bereits eine Tabellenspalte, keine Ueberschrift

Struktur je UC (aus echten Berichten vom 25./26.09.2026 abgeleitet):
  UC1: "## <technologie>" direkt gefolgt von der Werte-Tabelle (kein Unterabschnitt)
  UC2-4: "## <technologie>", darunter "### Einzellaeufe" und
         "### Zusammenfassung (nur vollstaendige Laeufe)" mit je eigener Tabelle
  UC5: "## Zusammenfassung" und "## Einzellaeufe", Technologie ist Tabellenspalte

Aendert sich das Format (z.B. eine Spalte umbenannt), bricht dieses Skript
nicht, sondern die neue Spalte taucht einfach zusaetzlich in der CSV auf --
kurz gegenpruefen, ob die Kopfzeile noch passt.
"""
import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

H2_RE = re.compile(r"^##\s+(.+)$")
H3_RE = re.compile(r"^###\s+(.+)$")
TABLE_ROW_RE = re.compile(r"^\|(.+)\|\s*$")
TABLE_SEP_RE = re.compile(r"^\|[\s:|-]+\|$")

# Kopfzeilen-Metadaten, die in fast jedem Bericht vorkommen -- best effort,
# fehlt ein Muster in einer Datei, bleibt das Feld einfach leer.
META_PATTERNS = {
    "soll_last": re.compile(r"Soll-Last:\s*([\d.]+)|Gesamtlast konstant\s*([\d.]+)\s*Nachrichten/s"),
    "nachrichten_pro_lauf": re.compile(r"Nachrichten (?:pro Lauf|gesamt pro Lauf):\s*(\d+)"),
    "wiederholungen": re.compile(r"Wiederholungen:\s*(\d+)"),
    # Ab sofort schreibt run_uc1_measurement.py "Persistenz: ja|nein" in den
    # Berichtskopf. Aeltere Berichte haben das nicht -> Zuordnungsdatei.
    "persistenz": re.compile(r"Persistenz:\s*([^\n|]+)"),
}


def load_persistenz_map(path):
    """CSV mit Spalten datei,persistenz. Dokumentierte Zuordnung fuer Berichte
    ohne Persistenz-Angabe im Kopf (Beleg: Orchestrator-Log, Berichts-
    Zeitstempel = Ende des jeweiligen Laufs)."""
    if not path:
        return {}
    with open(path, newline="", encoding="utf-8") as fh:
        return {r["datei"].strip(): r["persistenz"].strip() for r in csv.DictReader(fh)}


def resolve_persistenz(meta_value, filename, tech, persist_map, warnings):
    """Reihenfolge: Berichtskopf > Zuordnungsdatei > Standardregel.
    Standardregel greift NICHT fuer die mehrdeutige Bedingung (10.000 Nachrichten
    bei 500/s, RabbitMQ/IBM MQ), weil dort persistente und nicht-persistente
    Laeufe mit identischen Parametern existieren."""
    if meta_value:
        return meta_value.strip()
    if filename in persist_map:
        return persist_map[filename]
    if tech == "kafka":
        return "kafka-standard"  # Bestaetigung ohne fsync (Page Cache), RF=1
    if re.match(r"uc1_bericht_10000n_500r_", filename):
        warnings.append(f"{filename} ({tech}): Persistenz nicht bestimmbar -> UNBEKANNT")
        return "UNBEKANNT"
    return "ja"


def extract_meta(text):
    meta = {}
    for key, pat in META_PATTERNS.items():
        m = pat.search(text)
        if not m:
            meta[key] = ""
        else:
            groups = [g for g in m.groups() if g is not None]
            meta[key] = groups[0] if groups else ""
    return meta


def parse_tables(text):
    """Liefert Liste von (h2, h3, headers, rows) fuer jede gefundene Tabelle,
    in Dokumentreihenfolge. h3 ist '' wenn es keinen Unterabschnitt gibt."""
    lines = text.splitlines()
    h2 = h3 = ""
    out = []
    i = 0
    while i < len(lines):
        l = lines[i].strip()
        m3 = H3_RE.match(l)
        if m3:
            h3 = m3.group(1).strip()
            i += 1
            continue
        m2 = H2_RE.match(l)
        if m2:
            h2 = m2.group(1).strip()
            h3 = ""
            i += 1
            continue
        if TABLE_ROW_RE.match(l) and i + 1 < len(lines) and TABLE_SEP_RE.match(lines[i + 1].strip()):
            headers = [c.strip() for c in l.strip("|").split("|")]
            j = i + 2
            rows = []
            while j < len(lines) and TABLE_ROW_RE.match(lines[j].strip()):
                cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                if len(cells) == len(headers):
                    rows.append(cells)
                j += 1
            out.append((h2, h3, headers, rows))
            i = j
            continue
        i += 1
    return out


def uc_number(filename):
    m = re.match(r"(uc\d)", filename)
    return m.group(1) if m else "uc?"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ordner")
    ap.add_argument("--out-dir", default="auswertung")
    ap.add_argument("--persistenz-map", default=None,
                    help="CSV datei,persistenz fuer UC1-Berichte ohne Persistenz-Angabe im Kopf")
    ap.add_argument("--include-warmup", action="store_true",
                    help="UC1-Aufwaermlaeufe (500n/5000n) NICHT herausfiltern")
    args = ap.parse_args()

    base = Path(args.ordner)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)

    files = sorted(base.rglob("uc*_bericht_*.md"))
    if not files:
        print(f"Keine uc*_bericht_*.md-Dateien unter {base} gefunden.", file=sys.stderr)
        sys.exit(1)

    # sammel_key -> Liste von dicts
    buckets = defaultdict(list)
    persist_map = load_persistenz_map(args.persistenz_map)
    persist_warnings = []

    # Aufwaermlaeufe (nicht gewertet, aber gleiches Tabellenformat wie echte
    # Messungen) muessen ausgeschlossen werden, sonst zaehlen sie versehentlich
    # als Referenz-/Kapazitaetswerte mit. Feste Aufwaerm-Nachrichtenzahlen laut
    # Methodik: Kafka 5000, RabbitMQ/IBM MQ 500.
    WARMUP_MESSAGE_COUNTS = {"500", "5000"}
    excluded_warmup = []

    for f in files:
        uc = uc_number(f.name)
        text = f.read_text(encoding="utf-8")
        meta = extract_meta(text)
        if uc != "uc1":
            meta.pop("persistenz", None)  # nur fuer UC1 relevant, sonst leere Spalte
        is_uc1_warmup = uc == "uc1" and meta.get("nachrichten_pro_lauf") in WARMUP_MESSAGE_COUNTS
        # UC5-Aufwaermen laeuft mit Wiederholungen=1 ("... all all 1"), die
        # echte Messung mit 5 ("... all all 5") -- gleiches Berichtsformat,
        # muss genauso ausgeschlossen werden wie UC1s Aufwaermberichte.
        is_uc5_warmup = uc == "uc5" and meta.get("wiederholungen") == "1"
        if (is_uc1_warmup or is_uc5_warmup) and not args.include_warmup:
            excluded_warmup.append(f.name)
            continue
        tables = parse_tables(text)

        for h2, h3, headers, rows in tables:
            if uc == "uc1":
                sammel_key = "uc1"
                extra = {"technologie": h2,
                         "persistenz": resolve_persistenz(meta.get("persistenz", ""), f.name,
                                                          h2.strip().lower(), persist_map,
                                                          persist_warnings)}
            elif uc == "uc5":
                # h2 selbst ist der Abschnitt (Zusammenfassung/Einzellaeufe),
                # Technologie steckt schon als Tabellenspalte drin
                slug = h2.lower().replace(" ", "_")
                sammel_key = f"uc5_{slug}"
                extra = {}
            else:
                # uc2/uc3/uc4: h2 = technologie, h3 = Einzellaeufe/Zusammenfassung
                slug = re.sub(r"\s*\(.*\)", "", h3).strip().lower().replace(" ", "_")
                sammel_key = f"{uc}_{slug}" if slug else f"{uc}_werte"
                extra = {"technologie": h2}

            for cells in rows:
                row = {"_datei": f.name, **meta, **extra, **dict(zip(headers, cells))}
                buckets[sammel_key].append(row)

    if not buckets:
        print("Keine Tabellen gefunden -- Format unerwartet?", file=sys.stderr)
        sys.exit(1)

    for key, rows in sorted(buckets.items()):
        fieldnames = []
        for r in rows:
            for k in r:
                if k not in fieldnames:
                    fieldnames.append(k)
        out_path = out_dir / f"{key}.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames, restval="")
            w.writeheader()
            w.writerows(rows)
        print(f"{out_path}: {len(rows)} Zeilen")


    if excluded_warmup:
        print(f"\n{len(excluded_warmup)} Aufwaermbericht(e) uebersprungen (UC1: 500n/5000n, "
              f"UC5: Wiederholungen=1; --include-warmup erzwingt Einbeziehung):")
        for name in excluded_warmup:
            print(f"  {name}")

    if persist_warnings:
        print("\nWARNUNG Persistenz (Zuordnungsdatei ergaenzen oder Berichtskopf pruefen):")
        for w_ in sorted(set(persist_warnings)):
            print(f"  {w_}")


if __name__ == "__main__":
    main()