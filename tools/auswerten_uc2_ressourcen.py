#!/usr/bin/env python3
"""auswerten_uc2_ressourcen.py <iostat.log> <podstats.jsonl> <psutil.jsonl> [--out datei.md]

Wertet die Ressourcendaten eines UC2-Laufs (1/2/4/8 Instanzen, run_uc2_measurement.py)
aus: iostat (Platte, geraeteweise), podman stats (Container-CPU/RAM) und psutil
(Client-Prozess-CPU). Ersetzt das fehlerhafte Ad-hoc-Skript vom 29.09.2026.

KORREKTUR ggue. der ersten Fassung: iostat -x 1 gibt als ERSTEN Bericht den
Mittelwert seit Systemstart aus, nicht die erste Sekunde des Messfensters. Dieser
Block wird hier explizit verworfen (dieselbe Logik wie in auswerten_dauerlauf.py,
Funktion parse_iostat: "1. Block = Mittel seit Systemstart").

Segmentierung der vier Instanzbloecke: da run_uc2_measurement.py keine
Blockgrenzen in den Bericht schreibt, werden sie aus der Anzahl gleichzeitig
aktiver Client-Prozesse (psutil) rekonstruiert -- venv-Python-Prozesse mit
"run_uc2_measurement" im Kommando werden ausgeschlossen (das ist der
Orchestrator, kein Consumer/Producer). Die erkannte Blockfolge (aufsteigende,
sich stabilisierende Prozesszahlen) wird ausgegeben und muss gegen den Bericht
(Reihenfolge 1,2,4,8) geprueft werden -- bei Abweichungen bricht das Skript mit
einer Fehlermeldung ab, statt still falsche Blockgrenzen zu verwenden.
"""
import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime

EXPECTED_INSTANCES = [1, 2, 4, 8]


def fnum(s):
    return float(s.replace(",", "."))


def mean(v):
    return sum(v) / len(v) if v else float("nan")


def parse_iostat(path, device=None):
    """Liefert eine Liste von Sekunden-Records, OHNE den ersten Block
    (Mittelwert seit Systemstart). t ist die laufende Sekundennummer ab 0,
    gezaehlt ab dem ersten ECHTEN Block."""
    lines = open(path, errors="replace").read().splitlines()
    recs, block, cpu_idle, i = [], 0, None, 0
    dev = None
    while i < len(lines):
        l = lines[i].strip()
        if l.startswith("avg-cpu:") and i + 1 < len(lines):
            vals = lines[i + 1].replace(",", ".").split()
            try:
                cpu_idle = float(vals[-1])
            except (ValueError, IndexError):
                cpu_idle = None
            i += 2
            continue
        if l.startswith("Device"):
            hdr = l.split()
            i += 1
            while i < len(lines) and lines[i].strip():
                parts = lines[i].replace(",", ".").split()
                if device is None or parts[0] == device:
                    dev = dev or parts[0]
                    block += 1
                    if block > 1:  # 1. Block = Mittel seit Systemstart -> verwerfen
                        try:
                            d = dict(zip(hdr[1:], [fnum(x) for x in parts[1:]]))
                        except ValueError:
                            d = None
                        if d is not None:
                            d["t"] = len(recs)
                            d["cpu_idle"] = cpu_idle
                            recs.append(d)
                i += 1
            continue
        i += 1
    if not recs:
        sys.exit(f"FEHLER: keine iostat-Datenzeilen in {path} gefunden"
                  + (f" fuer Geraet {device}" if device else "") + ".")
    return recs, dev


def parse_podstats(path):
    """Gleiches kaputte Mehrzeilen-JSON-Format wie beim Dauerlauf: eine
    Podman-stats-JSON-Ausgabe ist ueber mehrere Zeilen verteilt, jede mit dem
    gemeinsamen {"ts":...,"stats": Praefix versehen -- erst zusammensetzen."""
    recs, buf, cur_ts = [], [], None
    for line in open(path, errors="replace"):
        line = line.rstrip("\n")
        m = re.match(r'^\{"ts":"([^"]+)","stats":(.*)$', line)
        if m:
            if buf:
                recs.append((cur_ts, "".join(buf)))
            cur_ts, buf = m.group(1), [m.group(2)]
        else:
            buf.append(line)
    if buf:
        recs.append((cur_ts, "".join(buf)))
    out = []
    for ts, blob in recs:
        blob = blob.rstrip()
        if blob.endswith("}"):
            blob = blob[:-1]
        try:
            data = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            data = [data]
        t = datetime.fromisoformat(ts).timestamp()
        for c in data:
            name = c.get("Name") or c.get("name")
            cpu_raw = c.get("CPU") or c.get("CPUPerc") or c.get("cpu_percent") or c.get("avg_cpu")
            mem_raw = c.get("MemUsage") or c.get("mem_usage")
            cpu = float(cpu_raw.rstrip("%")) if cpu_raw else None
            out.append((t, name, cpu, mem_raw))
    if not out:
        sys.exit(f"FEHLER: podstats-Datei {path} enthaelt keine auswertbaren Zeilen.")
    return out


def mem_mb(s):
    if not s:
        return None
    m = re.match(r"([\d.,]+)\s*([A-Za-z]+)", s.split("/")[0].strip())
    if not m:
        return None
    v = fnum(m.group(1))
    unit = m.group(2).lower()
    mult = {"b": 1e-6, "kb": 1e-3, "kib": 1 / 1024, "mb": 1, "mib": 1, "gb": 1000, "gib": 1024}
    return v * mult.get(unit, 1)


def detect_blocks(psutil_path):
    """Zeigt den Verlauf der gleichzeitig aktiven Client-Prozesse (Producer +
    Consumer-Instanzen) sekundengenau an, damit die vier Instanzbloecke von
    Auge bestaetigt werden koennen. Automatische Segmentierung ist hier
    bewusst NICHT implementiert: die Prozesszahl pendelt innerhalb eines
    Blocks zwischen Instanzzahl und Instanzzahl+1 (der Producer laeuft nur
    kurz mit), ein einfacher Schwellenwert waere unzuverlaessig und wuerde im
    Zweifel still falsche Grenzen erzeugen. Stattdessen: Uebergaenge ausgeben,
    Mensch bestaetigt, Grenzen werden explizit per --bounds uebergeben."""
    lines = [json.loads(l) for l in open(psutil_path, errors="replace")]
    clients = [d for d in lines
               if re.search(r"/venv/bin/python", d.get("cmd", ""))
               and "run_uc2_measurement" not in d.get("cmd", "")]
    if not clients:
        sys.exit(f"FEHLER: keine Client-Prozesse in {psutil_path} gefunden "
                  "(Filter auf '/venv/bin/python' liefert nichts -- Kommandozeilen-Format geaendert?).")
    t0 = min(d["ts"] for d in clients)
    by_sec = defaultdict(set)
    for d in clients:
        by_sec[int(d["ts"] - t0)].add(d["pid"])
    tmax = max(by_sec)

    transitions = []
    prev = None
    for t in range(tmax + 1):
        n = len(by_sec.get(t, set()))
        if n != prev:
            transitions.append((t, n))
            prev = n
    return transitions, t0


def print_transitions(transitions):
    print("Verlauf der gleichzeitigen Client-Prozesse (Sekunde, Anzahl) -- damit "
          "die vier Instanzbloecke von Auge bestimmen und per --bounds uebergeben:")
    for t, n in transitions:
        print(f"  t={t:5d}s  concurrent={n}")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("iostat")
    ap.add_argument("podstats")
    ap.add_argument("psutil")
    ap.add_argument("--device", default=None, help="Geraetename; Default: erstes Geraet in der Datei")
    ap.add_argument("--bounds", default=None,
                    help="Vier Zeitfenster (Sekunden ab erstem Client-Prozess) fuer die "
                         "Instanzzahlen 1,2,4,8, Format 's0:e0,s1:e1,s2:e2,s3:e3'. "
                         "Ohne Angabe zeigt das Skript den Prozessverlauf und bricht ab.")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    transitions, t0 = detect_blocks(a.psutil)
    if a.bounds is None:
        print_transitions(transitions)
        sys.exit("Keine --bounds uebergeben. Obige Uebergangsliste ansehen, die vier "
                 "Instanzbloecke (Reihenfolge 1,2,4,8) bestimmen und erneut aufrufen mit z.B.\n"
                 "  --bounds 0:271,272:302,303:456,457:613")
    try:
        parsed_bounds = []
        for part, n in zip(a.bounds.split(","), EXPECTED_INSTANCES):
            s_, e_ = part.split(":")
            parsed_bounds.append((n, int(s_), int(e_)))
    except ValueError:
        sys.exit(f"FEHLER: --bounds nicht parsebar: {a.bounds!r}. "
                 "Format: start0:end0,start1:end1,start2:end2,start3:end3 "
                 "(fuer die Instanzzahlen 1,2,4,8 in dieser Reihenfolge).")
    blocks = parsed_bounds
    io_recs, device = parse_iostat(a.iostat, a.device)
    pod = parse_podstats(a.podstats)
    kafka_pod = [(t, cpu, mem_mb(mem)) for t, name, cpu, mem in pod if name == "kafka"]
    if not kafka_pod:
        names = sorted({name for _, name, _, _ in pod})
        sys.exit(f"FEHLER: kein Container 'kafka' in {a.podstats} (gefunden: {names}).")
    pod_t0 = min(t for t, _, _ in kafka_pod)

    md = [f"# UC2-Ressourcenauswertung ({device})", "",
          f"Verwendete Blockgrenzen (Sekunden ab erstem Client-Prozess, manuell per --bounds "
          f"bestaetigt): {blocks}", "",
          "iostat-Block 1 (Mittelwert seit Systemstart) wurde verworfen.", "",
          "| Instanzen | Sek | CPU-idle Ø | w/s Ø | w/s Max | %util Ø | %util Max | "
          "Kafka-CPU Ø | Kafka-CPU Max | Kafka-RAM Ø MB | Kafka-RAM Max MB |",
          "|---|---|---|---|---|---|---|---|---|---|---|"]
    for n, s, e in blocks:
        io_sub = [r for r in io_recs if s <= r["t"] <= e]
        pod_sub = [(t, cpu, mem) for t, cpu, mem in kafka_pod if s <= t - pod_t0 <= e]
        cpu_i = [r["cpu_idle"] for r in io_sub if r["cpu_idle"] is not None]
        ws = [r["w/s"] for r in io_sub]
        ut = [r["%util"] for r in io_sub]
        pcpu = [c for _, c, _ in pod_sub if c is not None]
        pmem = [m for _, _, m in pod_sub if m is not None]
        md.append(f"| {n} | {e - s + 1} | {mean(cpu_i):.1f}% | {mean(ws):.1f} | {max(ws):.1f} | "
                  f"{mean(ut):.1f}% | {max(ut):.1f}% | {mean(pcpu):.1f}% | {max(pcpu):.1f}% | "
                  f"{mean(pmem):.0f} | {max(pmem):.0f} |")

    text = "\n".join(md) + "\n"
    print(text)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as fh:
            fh.write(text)
        print(f"-> geschrieben: {a.out}")


if __name__ == "__main__":
    main()
