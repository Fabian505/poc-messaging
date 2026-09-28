#!/usr/bin/env python3
"""auswerten_dauerlauf.py  --reports <ordner> --resources <ordner> [--log <datei>] [--out <datei.md>]

Fasst den UC1-Dauerlauf (1.000.000 Nachrichten x 3 je Technologie) zusammen:
  1. Log-Check           (Fehler-/Warnzeilen im dauerlauf_*.log)
  2. Berichte je Wdh.    (Rate, Rueckstand, Latenz, Drift, Sendedauer, Dauer)
  3. Platte je Wdh.      (iostat: w_await, f_await, w/s, wkB/s, %util, CPU idle,
                          dazu Vergleich Anfang/Mitte/Ende der Wiederholung)
  4. Clientprozesse      (psutil: CPU und Speicher, Verlauf)
  5. Container           (podman stats: CPU, Speicher) - best effort

Schreibt eine kompakte Markdown-Datei (zum Hochladen) und gibt sie aus.
Reine Auswertung, keine Messung: kann jederzeit laufen.

Zeitzuordnung: Wiederholung i = Zeitfenster [Ende Wdh. i-1, Ende Wdh. i] laut
Berichts-Zeitstempel (Epochensekunden im Dateinamen); Wdh. 1 beginnt mit dem
Start der Monitor-Datei (aus dem Dateinamen, lokale Zeit). Toleranz: einige
Sekunden (iostat-Zeilen tragen keinen Zeitstempel, 1 Zeile = 1 s).
"""
import argparse
import glob
import json
import math
import os
import re
import sys
from datetime import datetime, timezone

TECHS = ["kafka", "rabbitmq", "ibmmq"]
REPORT_RE = re.compile(r"uc1_bericht_1000000n_(\d+)r_(\d+)\.md$")
TOTAL_MSGS = 1_000_010  # 1.000.000 + 10 Warmup, wie im Producer gezaehlt


# ------------------------------------------------------------------ Helfer
def fnum(s):
    return float(s.replace(",", "."))


def mean(v):
    return sum(v) / len(v) if v else float("nan")


def pct(v, p):
    if not v:
        return float("nan")
    v = sorted(v)
    k = (len(v) - 1) * p / 100
    f, c = math.floor(k), math.ceil(k)
    return v[f] if f == c else v[f] + (v[c] - v[f]) * (k - f)


def fmt(x, nd=2):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    return f"{x:.{nd}f}"


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def name_ts(path):
    """Startzeit aus <label>_<art>_YYYYMMDD_HHMMSS.<ext> (lokale Zeit)."""
    m = re.search(r"_(\d{8}_\d{6})\.", os.path.basename(path))
    return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S").timestamp() if m else None


def pick(resdir, tech, art, ext):
    files = sorted(glob.glob(os.path.join(resdir, f"dauer_{tech}_{art}_*.{ext}")))
    return files[-1] if files else None


# ---------------------------------------------------------------- 1. Log
def log_check(path):
    if not path or not os.path.exists(path):
        return "Kein Log angegeben/gefunden."
    pat = re.compile(r"FEHLER|Traceback|abgebrochen|fehlgeschlagen|WARNUNG|MAXDEPTH|Error", re.I)
    hits = [l.rstrip() for l in open(path, errors="replace") if pat.search(l)]
    if not hits:
        return "Keine Fehler-/Warnzeilen gefunden."
    head = "\n".join(hits[:15])
    more = f"\n... ({len(hits) - 15} weitere)" if len(hits) > 15 else ""
    return f"{len(hits)} Treffer:\n```\n{head}{more}\n```"


# ------------------------------------------------------------ 2. Berichte
def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_reports(folder):
    reps = []
    for path in sorted(glob.glob(os.path.join(folder, "**", "uc1_bericht_1000000n_*.md"), recursive=True)):
        m = REPORT_RE.search(path)
        if not m:
            continue
        soll, ts = int(m.group(1)), int(m.group(2))
        tech, headers, row = None, None, None
        for line in open(path, encoding="utf-8"):
            line = line.rstrip("\n")
            if line.startswith("## "):
                tech = line[3:].strip()
            elif line.startswith("|") and headers is None and "Lauf" in line and "Min" in line:
                headers = [c.strip() for c in line.strip("|").split("|")]
            elif headers and re.match(r"^\|\s*\d+\s*\|", line):
                row = dict(zip(headers, [c.strip() for c in line.strip().strip("|").split("|")]))
                break
        reps.append({"tech": tech, "soll": soll, "ts": ts, "row": row, "file": os.path.basename(path)})
    by = {t: sorted([r for r in reps if r["tech"] == t], key=lambda r: r["ts"]) for t in TECHS}
    for t in TECHS:
        for i, r in enumerate(by[t], 1):
            r["rep"] = i
    return by


# --------------------------------------------------------------- 3. iostat
def parse_iostat(path, device, t0):
    lines = open(path, errors="replace").read().splitlines()
    recs, block, cpu, i = [], 0, {}, 0
    while i < len(lines):
        l = lines[i].strip()
        if l.startswith("avg-cpu:") and i + 1 < len(lines):
            names = l.split()[1:]
            try:
                cpu = dict(zip(names, [fnum(x) for x in lines[i + 1].split()]))
            except ValueError:
                cpu = {}
            i += 2
            continue
        if l.startswith("Device"):
            hdr = l.split()
            i += 1
            while i < len(lines) and lines[i].strip():
                parts = lines[i].split()
                if parts[0] == device:
                    try:
                        d = dict(zip(hdr[1:], [fnum(x) for x in parts[1:]]))
                    except ValueError:
                        d = None
                    block += 1
                    if d is not None and block > 1:  # 1. Block = Mittel seit Systemstart
                        rec = {"t": t0 + block - 1, **d}
                        rec["cpu_idle"] = cpu.get("%idle")
                        recs.append(rec)
                i += 1
            continue
        i += 1
    return recs


def window(recs, a, b, key="t"):
    return [r for r in recs if a <= r[key] < b]


def thirds(a, b):
    w = (b - a) / 3
    return [(a, a + w), (a + w, a + 2 * w), (a + 2 * w, b)]


def vals(recs, k):
    return [r[k] for r in recs if r.get(k) is not None]


def parse_amq_times(path):
    """Zeitstempel (UTC) der AMQ6729W-Warnungen aus Zeilen 'Time(2026-09-28T06:13:35.017Z)'."""
    out = set()
    for l in open(path, errors="replace"):
        m = re.search(r"Time\((\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})", l)
        if m:
            out.add(datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M:%S")
                    .replace(tzinfo=timezone.utc).timestamp())
    return sorted(out)


# --------------------------------------------------------------- 4. psutil
def parse_psutil(path):
    per_pid = {}
    for line in open(path, errors="replace"):
        try:
            d = json.loads(line)
        except ValueError:
            continue
        cmd = d.get("cmd", "")
        # Nur die Python-Clients (Producer/Consumer). Der Monitor kuerzt die Kommandozeile
        # auf 60 Zeichen ("...venv/bin/python -"), der Skriptname ist nicht mehr sichtbar.
        # Ausgeschlossen: Orchestrator ("python run_uc1_..."), Bash-Wrapper, Monitor selbst.
        if "python" not in cmd or "run_uc" in cmd or "monitor" in cmd:
            continue
        per_pid.setdefault(d["pid"], {"cmd": cmd, "s": []})["s"].append((d["ts"], d["cpu_percent"], d["rss"]))
    return per_pid


# -------------------------------------------------------------- 5. podman
SIZE_UNITS = {"b": 1, "kb": 1e3, "kib": 1024, "mb": 1e6, "mib": 1024 ** 2, "gb": 1e9, "gib": 1024 ** 3}


def parse_size_mb(s):
    m = re.match(r"\s*([\d.,]+)\s*([A-Za-z]+)", str(s))
    if not m:
        return None
    return fnum(m.group(1)) * SIZE_UNITS.get(m.group(2).lower(), 1) / 1e6


def parse_podstats(path):
    snaps = {}
    pat = re.compile(r'^\{"ts":"([^"]+)","stats":(.*)\}$')
    for l in open(path, errors="replace"):
        m = pat.match(l.strip())
        if m:
            snaps.setdefault(m.group(1), []).append(m.group(2))
    out, keys = [], set()
    for ts, parts in snaps.items():
        try:
            data = json.loads("".join(parts))
            t = datetime.fromisoformat(ts).timestamp()
        except ValueError:
            continue
        if isinstance(data, dict):
            data = [data]
        for c in data or []:
            keys.update(c.keys())
            name = c.get("name") or c.get("Name") or c.get("ContainerName")
            cpu = mem = None
            for k, v in c.items():
                kl = k.lower()
                if cpu is None and "cpu" in kl and ("perc" in kl or kl in ("cpu", "avgcpu", "avg_cpu")):
                    try:
                        cpu = fnum(str(v).replace("%", ""))
                    except ValueError:
                        pass
                if mem is None and "mem" in kl and "usage" in kl:
                    mem = parse_size_mb(str(v).split("/")[0])
            if name:
                out.append({"t": t, "name": name, "cpu": cpu, "mem_mb": mem})
    return out, keys


# -------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reports", required=True)
    ap.add_argument("--resources", required=True)
    ap.add_argument("--log", default=None)
    ap.add_argument("--device", default="nvme0n1")
    ap.add_argument("--amq-times", default=None,
                    help="Datei mit den AMQ6729W-Zeitstempeln (Zeilen 'Time(...Z)') fuer den Abgleich mit iostat")
    ap.add_argument("--out", default="dauerlauf_zusammenfassung.md")
    a = ap.parse_args()

    by = parse_reports(a.reports)
    if not any(by.values()):
        sys.exit(f"Keine uc1_bericht_1000000n_*.md unter {a.reports} gefunden.")

    md = ["# Dauerlauf-Zusammenfassung (UC1, 1.000.000 Nachrichten x 3)",
          f"\nErstellt: {datetime.now().isoformat(timespec='seconds')}\n",
          "## 1. Log-Check\n", log_check(a.log), ""]

    # ---- 2. Berichte
    md.append("\n## 2. Berichte je Wiederholung\n")
    rows, missing = [], []
    windows = {}  # tech -> [(rep, a, b)]
    for t in TECHS:
        if len(by[t]) != 3:
            missing.append(f"{t}: {len(by[t])} von 3 Berichten")
        prev = None
        for r in by[t]:
            row = r["row"]
            start = prev
            if prev is None:
                iof = pick(a.resources, t, "iostat", "log")
                start = name_ts(iof) if iof else None
            windows.setdefault(t, []).append((r["rep"], start, r["ts"]))
            dur = (r["ts"] - start) if start else None
            prev = r["ts"]
            if not row:
                rows.append([t, r["rep"], r["soll"]] + ["FEHLGESCHLAGEN"] + ["-"] * 11)
                continue
            rate, behind = num(row.get("Rate (msg/s)")), num(row.get("Rueckstand"))
            rows.append([
                t, r["rep"], r["soll"], fmt(rate, 1),
                fmt(rate / r["soll"] * 100, 1) + " %" if rate else "-",
                fmt(behind / TOTAL_MSGS * 100, 1) + " %" if behind is not None else "-",
                row.get("Median (ms)"), row.get("P95 (ms)"), row.get("P99 (ms)"), row.get("Max (ms)"),
                row.get("Drift (ms)"), row.get("Sendedauer Mittel (ms)"),
                row.get("Sendedauer Median (ms)"), row.get("Sendedauer P99 (ms)"),
                fmt(dur, 0) if dur else "-"])
    md.append(md_table(["Tech", "Wdh", "Soll", "Rate", "Rate/Soll", "Verpasste Slots", "Median ms", "P95", "P99",
                        "Max", "Drift ms", "SD Mittel", "SD Median", "SD P99", "Dauer s*"], rows))
    md.append("\n*Dauer = Bericht-Zeitstempel minus Start der Vorwiederholung (bei Wdh. 1: Monitor-Start), "
              "enthaelt Start/Ende-Overhead von einigen Sekunden. Verpasste Slots = Producer-Intervalle im "
              "Rueckstand, bezogen auf 1.000.010 Nachrichten.")
    if missing:
        md.append("\n**WARNUNG unvollstaendig:** " + "; ".join(missing))

    # ---- 3. iostat
    md.append("\n## 3. Platte und CPU (iostat) je Wiederholung\n")
    thirds_rows, main_rows, notes, stall_rows, all_eps = [], [], [], [], []
    parsed = {}
    rate_of = {(r["tech"], r["rep"]): num((r["row"] or {}).get("Rate (msg/s)")) for t in TECHS for r in by[t]}
    for t in TECHS:
        f = pick(a.resources, t, "iostat", "log")
        if not f or not windows.get(t):
            notes.append(f"{t}: keine iostat-Datei gefunden (dauer_{t}_iostat_*.log)")
            continue
        recs = parse_iostat(f, a.device, name_ts(f))
        if not recs:
            notes.append(f"{t}: iostat-Datei ohne auswertbare Zeilen fuer Geraet {a.device}")
            continue
        parsed[t] = recs
    has_f = any("f_await" in r for recs in parsed.values() for r in recs)
    for t, recs in parsed.items():
        for rep, s, e in windows[t]:
            w = window(recs, s, e)
            if not w:
                continue
            wa = vals(w, "w_await")
            row = [t, rep, len(w), fmt(mean(wa)), fmt(pct(wa, 95)), fmt(max(wa, default=float("nan")))]
            if has_f:
                row.append(fmt(mean(vals(w, "f_await"))))
            ws, wkb = mean(vals(w, "w/s")), mean(vals(w, "wkB/s"))
            rt = rate_of.get((t, rep))
            row += [fmt(ws, 1), fmt(wkb, 1),
                    fmt(ws / rt, 2) if rt else "-", fmt(wkb / rt, 1) if rt else "-",
                    fmt(mean(vals(w, "%util")), 1), fmt(mean(vals(w, "cpu_idle")), 1)]
            main_rows.append(row)
            # Schreibpausen: Sekunden, in denen das Geraet (fast) nichts geschrieben hat
            thr = 0.10 * ws
            flagged = [r["t"] for r in w if r.get("w/s") is not None and r["w/s"] < thr]
            episodes = []
            for ts_ in sorted(flagged):
                if episodes and ts_ - episodes[-1][-1] <= 1.5:
                    episodes[-1].append(ts_)
                else:
                    episodes.append([ts_])
            by_t = {r["t"]: r for r in w}
            for ep in episodes:
                wa_ep = vals([by_t[x] for x in ep if x in by_t], "w_await")
                ws_ep = vals([by_t[x] for x in ep if x in by_t], "w/s")
                all_eps.append([t, rep, datetime.fromtimestamp(ep[0]).strftime("%Y-%m-%d %H:%M:%S"),
                                int(ep[0]), len(ep),
                                round(mean(wa_ep), 2) if wa_ep else "", round(mean(ws_ep), 1) if ws_ep else ""])
            first = ", ".join(datetime.fromtimestamp(ep[0]).strftime("%H:%M:%S") + f" ({len(ep)} s)"
                              for ep in episodes[:4])
            stall_rows.append([t, rep, len(flagged), len(episodes),
                               max((len(ep) for ep in episodes), default=0), first or "-"])
            tr = [t, rep]
            for (x, y) in thirds(s, e):
                ww = window(recs, x, y)
                cell = fmt(mean(vals(ww, "w_await")))
                if has_f:
                    cell += " / " + fmt(mean(vals(ww, "f_await")))
                tr.append(cell)
            for (x, y) in thirds(s, e):
                tr.append(fmt(mean(vals(window(recs, x, y), "w/s")), 1))
            thirds_rows.append(tr)
    if main_rows:
        hdr = ["Tech", "Wdh", "Sekunden", "w_await Mittel", "P95", "Max"]
        if has_f:
            hdr.append("f_await Mittel")
        hdr += ["w/s", "wkB/s", "Schreib-IOs/Nachr.", "kB/Nachr.", "%util", "CPU idle %"]
        md.append(md_table(hdr, main_rows))
        md.append("\nSchreib-IOs und kB je Nachricht = w/s bzw. wkB/s des ganzen Geraets geteilt durch die "
                  "Rate der Wiederholung (enthaelt Hintergrundlast des Systems, siehe Kafka als Grundrauschen).")
        md.append("\n**Schreibpausen** (Sekunden mit w/s unter 10 % des Mittels der Wiederholung; "
                  "sichtbar sind nur Stalls, in denen das Geraet still war - ein reiner Consumer-Stall "
                  "waehrend der Producer weiterschreibt faellt hier nicht auf; bei Kafka nicht aussagekraeftig, weil es ohne fsync gebuendelt im Hintergrund schreibt):\n")
        md.append(md_table(["Tech", "Wdh", "Sekunden", "Episoden", "laengste (s)", "erste Zeitpunkte"], stall_rows))
        if a.amq_times and "ibmmq" in parsed:
            amq = parse_amq_times(a.amq_times)
            eps_i = [(int(r[3]), int(r[3]) + r[4] - 1, r[1]) for r in all_eps if r[0] == "ibmmq"]
            x_rows, unmatched = [], []
            for rep, s0, e0 in windows["ibmmq"]:
                warn = [x for x in amq if s0 <= x < e0]
                eps = [(st, en) for st, en, rp in eps_i if rp == rep]
                w_hit = [x for x in warn if any(st - 10 <= x <= en + 10 for st, en in eps)]
                unmatched += [x for x in warn if x not in w_hit]
                long_full = [(r[3], r[3] + r[4] - 1, r[5]) for r in all_eps
                             if r[0] == "ibmmq" and r[1] == rep and r[4] >= 3]
                hit_wa, miss_wa = [], []
                for st, en, wa in long_full:
                    (hit_wa if any(st - 10 <= x <= en + 10 for x in warn) else miss_wa).append(wa)
                x_rows.append([rep, len(warn), len(w_hit), len(long_full), len(hit_wa),
                               fmt(pct([v for v in hit_wa if v != ""], 50)),
                               fmt(pct([v for v in miss_wa if v != ""], 50))])
            md.append("\n**Abgleich IBM MQ Fehlerlog (AMQ6729W = Log-Schreibvorgang > 1 s) mit iostat** "
                      "(Toleranz +-10 s):\n")
            md.append(md_table(["Wdh", "AMQ-Warnungen", "davon mit Schreibpause", "Pausen >= 3 s",
                                "davon mit Warnung", "w_await ms (Pausen mit Warnung)",
                                "w_await ms (Pausen ohne Warnung)"], x_rows))
            md.append("\nw_await = Median der Sekundenmittel waehrend der Pausen. Hoch (deutlich ueber dem "
                      "Wiederholungsmittel) = die wenigen Schreibvorgaenge waren langsam (Geraet); niedrig = "
                      "es wurde kaum etwas geschrieben (Broker/Clients haben nichts abgeschickt).")
            if unmatched:
                md.append("\nWarnungen ohne Schreibpause (lokale Zeit): " + ", ".join(
                    datetime.fromtimestamp(x).strftime("%H:%M:%S") for x in unmatched[:12]))
        md.append("\n**Verlauf innerhalb der Wiederholung** (Anfang / Mitte / Ende; w_await"
                  + (" / f_await" if has_f else "") + " in ms, dann Schreiboperationen/s):\n")
        md.append(md_table(["Tech", "Wdh", "w_await A", "w_await M", "w_await E", "w/s A", "w/s M", "w/s E"],
                           thirds_rows))
    md += ["\n" + n for n in notes]

    md.append("\n## 4. Clientprozesse (psutil)\n")
    prow = []
    for t in TECHS:
        f = pick(a.resources, t, "psutil", "jsonl")
        if not f or not windows.get(t):
            md.append(f"{t}: keine psutil-Datei gefunden.\n")
            continue
        per_pid = parse_psutil(f)
        for rep, s, e in windows[t]:
            cands = []
            for pid, info in per_pid.items():
                ss = [x for x in info["s"] if s <= x[0] < e]
                if len(ss) > 30:
                    cands.append((pid, info["cmd"], ss))
            cands = sorted(cands, key=lambda c: -len(c[2]))[:2]      # die zwei langlebigsten
            cands.sort(key=lambda c: c[0])  # aelterer PID = zuerst gestartet = Consumer
            for label, (pid, cmd, ss) in zip(("Client 1 (Consumer?)", "Client 2 (Producer?)"), cands):
                n3 = len(ss) // 3
                rss = [x[2] / 1e6 for x in ss]
                cpu = [x[1] for x in ss]
                prow.append([t, rep, label, pid, fmt(mean(cpu), 1), fmt(pct(cpu, 99), 1), fmt(max(cpu), 1),
                             fmt(mean(rss[:n3]), 0), fmt(mean(rss[-n3:]), 0), fmt(max(rss), 0)])
            if len(cands) < 2:
                md.append(f"{t} Wdh {rep}: nur {len(cands)} Python-Client(s) gefunden (erwartet 2).\n")
    if prow:
        md.append(md_table(["Tech", "Wdh", "Prozess", "PID", "CPU % Mittel", "P99", "Max",
                            "RSS MB Anfang", "RSS MB Ende", "RSS MB Max"], prow))
        md.append("\nRollenzuordnung nach PID-Reihenfolge (Consumer wird vor dem Producer gestartet); "
                  "die Kommandozeile ist im Monitor auf 60 Zeichen gekuerzt und nicht unterscheidbar. "
                  "CPU % ist pro Kern (100 % = ein voller Kern).")

    md.append("\n## 5. Container (podman stats, best effort)\n")
    crow, pnotes = [], []
    for t in TECHS:
        f = pick(a.resources, t, "podstats", "jsonl")
        if not f or not windows.get(t):
            pnotes.append(f"{t}: keine podstats-Datei gefunden.")
            continue
        recs, keys = parse_podstats(f)
        recs = [r for r in recs if r["name"] == t]
        if not recs:
            pnotes.append(f"{t}: podstats leer/nicht auswertbar (Dateigroesse {os.path.getsize(f)} B; "
                          f"gefundene Schluessel: {sorted(keys)[:12]}). Pruefe: head -c 600 {f}")
            continue
        for rep, s, e in windows[t]:
            w = [r for r in recs if s <= r["t"] < e]
            if not w:
                continue
            n3 = len(w) // 3 or 1
            mem = [r["mem_mb"] for r in w if r["mem_mb"] is not None]
            crow.append([t, rep, fmt(mean([r["cpu"] for r in w if r["cpu"] is not None]), 1),
                         fmt(max([r["cpu"] for r in w if r["cpu"] is not None], default=float("nan")), 1),
                         fmt(mean(mem[:n3]), 0), fmt(mean(mem[-n3:]), 0), fmt(max(mem, default=float("nan")), 0)])
    if crow:
        md.append(md_table(["Container", "Wdh", "CPU % Mittel", "CPU % Max", "Mem MB Anfang", "Mem MB Ende",
                            "Mem MB Max"], crow))
    md += pnotes

    if all_eps:
        import csv
        stalls_path = os.path.splitext(a.out)[0] + "_schreibpausen.csv"
        with open(stalls_path, "w", newline="", encoding="utf-8") as fh:
            w_ = csv.writer(fh)
            w_.writerow(["tech", "wdh", "start", "start_epoch", "dauer_s", "w_await_ms_mittel", "w_s_mittel"])
            w_.writerows(all_eps)
        md.append(f"\nAlle Schreibpausen-Episoden: {stalls_path}")

    text = "\n".join(md) + "\n"
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(text)
    print(f"\n-> geschrieben: {a.out}")


if __name__ == "__main__":
    main()