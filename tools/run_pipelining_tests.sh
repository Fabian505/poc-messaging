#!/bin/bash
# tools/run_pipelining_tests.sh  --  Kapitel-7-Zusatztest: gruppierte Bestaetigungen
# (Fenstergroesse WINDOW: RabbitMQ Confirms, IBM MQ put() unter einem Syncpoint,
# Kafka produce()+flush() je Fenster, letzteres mit und ohne fsync je Produce-Request).
#
# NICHT Teil der offiziellen Messreihe. Ergebnis nur als Zusatzbefund fuer Kapitel 7.
#
# Aus der Repo-Wurzel starten (venv aktiv), Skripte liegen in pipelining_test/:
#   systemd-inhibit --what=idle:sleep --why="Pipelining" ./tools/run_pipelining_tests.sh
# Einstellungen per Umgebung: WINDOWS="1 5 20 50 200"  MEASURE_COUNT=5000  REPEAT=3
# DRY=1 zeigt nur an, was laufen wuerde.
#
# Ablauf: je Technologie isoliert (run_isolated.sh), Aufwaermlauf (nicht gewertet),
# dann alle Fenstergroessen x REPEAT Wiederholungen. WINDOW=1 ist die Vergleichsbasis
# im selben Skript. Ergebnisse: messungen/pipelining_<datum>/ (Rohausgaben, zusammenfassung.csv).

set -uo pipefail

MODE="${1:-all}"
SELF="$(readlink -f "$0")"
export PT_BASE="${PT_BASE:-$(pwd)}"
BASE="$PT_BASE"
export PT_DEST="${PT_DEST:-$BASE/messungen/pipelining_$(date +%Y%m%d)}"
DEST="$PT_DEST"
TESTDIR="$BASE/pipelining_test"
WINDOWS="${WINDOWS:-1 5 20 50 200}"
COUNT="${MEASURE_COUNT:-5000}"
REPEAT="${REPEAT:-3}"
DRY="${DRY:-0}"

log() { echo "[$(date -Iseconds)] $*"; }

# ------------------------------------------------------------ IBM-MQ-Helfer
ibm_qcmd()  { podman exec ibmmq bash -c "echo '$1' | runmqsc QM1"; }
ibm_depth() { ibm_qcmd 'DIS QL(DEV.QUEUE.2) CURDEPTH' | grep -oE 'CURDEPTH\([0-9]+\)' | grep -oE '[0-9]+'; }
ibm_clear_verified() { ibm_qcmd 'CLEAR QLOCAL(DEV.QUEUE.2)' >/dev/null 2>&1; [ "$(ibm_depth)" = "0" ]; }

# ------------------------------------------------------------------ Worker
run_one() {  # tech script variant window rep count [warmup]
  local tech="$1" script="$2" variant="$3" w="$4" rep="$5" count="$6" prefix="${7:-}"
  local out="$DEST/${prefix}${tech}_${variant}_w${w}_r${rep}.txt"
  local fs=()
  [ "$variant" = fsync ] && fs=(FSYNC=1)
  [ "$variant" = nofsync ] && fs=(FSYNC=0)
  if [ "$DRY" = 1 ]; then log "DRY: $tech $variant W=$w rep=$rep N=$count -> $out"; return 0; fi
  if [ "$tech" = ibmmq ]; then
    ibm_clear_verified || { log "FEHLER: IBM-MQ-Queue nicht leer nach CLEAR"; return 1; }
  fi
  env WINDOW="$w" MEASURE_COUNT="$count" PYTHONUNBUFFERED=1 "${fs[@]}" \
      timeout 900 python "$TESTDIR/$script" > "$out" 2>&1
  local rc=$?
  local res; res=$(grep -h "Fertig" "$out" | head -1)
  if [ $rc -ne 0 ]; then log "FEHLER ($rc): $tech $variant W=$w rep=$rep (siehe $out)"; return 1; fi
  log "OK: $tech $variant W=$w rep=$rep: ${res:-<keine Ergebniszeile>}"
}

worker() {
  local tech="$1" script variants warm
  case "$tech" in
    kafka)    script=kafka_batched_test.py;      variants="fsync nofsync"; warm=5000 ;;  # JVM-Aufwaermung
    rabbitmq) script=rabbitmq_pipelined_test.py; variants="std";           warm=500  ;;
    ibmmq)    script=ibmmq_batched_test.py;      variants="std";           warm=500  ;;
    *) echo "unbekannte Technologie: $tech" >&2; exit 2 ;;
  esac
  mkdir -p "$DEST"
  [ "$DRY" = 1 ] || [ -f "$TESTDIR/$script" ] || { log "FEHLER: $TESTDIR/$script fehlt"; exit 2; }
  log "=== $tech: Aufwaermen ($warm Nachrichten, nicht gewertet) ==="
  local first="${variants%% *}"
  run_one "$tech" "$script" "$first" 50 0 "$warm" warmup_ || log "WARNUNG: Aufwaermlauf fehlgeschlagen"
  local v w r
  for v in $variants; do
    for w in $WINDOWS; do
      for r in $(seq 1 "$REPEAT"); do
        run_one "$tech" "$script" "$v" "$w" "$r" "$COUNT" || true
      done
    done
  done
  if [ "$tech" = ibmmq ] && [ "$DRY" != 1 ]; then
    ibm_clear_verified && log "IBM-MQ-Queue aufgeraeumt" || log "WARNUNG: IBM-MQ-Queue manuell leeren (CLEAR QLOCAL(DEV.QUEUE.2))"
  fi
}

summary() {
  [ "$DRY" = 1 ] && { log "DRY: Zusammenfassung entfaellt"; return 0; }
  python3 - "$DEST" <<'PYEOF'
import csv, glob, os, re, statistics, sys
dest = sys.argv[1]
rows, failed = [], []
for f in sorted(glob.glob(os.path.join(dest, "*_r*.txt"))):
    name = os.path.basename(f)
    if name.startswith("warmup_"):
        continue
    m = re.match(r"(?P<tech>[a-z]+)_(?P<var>[a-z]+)_w(?P<w>\d+)_r(?P<r>\d+)\.txt$", name)
    if not m:
        continue
    text = open(f, encoding="utf-8", errors="replace").read()
    rate = re.search(r"-> ([0-9.]+) Nachrichten/s", text)
    if rate:
        rows.append((m["tech"], m["var"], int(m["w"]), int(m["r"]), float(rate.group(1))))
    else:
        failed.append(name)
out = os.path.join(dest, "zusammenfassung.csv")
with open(out, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["technologie", "variante", "fenster", "wiederholung", "nachrichten_pro_s"])
    w.writerows(rows)
print(f"\nZusammenfassung ({len(rows)} Laeufe) -> {out}")
if failed:
    print("FEHLGESCHLAGEN / ohne Ergebniszeile:", ", ".join(failed))
groups = {}
for t, v, win, r, rate in rows:
    groups.setdefault((t, v), {}).setdefault(win, []).append(rate)
print(f"\n{'Technologie':10s} {'Variante':9s} {'Fenster':>7s} {'Median msg/s':>13s} {'min-max':>15s} {'Faktor vs. W=1':>15s}")
for (t, v), wins in sorted(groups.items()):
    base = statistics.median(wins[1]) if 1 in wins else None
    for win in sorted(wins):
        vals = wins[win]
        med = statistics.median(vals)
        fac = f"{med / base:.1f}x" if base else "-"
        print(f"{t:10s} {v:9s} {win:7d} {med:13.1f} {min(vals):7.0f}-{max(vals):<7.0f} {fac:>15s}")
PYEOF
}

# ------------------------------------------------------------------- Main
if [ "$MODE" = "_worker" ]; then worker "${2:-}"; exit $?; fi

[ -d "$TESTDIR" ] || { echo "FEHLER: $TESTDIR nicht gefunden - aus der Repo-Wurzel starten." >&2; exit 2; }
if [ "$DRY" != 1 ]; then
  pgrep -f "run_dauerlauf|run_measurement_day|run_nachmessung" >/dev/null 2>&1 \
    && { echo "FEHLER: andere Messung laeuft noch." >&2; exit 2; }
  [ -n "${VIRTUAL_ENV:-}" ] || { echo "FEHLER: venv nicht aktiv." >&2; exit 2; }
fi
mkdir -p "$DEST"
log "=== PIPELINING-TESTS START: Fenster [$WINDOWS], $COUNT Nachrichten, $REPEAT Wiederholungen -> $DEST ==="
for tech in kafka rabbitmq ibmmq; do
  if [ "$DRY" = 1 ]; then bash "$SELF" _worker "$tech"
  else "$BASE/run_isolated.sh" "$tech" bash "$SELF" _worker "$tech"; fi
done
summary
log "=== PIPELINING-TESTS ENDE ==="
