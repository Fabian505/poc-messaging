#!/bin/bash
# run_dauerlauf.sh [kafka] [rabbitmq] [ibmmq]   -- ohne Argument: alle drei
# UC1-Dauerlauf: 1.000.000 Nachrichten x 3 Wiederholungen (je eigener Aufruf/Bericht),
# jeweils bei der KAPAZITAETSrate (Kafka 500, RabbitMQ 300, IBM MQ 130),
# NICHT bei der Ueberlast-Referenzlast 500 (RabbitMQ-Memory-Alarm-Risiko).
# Pro Technologie: Isolation -> Aufwaermlauf -> Dauerlauf mit Ressourcenmonitor.
# Aus poc-messaging/ (Repo-Wurzel) starten:
#   systemd-inhibit --what=idle:sleep --why="Dauerlauf" ./tools/run_dauerlauf.sh
# Einzelne Technologie (z.B. auf mehrere Naechte verteilen):
#   systemd-inhibit --what=idle:sleep --why="Dauerlauf" ./tools/run_dauerlauf.sh ibmmq

set -uo pipefail
BASE=$(pwd)
LOG="$BASE/messungen/dauerlauf_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$BASE/messungen"
MONITOR="$BASE/tools/monitor_resources.sh"
CHECK="$BASE/tools/check_report.py"
DEVICE="nvme0n1"
RUN_ISOLATED="$BASE/run_isolated.sh"
COUNT=1000000
REPS=3

declare -A RATE=( [kafka]=500 [rabbitmq]=300 [ibmmq]=130 )
declare -A WARMUP=( [kafka]=5000 [rabbitmq]=500 [ibmmq]=500 )

log() { echo "[$(date -Iseconds)] $*" | tee -a "$LOG"; }

dauer_block() {
  local tech=$1
  local rate=${RATE[$tech]} warm=${WARMUP[$tech]}
  log "=== DAUERLAUF BLOCK START: $tech ($COUNT Nachrichten x $REPS bei $rate/s) ==="

  cd "$BASE/use-cases/uc1-simple-queue" || return 1
  log "START: Aufwaermen $tech ($warm Nachrichten)"
  python run_uc1_measurement.py "$tech" "$warm" 1 >> "$LOG" 2>&1 \
    || { log "FEHLER: Aufwaermen $tech -- Block abgebrochen"; return 1; }

  # Monitor aus der Repo-Wurzel starten, damit die Ressourcendaten in
  # $BASE/messungen/resources landen (nicht in einem Unterordner von uc1).
  cd "$BASE" || return 1
  log "START: Dauerlauf $tech"
  # Jede Wiederholung als EIGENER Aufruf (Wiederholungen=1): run_uc1_measurement.py
  # schreibt den Bericht erst am Ende eines Aufrufs. Mit "... $COUNT $REPS" waeren
  # nach einem Absturz in Wiederholung 3 auch Wiederholung 1+2 verloren. So hat jede
  # fertige Wiederholung sofort ihren Bericht (uc1_bericht_..._<zeitstempel>.md).
  "$MONITOR" "dauer_${tech}" "$DEVICE" -- \
    bash -c "cd '$BASE/use-cases/uc1-simple-queue' && for i in \$(seq 1 $REPS); do echo \"[$tech] Wiederholung \$i/$REPS\"; TARGET_RATE=$rate python run_uc1_measurement.py $tech $COUNT 1 || echo \"FEHLER in Wiederholung \$i\"; done" \
    >> "$LOG" 2>&1
  log "Dauerlauf $tech: alle $REPS Wiederholungen durchlaufen"

  # Nur informativ (bricht nichts ab): Rate/Rueckstand jedes Berichts gegen die Sollrate
  local reports n
  reports=$(ls -t "$BASE"/use-cases/uc1-simple-queue/uc1_bericht_${COUNT}n_${rate}r_*.md 2>/dev/null | head -n "$REPS")
  n=$(echo "$reports" | grep -c . || true)
  [ "$n" -eq "$REPS" ] || log "WARNUNG: nur $n von $REPS Berichten fuer ${tech} gefunden"
  for report in $reports; do
    python3 "$CHECK" "$report" "$rate" >> "$LOG" 2>&1 \
      && log "REPORT-CHECK OK: $report" \
      || log "REPORT-CHECK AUFFAELLIG: $report (Drift/Rate im Bericht selbst ansehen)"
  done
  log "=== DAUERLAUF BLOCK ENDE: $tech ==="
}

techs=("$@")
[ ${#techs[@]} -eq 0 ] && techs=(kafka rabbitmq ibmmq)
for t in "${techs[@]}"; do
  [ -n "${RATE[$t]:-}" ] || { echo "Unbekannte Technologie: $t (erlaubt: kafka rabbitmq ibmmq)"; exit 2; }
done

# run_isolated.sh startet fuer "bash -c ..." einen NEUEN Prozess: Variablen und
# Funktionen muessen explizit in den Befehlstext (gleiches Muster wie im Messtag-Skript).
build_cmd() {
  local tech=$1
  echo "BASE='$BASE'; LOG='$LOG'; MONITOR='$MONITOR'; CHECK='$CHECK'; DEVICE='$DEVICE'; COUNT=$COUNT; REPS=$REPS; $(declare -p RATE WARMUP); $(declare -f dauer_block log); dauer_block $tech"
}

log "DAUERLAUF START: ${techs[*]}"
for tech in "${techs[@]}"; do
  "$RUN_ISOLATED" "$tech" bash -c "$(build_cmd "$tech")"
done
log "DAUERLAUF ENDE"
