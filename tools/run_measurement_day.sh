#!/bin/bash
# run_measurement_day.sh — unbeaufsichtigter Messtag.
# Voraussetzung: pre_measurement_check.sh lief bereits manuell mit 0 FAIL.
# Aus poc-messaging/ (Repo-Wurzel) heraus starten:
#   systemd-inhibit --what=idle:sleep --why="Messtag" ./tools/run_measurement_day.sh

set -uo pipefail
BASE=$(pwd)
LOG="$BASE/messungen/orchestrator_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$BASE/messungen"
RUN_TIMEOUT=10800   # 3 h: nur Haenger-Schutz, KEIN Limit fuer legitime Laeufe (IBM MQ UC2 dauert ~1 h)
MONITOR="$BASE/tools/monitor_resources.sh"
CHECK="$BASE/tools/check_report.py"
DEVICE="nvme0n1"
RUN_ISOLATED="$BASE/run_isolated.sh"

declare -A CAP_RATE=( [rabbitmq]=300 [ibmmq]=120 )     # NACH UC1b-Ergebnis anpassen!

log() { echo "[$(date -Iseconds)] $*" | tee -a "$LOG"; }
skip_block() { log "ABBRUCH BLOCK: $*  -- weiter mit naechstem Block"; }

run_or_skip() {
  local desc="$1"; shift
  log "START: $desc"
  timeout "$RUN_TIMEOUT" "$@" >> "$LOG" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then log "FEHLER ($rc): $desc"; return 1; fi
  log "OK: $desc"
  return 0
}

check_or_skip() {
  local report
  report=$(ls -t $1 2>/dev/null | head -1)
  if [ -z "$report" ]; then log "FEHLER: kein Bericht gefunden fuer Muster $1"; return 1; fi
  python3 "$CHECK" "$report" "$2" >> "$LOG" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && log "REPORT-CHECK FEHLGESCHLAGEN: $report (siehe $LOG)"
  return $rc
}

# laeuft bereits isoliert (siehe Hauptablauf) -- deshalb HIER kein run_isolated.sh mehr
run_uc1_block() {
  local tech=$1 warmup=$2
  cd "$BASE/use-cases/uc1-simple-queue" || return 1

  run_or_skip "UC1 Aufwaermen $tech" \
    python run_uc1_measurement.py "$tech" "$warmup" 1 || { skip_block "UC1 $tech (Aufwaermen)"; return; }
  if [ "$tech" = "kafka" ]; then
    # Rate-Check ergibt nur bei Kafka Sinn (JIT-Stabilisierung bei einer
    # Last, die Kafka auch erreicht). RabbitMQ/IBM MQ schaffen 500/s beim
    # Aufwaermen NICHT (bekannte Kapazitaetsgrenze, das ist der Referenz-
    # Befund selbst) -- ein Rate-Check wuerde hier IMMER fehlschlagen und
    # den Block faelschlich abbrechen. Nur pruefen, dass ueberhaupt ein
    # Bericht geschrieben wurde.
    check_or_skip "uc1_bericht_${warmup}n_*.md" 500 || { skip_block "UC1 $tech (Aufwaerm-Check)"; return; }
  else
    ls -t uc1_bericht_${warmup}n_*.md >/dev/null 2>&1 || { skip_block "UC1 $tech (kein Aufwaermbericht geschrieben)"; return; }
  fi

  run_or_skip "UC1 Referenz $tech" \
    "$MONITOR" "uc1_ref_${tech}" "$DEVICE" -- \
      python run_uc1_measurement.py "$tech" 10000 5 || { skip_block "UC1 $tech (Referenz)"; return; }
  check_or_skip "uc1_bericht_10000n_*.md" 500

  if [ "$tech" != "kafka" ]; then
    local cap=${CAP_RATE[$tech]}
    run_or_skip "UC1 Kapazitaet $tech ($cap/s)" \
      "$MONITOR" "uc1_cap_${tech}" "$DEVICE" -- \
        env TARGET_RATE="$cap" python run_uc1_measurement.py "$tech" 10000 5
    check_or_skip "uc1_bericht_10000n_${cap}r_*.md" "$cap"

    local persist_var; [ "$tech" = "rabbitmq" ] && persist_var=RABBITMQ_PERSISTENT || persist_var=MQ_PERSISTENT
    run_or_skip "UC1 nicht-persistent $tech" \
      "$MONITOR" "uc1_nonpersist_${tech}" "$DEVICE" -- \
        env "$persist_var"=0 python run_uc1_measurement.py "$tech" 10000 5
    check_or_skip "uc1_bericht_10000n_*.md" 500
  fi
}

run_tech_block() {
  local tech=$1 warmup=$2
  log "=== BLOCK START: $tech ==="

  run_uc1_block "$tech" "$warmup"

  # UC1b-Stufen je Technologie: Stufen oberhalb der Saettigung liefern nur
  # identische Werte (RabbitMQ ~320-330/s, IBM MQ ~190/s auf dem Home-PC),
  # deshalb Stufen um die jeweilige Grenze herum, damit die Grenze sichtbar wird.
  local stages
  case "$tech" in
    kafka)    stages="500 1000 1500 2000 3000" ;;
    rabbitmq) stages="200 250 300 350 400" ;;
    ibmmq)    stages="100 150 175 200 250" ;;
  esac
  cd "$BASE/use-cases/uc1-simple-queue" || return 1
  run_or_skip "UC1b Stufenreihe $tech ($stages)" bash -c "
    for r in $stages; do
      TARGET_RATE=\$r python run_uc1_measurement.py $tech 20000 3 || exit 1
    done"

  # UC2: Vorbefuellung laeuft mit der persistenten Senderate (IBM MQ ~190/s),
  # 100000 x 10 x 4 waeren fuer IBM MQ ~6 h. RabbitMQ-UC2 hat den bekannten
  # Skriptfehler (Tiefe None) -> ueberspringen statt Stunden zu verschwenden.
  cd "$BASE/use-cases/uc2-1-to-m-lastverteilung" || return 1
  case "$tech" in
    kafka)    run_or_skip "UC2 $tech" python run_uc2_measurement.py "$tech" 100000 10 1,2,4,8 ;;
    ibmmq)    run_or_skip "UC2 $tech" python run_uc2_measurement.py "$tech" 20000 5 1,2,4,8 ;;
    rabbitmq) log "UEBERSPRUNGEN: UC2 rabbitmq (bekannter Skriptfehler Tiefenabfrage, erst fixen, dann manuell nachholen)" ;;
  esac

  cd "$BASE/use-cases/uc3-1-to-m-broadcast" || return 1
  run_or_skip "UC3 $tech" python run_uc3_measurement.py "$tech" 10000 5 1,2,4,8

  cd "$BASE/use-cases/uc4-m-to-1" || return 1
  run_or_skip "UC4 $tech" python run_uc4_measurement.py "$tech" 10000 5 1,2,4,8

  log "=== BLOCK ENDE: $tech ==="
}

log "MESSTAG START"

# WICHTIG: run_isolated.sh startet fuer "bash -c ..." einen NEUEN Prozess.
# Der kennt weder $BASE/$LOG/$MONITOR/$CHECK/$DEVICE/$CAP_RATE noch die
# Funktionen automatisch -- declare -f kopiert nur die Funktions-DEFINITIONEN,
# nicht die Variablenwerte, die diese Funktionen brauchen. Deshalb muessen
# alle Variablen explizit in den uebergebenen Befehlstext mit reingeschrieben
# werden (nicht nur exportiert -- assoziative Arrays wie CAP_RATE lassen sich
# in bash gar nicht exportieren).
build_cmd() {
  local tech=$1 warmup=$2
  echo "BASE='$BASE'; LOG='$LOG'; RUN_TIMEOUT=$RUN_TIMEOUT; MONITOR='$MONITOR'; CHECK='$CHECK'; DEVICE='$DEVICE'; $(declare -p CAP_RATE); $(declare -f run_tech_block run_uc1_block run_or_skip check_or_skip skip_block log); run_tech_block $tech $warmup"
}

"$RUN_ISOLATED" kafka    bash -c "$(build_cmd kafka 5000)"
"$RUN_ISOLATED" rabbitmq bash -c "$(build_cmd rabbitmq 500)"
"$RUN_ISOLATED" ibmmq    bash -c "$(build_cmd ibmmq 500)"

cd "$BASE/use-cases/uc5-entkopplung" || exit 1
for tech in kafka rabbitmq ibmmq; do
  run_or_skip "UC5 Aufwaermen $tech" python run_uc5_measurement.py "$tech" all all 1
  run_or_skip "UC5 Messung $tech"    python run_uc5_measurement.py "$tech" all all 5
done

log "MESSTAG ENDE"