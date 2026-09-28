#!/bin/bash
# tools/run_nachmessung.sh <block>  --  Zusatzmessungen NACH dem UC1-Dauerlauf.
#
# Bloecke (jeder einzeln, jeder isoliert ueber run_isolated.sh):
#   kafka-fsync          Kafka mit fsync pro Nachricht (flush.messages=1), UC1 Referenz + Stufen
#   ibmmq-producer-only  IBM MQ Put-Kapazitaet OHNE Consumer (Beleg fuer ~246/s)
#   outlier-rabbitmq     UC1b-Vorgeschichte 200/250/300 mit Ressourcenmonitor (Ausreisser Lauf 1 bei 200/s)
#   outlier-ibmmq        UC1b-Vorgeschichte 100/150/175 mit Ressourcenmonitor (Ausreisser Lauf 3 bei 175/s)
#
# Aus der Repo-Wurzel starten (venv aktiv), z.B.:
#   systemd-inhibit --what=idle:sleep --why="Nachmessung" \
#     ./run_isolated.sh kafka ./tools/run_nachmessung.sh kafka-fsync
#
# DRY=1 ./tools/run_nachmessung.sh <block>   -> zeigt nur an, fuehrt nichts aus.
#
# Ergebnisse landen NICHT im Messtag-Ordner, sondern in
# messungen/nachmessung_<datum>/ (sonst zaehlt auswerten.py Bedingungen doppelt).

set -uo pipefail

BLOCK="${1:-}"
SELF="$(readlink -f "$0")"
export NACH_BASE="${NACH_BASE:-$(pwd)}"
BASE="$NACH_BASE"
export NACH_DEST="${NACH_DEST:-$BASE/messungen/nachmessung_$(date +%Y%m%d)}"
DEST="$NACH_DEST"
DRY="${DRY:-0}"
UC1="$BASE/use-cases/uc1-simple-queue"
MONITOR="$BASE/tools/monitor_resources.sh"
DEVICE="nvme0n1"
RUN_TIMEOUT=10800   # nur Haenger-Schutz

usage() {
  sed -n '2,20p' "$SELF" >&2
}

# ---------------------------------------------------------------- Helfer
log() { echo "[$(date -Iseconds)] $*" | tee -a "${LOG:-/dev/null}"; }

run() {  # run <Beschreibung> <Befehl...>
  local desc="$1"; shift
  log "START: $desc"
  if [ "$DRY" = 1 ]; then log "DRY: $*"; return 0; fi
  "$@" >> "$LOG" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then log "FEHLER ($rc): $desc"; return 1; fi
  log "OK: $desc"
}

latest_report() { ls -t "$UC1"/uc1_bericht_*.md 2>/dev/null | head -1; }

report_col() {  # $1 Bericht, $2 Spaltenindex (Lauf=0) -> Wert der ersten Datenzeile
  awk -F'|' -v c="$2" '/^\| *[0-9]+ *\|/ {gsub(/ /,"",$(c+2)); print $(c+2); exit}' "$1"
}

collect() {  # neue Berichte + Ressourcendateien in den Nachmessungs-Ordner verschieben
  find "$UC1" -maxdepth 1 -name 'uc1_bericht_*.md' -newer "$MARK" -exec mv -t "$DEST" {} + 2>/dev/null
  find "$BASE/messungen/resources" -maxdepth 1 -type f -newer "$MARK" -exec mv -t "$DEST/resources" {} + 2>/dev/null
  # falls noch die alte (relative) Monitor-Version aktiv ist:
  find "$UC1/messungen/resources" -maxdepth 1 -type f -newer "$MARK" -exec mv -t "$DEST/resources" {} + 2>/dev/null
  return 0
}

auswerten() {
  local script=""
  for c in "$BASE/tools/auswerten.py" "$BASE/auswerten.py"; do [ -f "$c" ] && script="$c" && break; done
  if [ -z "$script" ]; then log "HINWEIS: auswerten.py nicht gefunden, Auswertung manuell."; return 0; fi
  [ "$DRY" = 1 ] && { log "DRY: python3 $script $DEST --out-dir $DEST/auswertung"; return 0; }
  python3 "$script" "$DEST" --out-dir "$DEST/auswertung" >> "$LOG" 2>&1 \
    && log "Auswertung: $DEST/auswertung/" || log "WARNUNG: auswerten.py fehlgeschlagen (siehe Log)"
}

# ------------------------------------------------------------ Kafka-Helfer
kcfg() { podman exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 "$@"; }
kafka_flush_set()   { kcfg --alter --entity-type topics --entity-name latency.test --add-config flush.messages=1; }
kafka_flush_reset() { kcfg --alter --entity-type topics --entity-name latency.test --delete-config flush.messages; }
kafka_flush_state() { kcfg --describe --entity-type topics --entity-name latency.test 2>&1; }
kafka_flush_verify() {  # bewusst ohne "| grep -q" (SIGPIPE unter pipefail)
  local s; s=$(kafka_flush_state); [[ "$s" == *"flush.messages=1"* ]]
}
kafka_flush_is_off() {
  local s; s=$(kafka_flush_state); [[ "$s" != *"flush.messages=1"* ]]
}

kafka_dump_config() {
  local f="$DEST/kafka_konfiguration_$(date +%H%M%S).txt" id
  {
    echo "## Topic latency.test (describe)"
    podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic latency.test
    echo; echo "## Topic-Konfiguration"
    kafka_flush_state
    id=$(podman exec kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status 2>/dev/null | awk '/^NodeId:/{print $2}')
    id=${id:-1}
    echo; echo "## Broker $id (relevante Werte)"
    kcfg --entity-type brokers --entity-name "$id" --describe --all \
      | grep -E "min.insync|default.replication|log.flush|offsets.topic.replication|log.retention.hours"
  } > "$f" 2>&1
  log "Kafka-Konfiguration gesichert: $f"
}

kafka_fsync_wirkt() {  # Kurztest: Sendedauer muss deutlich ueber 1,5 ms liegen (ohne fsync ca. 0,05 ms)
  ( cd "$UC1" && TARGET_RATE=200 PERSISTENZ_LABEL=kafka-fsync-kurztest \
      timeout "$RUN_TIMEOUT" python run_uc1_measurement.py kafka 2000 1 ) >> "$LOG" 2>&1 || return 1
  local rep sd
  rep=$(latest_report); sd=$(report_col "$rep" 11)
  log "Kurztest: Sendedauer Mittel = ${sd:-?} ms (mit fsync erwartet ca. 3 ms, ohne ca. 0,05 ms)"
  mkdir -p "$DEST/diagnose"; mv "$rep" "$DEST/diagnose/"
  awk -v s="${sd:-0}" 'BEGIN{exit !(s+0 >= 1.5)}'
}

# ------------------------------------------------------------ IBM-MQ-Helfer
ibm_qcmd()   { podman exec ibmmq bash -c "echo '$1' | runmqsc QM1"; }
ibm_num()    { grep -oE "$1\([0-9]+\)" | grep -oE '[0-9]+'; }
ibm_depth()  { ibm_qcmd 'DIS QL(DEV.QUEUE.2) CURDEPTH' | ibm_num CURDEPTH; }
ibm_clear_verified() { ibm_qcmd 'CLEAR QLOCAL(DEV.QUEUE.2)' >/dev/null 2>&1; [ "$(ibm_depth)" = "0" ]; }
ibm_maxdepth_ok() { local m; m=$(ibm_qcmd 'DIS QL(DEV.QUEUE.2) MAXDEPTH' | ibm_num MAXDEPTH); [ "${m:-0}" -ge 10100 ]; }

amq_count() {  # Anzahl AMQ6729W-Warnungen (Log-Schreibvorgang > 1 s) in allen Fehlerlog-Dateien
  local n
  n=$(podman exec ibmmq bash -c 'cat /var/mqm/qmgrs/QM1/errors/AMQERR0*.LOG 2>/dev/null | grep -c AMQ6729W' 2>/dev/null)
  echo "${n:-?}"
}

ibm_producer_only_loop() {  # laeuft unter dem Ressourcenmonitor (Selbstaufruf, siehe unten)
  cd "$UC1" || return 1
  local i
  for i in 1 2 3; do
    ibm_clear_verified || { echo "FEHLER: Queue nach CLEAR nicht leer"; return 1; }
    ibm_maxdepth_ok    || { echo "FEHLER: MAXDEPTH < 10100"; return 1; }
    echo "== Producer-only Lauf $i =="
    MEASURE_COUNT=10000 TARGET_RATE=500 PYTHONUNBUFFERED=1 \
      python ibmmq_producer_paced.py > "$DEST/ibmmq_producer_only_lauf${i}.txt" 2>&1 || return 1
    sleep 5
  done
  ibm_clear_verified
}

# interner Einstieg fuer den Selbstaufruf durch den Monitor (kein Setup, keine Guards)
if [ "$BLOCK" = "_ibm-loop" ]; then ibm_producer_only_loop; exit $?; fi

# ------------------------------------------------------------------ Setup
[ -d "$UC1" ] || { echo "FEHLER: aus der Repo-Wurzel starten." >&2; exit 2; }
case "$BLOCK" in kafka-fsync|ibmmq-producer-only|outlier-rabbitmq|outlier-ibmmq) ;; *) usage; exit 2 ;; esac
mkdir -p "$DEST/resources"
LOG="$DEST/nachmessung_$(date +%Y%m%d_%H%M%S)_${BLOCK}.log"
MARK=$(mktemp "$DEST/.mark.XXXXXX")

if [ "$DRY" != 1 ]; then
  if pgrep -f "run_dauerlauf|run_measurement_day" >/dev/null 2>&1; then
    echo "FEHLER: Dauerlauf/Messtag laeuft noch - nichts starten." >&2; exit 2
  fi
  [ -n "${VIRTUAL_ENV:-}" ] || { echo "FEHLER: venv nicht aktiv." >&2; exit 2; }
  grep -q PERSISTENZ_LABEL "$UC1/run_uc1_measurement.py" \
    || { echo "FEHLER: run_uc1_measurement.py ohne Persistenz-Kopfzeile (tools/patch_persistenz.py ausfuehren)." >&2; exit 2; }
  grep -q "Sendedauer Drift" "$UC1/run_uc1_measurement.py" \
    || { echo "FEHLER: run_uc1_measurement.py hat nur 14 Berichtsspalten (tools/patch_sendedauer_drift.py ausfuehren)." >&2; exit 2; }
fi
log "=== NACHMESSUNG $BLOCK START (Ziel: $DEST) ==="

# ----------------------------------------------------------------- Bloecke
block_kafka_fsync() {
  grep -q PERSISTENZ_LABEL "$UC1/run_uc1_measurement.py" \
    || { log "FEHLER: run_uc1_measurement.py ohne Persistenz-Kopfzeile (gepatchte Version einspielen)"; return 1; }
  grep -q 'BASH_SOURCE' "$MONITOR" \
    || { log "FEHLER: monitor_resources.sh noch ohne absoluten Pfad (gepatchte Version einspielen)"; return 1; }
  [ "$DRY" = 1 ] || kafka_dump_config
  trap 'kafka_flush_reset >/dev/null 2>&1' EXIT      # Konfiguration IMMER zuruecksetzen
  cd "$UC1" || return 1

  run "Aufwaermen Kafka (Standardkonfiguration, 5000)" \
      timeout "$RUN_TIMEOUT" env PERSISTENZ_LABEL=kafka-warmup python run_uc1_measurement.py kafka 5000 1 || return 1
  run "flush.messages=1 setzen" kafka_flush_set || return 1
  run "flush.messages=1 verifizieren (--describe)" kafka_flush_verify || return 1
  if [ "$DRY" != 1 ]; then
    kafka_fsync_wirkt || { log "ABBRUCH: Kurztest zeigt keine fsync-Wirkung - Konfiguration greift nicht."; return 1; }
  fi
  run "UC1 Kafka fsync Referenz 500/s (10000 x 5)" \
      "$MONITOR" uc1_ref_kafka_fsync "$DEVICE" -- \
      timeout "$RUN_TIMEOUT" env PERSISTENZ_LABEL=kafka-fsync python run_uc1_measurement.py kafka 10000 5 || return 1
  run "UC1b Kafka fsync Stufen 200-400 (20000 x 3)" \
      "$MONITOR" uc1b_kafka_fsync "$DEVICE" -- \
      env PERSISTENZ_LABEL=kafka-fsync bash -c \
      'for r in 200 250 300 350 400; do TARGET_RATE=$r python run_uc1_measurement.py kafka 20000 3 || exit 1; done' || return 1
  run "flush.messages zuruecksetzen" kafka_flush_reset
  [ "$DRY" = 1 ] || { kafka_flush_is_off || log "WARNUNG: flush.messages=1 steht noch - manuell zuruecksetzen!"; }
}

block_ibm_producer_only() {
  cd "$UC1" || return 1
  run "Aufwaermen IBM MQ (500)" \
      timeout "$RUN_TIMEOUT" env PERSISTENZ_LABEL=ibmmq-warmup python run_uc1_measurement.py ibmmq 500 1 || return 1
  [ "$DRY" = 1 ] || log "AMQ6729W-Warnungen vorher: $(amq_count)"
  run "Producer-only IBM MQ 3 x 10000 (ohne Consumer)" \
      "$MONITOR" uc1_produceronly_ibmmq "$DEVICE" -- "$SELF" _ibm-loop || return 1
  [ "$DRY" = 1 ] || log "AMQ6729W-Warnungen nachher: $(amq_count)"
  if [ "$DRY" != 1 ]; then
    log "Ergebnis (Rate und Sendedauer je Lauf):"
    grep -H -E "Tatsaechliche Rate|Mittel:|Median:|P99:" "$DEST"/ibmmq_producer_only_lauf*.txt | tee -a "$LOG"
  fi
}

block_outlier() {  # $1 tech, $2 Stufen
  local tech="$1" stages="$2"
  cd "$UC1" || return 1
  run "Aufwaermen $tech (500)" \
      timeout "$RUN_TIMEOUT" python run_uc1_measurement.py "$tech" 500 1 || return 1
  [ "$tech" = ibmmq ] && [ "$DRY" != 1 ] && log "AMQ6729W-Warnungen vorher: $(amq_count)"
  run "UC1b-Vorgeschichte $tech ($stages), 20000 x 3, mit Monitor" \
      "$MONITOR" "uc1b_${tech}_replikation" "$DEVICE" -- \
      bash -c "for r in $stages; do TARGET_RATE=\$r python run_uc1_measurement.py $tech 20000 3 || exit 1; done" || return 1
  [ "$tech" = ibmmq ] && [ "$DRY" != 1 ] && log "AMQ6729W-Warnungen nachher: $(amq_count)"
  return 0
}

RC=0
case "$BLOCK" in
  kafka-fsync)         block_kafka_fsync || RC=1 ;;
  ibmmq-producer-only) block_ibm_producer_only || RC=1 ;;
  outlier-rabbitmq)    block_outlier rabbitmq "200 250 300" || RC=1 ;;
  outlier-ibmmq)       block_outlier ibmmq "100 150 175" || RC=1 ;;
esac

collect
auswerten
log "=== NACHMESSUNG $BLOCK ENDE (Exit $RC), Ergebnisse: $DEST ==="
exit $RC