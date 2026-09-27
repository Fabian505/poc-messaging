#!/bin/bash
# run_dauerlauf.sh -- UC1-Dauerlaufblock (1M Nachrichten), EIGENE Nacht,
# NICHT zusammen mit run_measurement_day.sh in derselben Sitzung starten.
# Rate je Technologie = jeweilige Kapazitaet (Kafka Referenz=Kapazitaet),
# NICHT die Ueberlast-Referenzlast 500 fuer RabbitMQ/IBM MQ -- sonst droht
# RabbitMQs Memory-Watermark-Alarm bei ~660.000 Nachrichten Rueckstand.
#
# Start: systemd-inhibit --what=idle:sleep --why="Dauerlauf" ./tools/run_dauerlauf.sh

set -uo pipefail
BASE=$(pwd)
LOG="$BASE/messungen/dauerlauf_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$BASE/messungen"
RUN_ISOLATED="$BASE/run_isolated.sh"

log() { echo "[$(date -Iseconds)] $*" | tee -a "$LOG"; }

log "DAUERLAUF START"

cd "$BASE/use-cases/uc1-simple-queue"

"$RUN_ISOLATED" kafka bash -c "cd '$BASE/use-cases/uc1-simple-queue' && \
  python run_uc1_measurement.py kafka 1000000 3 >> '$LOG' 2>&1"

"$RUN_ISOLATED" rabbitmq bash -c "cd '$BASE/use-cases/uc1-simple-queue' && \
  TARGET_RATE=300 python run_uc1_measurement.py rabbitmq 1000000 3 >> '$LOG' 2>&1"

"$RUN_ISOLATED" ibmmq bash -c "cd '$BASE/use-cases/uc1-simple-queue' && \
  TARGET_RATE=130 python run_uc1_measurement.py ibmmq 1000000 3 >> '$LOG' 2>&1"

log "DAUERLAUF ENDE"
