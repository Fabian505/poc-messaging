#!/usr/bin/env bash
#
# setup.sh - richtet die komplette PoC-Umgebung auf einem neuen Rechner ein.
# Einmalig vor dem ersten Messlauf ausfuehren, oder erneut nach Loeschen der
# Datenverzeichnisse. Idempotent: kann gefahrlos mehrfach laufen.
#
# Schritte:
#   1. Voraussetzungen pruefen (Podman, Python, IBM-MQ-Client fuer pymqi)
#   2. Datenverzeichnisse und UID-Mapping (rootless Podman)
#   3. Container starten und warten, bis alle drei Broker bereit sind
#   4. Broker-Konfiguration, die fuer ALLE Laeufe gelten muss
#   5. Python-venv anlegen und Abhaengigkeiten installieren
#
# Abgrenzung: Hier steht nur die dauerhafte Umgebungskonfiguration. Alles,
# was pro Messlauf entsteht (frische Kafka-Topics fuer UC2-UC4, dauerhafte
# Subscriptions fuer UC3, Leeren von Queues), legen die Orchestratoren
# run_uc*_measurement.py selbst an und raeumen es wieder ab.
#
# Umgebungsvariablen (optional):
#   PYTHON=/usr/bin/python3   Interpreter fuer die venv (Standard: python3)
#   SKIP_VENV=1               Schritt 5 ueberspringen

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-python3}"
VENV_DIR="$SCRIPT_DIR/venv"
MQ_CLIENT_DIR="/opt/mqm"
READY_TIMEOUT=180

step() { echo; echo "=== $* ==="; }
fail() { echo "FEHLER: $*" >&2; exit 1; }

wait_for() {
    # wait_for <Beschreibung> <Befehl...>: wiederholt den Befehl bis Erfolg
    local desc="$1"; shift
    local waited=0
    until "$@" >/dev/null 2>&1; do
        (( waited >= READY_TIMEOUT )) && fail "$desc nach ${READY_TIMEOUT}s nicht bereit."
        sleep 3; waited=$((waited + 3))
    done
    echo "  $desc bereit (${waited}s)."
}

mqsc() {
    podman exec ibmmq bash -c "echo \"$1\" | runmqsc QM1" \
        | grep -E "AMQ[0-9]+[IEW]:" || true
}

# ---------------------------------------------------------------- 1
step "1/5 Voraussetzungen"

command -v podman >/dev/null || fail "podman nicht gefunden."
command -v "$PYTHON" >/dev/null || fail "Python-Interpreter '$PYTHON' nicht gefunden."

if [[ -n "${CONDA_PREFIX:-}" ]]; then
    echo "  WARNUNG: Conda-Umgebung aktiv ($CONDA_PREFIX). Die venv wuerde auf dem"
    echo "  Conda-Python aufbauen. Fuer reproduzierbare Messungen vorher"
    echo "  'conda deactivate' oder PYTHON=/usr/bin/python3 setzen."
fi

# pymqi-Build ignoriert MQ_INSTALLATION_PATH und sucht Header/Libs nur unter
# /opt/mqm (siehe Lessons Learned). Hier nur pruefen, nicht installieren:
# der IBM MQ Redistributable Client muss manuell dorthin kopiert werden
# (keine globale Installation, siehe mqm.conf-Vorfall).
if [[ -z "${SKIP_VENV:-}" ]]; then
    [[ -f "$MQ_CLIENT_DIR/inc/cmqc.h" ]] \
        || fail "IBM-MQ-Client-Header fehlen ($MQ_CLIENT_DIR/inc/cmqc.h). Redistributable Client nach $MQ_CLIENT_DIR kopieren."
    ls "$MQ_CLIENT_DIR"/lib64/libmqic_r.so* >/dev/null 2>&1 \
        || fail "IBM-MQ-Client-Bibliothek fehlt ($MQ_CLIENT_DIR/lib64/libmqic_r.so)."
    echo "  IBM-MQ-Client unter $MQ_CLIENT_DIR gefunden."
fi
echo "  $("$PYTHON" --version), $(podman --version)"

# ---------------------------------------------------------------- 2
step "2/5 Datenverzeichnisse und UID-Mapping"

# Rootless Podman: Bind-Mounts gehoeren sonst dem Host-Benutzer, die
# Container laufen aber mit eigenen UIDs (Kafka 1000, IBM MQ 1001) und
# duerfen nicht schreiben. RabbitMQ braucht kein Mapping.
mkdir -p kafka/data rabbitmq/data ibmmq/data
podman unshare chown -R 1000:0 kafka/data
podman unshare chown -R 1001:0 ibmmq/data
echo "  UID-Mapping gesetzt (Kafka 1000:0, IBM MQ 1001:0)."

# ---------------------------------------------------------------- 3
step "3/5 Container starten"

podman compose up -d

wait_for "Kafka" podman exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list
wait_for "RabbitMQ" podman exec rabbitmq rabbitmq-diagnostics -q check_running
wait_for "IBM MQ" bash -c "podman exec ibmmq dspmq -m QM1 | grep -q 'STATUS(Running)'"

# ---------------------------------------------------------------- 4
step "4/5 Broker-Konfiguration"

# Kafka: festes Topic nur fuer UC1 (die Orchestratoren von UC2-UC4 legen
# ihre Topics pro Lauf selbst an). 1 Partition = total geordnetes Log.
podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic latency.test --partitions 1 --replication-factor 1
echo "  Kafka: Topic latency.test vorhanden."

# IBM MQ: Standard-MAXDEPTH 5.000 reicht fuer die grossen Laufstufen nicht.
mqsc "ALTER QLOCAL(DEV.QUEUE.2) MAXDEPTH(1200000)"

# IBM MQ: Topic-Objekt fuer UC3. Ohne dieses Objekt faellt der Topic-String
# 'dev/broadcast' auf SYSTEM.BASE.TOPIC zurueck, fuer das 'app' keine
# Rechte hat -> 2035 MQRC_NOT_AUTHORIZED (AMQ8009W). Mit DEV.-Praefix greift
# die vorhandene Berechtigung DEV.** (PUB,SUB). Siehe Lessons Learned.
mqsc "DEFINE TOPIC(DEV.BROADCAST.TOPIC) TOPICSTR('dev/broadcast') REPLACE"

echo "  IBM MQ: Kontrolle:"
podman exec ibmmq bash -c "echo \"DIS QL(DEV.QUEUE.2) MAXDEPTH
DIS TOPIC(DEV.BROADCAST.TOPIC) TOPICSTR\" | runmqsc QM1" \
    | grep -oE "MAXDEPTH\([0-9]+\)|TOPICSTR\([^)]*\)" | sed 's/^/    /'

# RabbitMQ: nichts noetig, Queues und der Fanout-Exchange werden von den
# Skripten selbst (durable) deklariert.

# ---------------------------------------------------------------- 5
if [[ -z "${SKIP_VENV:-}" ]]; then
    step "5/5 Python-venv"
    if [[ ! -x "$VENV_DIR/bin/python" ]]; then
        "$PYTHON" -m venv "$VENV_DIR"
        echo "  venv angelegt: $VENV_DIR"
    else
        echo "  venv existiert bereits: $VENV_DIR"
    fi
    "$VENV_DIR/bin/python" -m pip install --upgrade pip --quiet
    "$VENV_DIR/bin/python" -m pip install -r "$SCRIPT_DIR/requirements.txt"

    # Importtest: faengt fehlende librdkafka bzw. MQ-Client-Libs zur Laufzeit ab
    "$VENV_DIR/bin/python" -c "import confluent_kafka, pika, pymqi; \
print('  Importtest ok: confluent-kafka', confluent_kafka.version()[0], \
'| pika', pika.__version__, '| pymqi', pymqi.__version__)" \
        || fail "Importtest fehlgeschlagen (librdkafka bzw. $MQ_CLIENT_DIR/lib64 pruefen)."
else
    step "5/5 Python-venv uebersprungen (SKIP_VENV gesetzt)"
fi

echo
echo "Fertig. Aktivieren mit: source venv/bin/activate"
podman ps --format "  {{.Names}}: {{.Status}}"
