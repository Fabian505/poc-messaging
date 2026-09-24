#!/usr/bin/env bash
#
# pre_measurement_check.sh - vor JEDER Messsitzung auf dem Home-PC ausfuehren.
#
# Prueft den Zustand der Messumgebung und schreibt einen Umgebungs-Snapshot
# fuer Kapitel 6 (Messumgebung) nach messumgebung_<zeitstempel>.txt.
# Veraendert NICHTS am System: jede Abweichung wird nur gemeldet, mit dem
# Befehl zur Behebung. Grund: Governor, Energieprofil usw. sind Zustand der
# Messsitzung, nicht der Umgebung, und sollen nicht dauerhaft verstellt werden.
#
# Ergebnis:  OK / WARN (pruefen, Messung moeglich) / FAIL (nicht messen)
# Exit-Code: 0 ohne FAIL, 1 mit mindestens einem FAIL
#
# Umgebungsvariablen (optional):
#   EXPECTED_PYTHON=3.13   erwartete Python-Minor-Version der venv
#   MIN_FREE_GB=10         minimal freier Plattenplatz im Repo-Verzeichnis

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

EXPECTED_PYTHON="${EXPECTED_PYTHON:-3.13}"
MIN_FREE_GB="${MIN_FREE_GB:-10}"
VENV_PY="$SCRIPT_DIR/venv/bin/python"
SNAPSHOT="messumgebung_$(date +%Y%m%d_%H%M%S).txt"
FAILS=0; WARNS=0

ok()   { echo "  [OK]   $*"; }
warn() { echo "  [WARN] $*"; WARNS=$((WARNS + 1)); }
bad()  { echo "  [FAIL] $*"; FAILS=$((FAILS + 1)); }
hint() { echo "         -> $*"; }
section() { echo; echo "== $* =="; }

mqsc() { podman exec ibmmq bash -c "echo \"$1\" | runmqsc QM1" 2>/dev/null; }

# ---------------------------------------------------------------- Energie/CPU
section "CPU und Energie"

governors=$(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort -u | tr '\n' ' ')
if [[ -z "$governors" ]]; then
    warn "CPU-Governor nicht lesbar"
elif [[ "$governors" == "performance " ]]; then
    ok "CPU-Governor: performance (alle Kerne)"
else
    bad "CPU-Governor: $governors(erwartet: performance)"
    hint "sudo cpupower frequency-set -g performance   (nach der Messung wieder zuruecksetzen)"
fi

epp=$(cat /sys/devices/system/cpu/cpu0/cpufreq/energy_performance_preference 2>/dev/null || true)
if [[ -n "$epp" && "$epp" != "performance" ]]; then
    warn "Energy-Performance-Preference: $epp (erwartet: performance, amd-pstate/intel_pstate)"
fi

if command -v powerprofilesctl >/dev/null; then
    profile=$(powerprofilesctl get 2>/dev/null || echo unbekannt)
    if [[ "$profile" == "performance" ]]; then
        ok "Energieprofil: performance"
    else
        bad "Energieprofil: $profile (erwartet: performance)"
        hint "powerprofilesctl set performance"
    fi
fi

on_battery=0
for ps in /sys/class/power_supply/*; do
    [[ "$(cat "$ps/type" 2>/dev/null)" == "Mains" ]] || continue
    [[ "$(cat "$ps/online" 2>/dev/null)" == "1" ]] || on_battery=1
done
if (( on_battery )); then
    bad "Geraet laeuft auf Akku"
    hint "Netzteil anschliessen"
fi

# ---------------------------------------------------------------- Sperre/Suspend
section "Bildschirmsperre und Suspend"

if command -v gsettings >/dev/null && gsettings list-schemas 2>/dev/null | grep -q org.gnome.settings-daemon.plugins.power; then
    sleep_ac=$(gsettings get org.gnome.settings-daemon.plugins.power sleep-inactive-ac-type 2>/dev/null)
    idle=$(gsettings get org.gnome.desktop.session idle-delay 2>/dev/null | awk '{print $NF}')
    [[ "$sleep_ac" == "'nothing'" ]] && ok "Automatischer Suspend (Netzbetrieb): aus" \
        || { warn "Automatischer Suspend (Netzbetrieb): $sleep_ac"; hint "gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-ac-type 'nothing'"; }
    [[ "$idle" == "0" ]] && ok "Bildschirm-Leerlauf: aus" \
        || { warn "Bildschirm-Leerlauf nach ${idle}s (Sperre/Energiesparen kann greifen)"; hint "gsettings set org.gnome.desktop.session idle-delay 0"; }
else
    warn "Kein GNOME erkannt, Sperre/Suspend nicht automatisch pruefbar"
fi
echo "         Unabhaengig davon Messlaeufe immer so starten (verhindert Suspend/Leerlauf"
echo "         fuer die Dauer des Laufs, unabhaengig von der Desktop-Umgebung):"
echo "         systemd-inhibit --what=idle:sleep --why=Messung python run_uc1_measurement.py ..."

# ---------------------------------------------------------------- Last
section "Systemlast"

read -r load1 _ < /proc/loadavg
cores=$(nproc)
if awk -v l="$load1" 'BEGIN{exit !(l < 1.0)}'; then
    ok "Load (1 min): $load1 bei $cores Threads"
else
    warn "Load (1 min): $load1 bei $cores Threads (Hintergrundlast?)"
fi

busy=$(ps -eo pcpu=,comm= --sort=-pcpu | awk '$1 > 5.0' | head -5)
if [[ -n "$busy" ]]; then
    warn "Prozesse mit > 5 % CPU:"
    echo "$busy" | sed 's/^/           /'
    hint "Browser, IDE, Indexer, Updates schliessen"
else
    ok "Keine Prozesse mit > 5 % CPU"
fi

mem_avail_gb=$(awk '/MemAvailable/ {printf "%.1f", $2/1048576}' /proc/meminfo)
awk -v m="$mem_avail_gb" 'BEGIN{exit !(m >= 4)}' && ok "Freier Arbeitsspeicher: ${mem_avail_gb} GB" \
    || warn "Freier Arbeitsspeicher nur ${mem_avail_gb} GB"

free_gb=$(df -BG --output=avail "$SCRIPT_DIR" | tail -1 | tr -dc '0-9')
(( free_gb >= MIN_FREE_GB )) && ok "Freier Plattenplatz: ${free_gb} GB" \
    || bad "Freier Plattenplatz nur ${free_gb} GB (mind. ${MIN_FREE_GB} GB)"

# ---------------------------------------------------------------- Container
section "Container und Broker"

for c in kafka rabbitmq ibmmq; do
    state=$(podman inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo fehlt)
    [[ "$state" == "running" ]] && ok "Container $c laeuft" \
        || { bad "Container $c: $state"; hint "podman compose up -d"; }
done

others=$(podman ps --format '{{.Names}}' | grep -vxE 'kafka|rabbitmq|ibmmq' || true)
[[ -z "$others" ]] && ok "Keine weiteren Container aktiv" \
    || warn "Weitere Container aktiv: $(echo $others)"

BROKERS_UP=1
podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
    >/tmp/pmc_topics 2>/dev/null && ok "Kafka erreichbar" || { bad "Kafka nicht erreichbar"; BROKERS_UP=0; }
podman exec rabbitmq rabbitmq-diagnostics -q check_running >/dev/null 2>&1 \
    && ok "RabbitMQ erreichbar" || { bad "RabbitMQ nicht erreichbar"; BROKERS_UP=0; }
podman exec ibmmq dspmq -m QM1 2>/dev/null | grep -q 'STATUS(Running)' \
    && ok "IBM MQ QM1 laeuft" || { bad "IBM MQ QM1 laeuft nicht"; BROKERS_UP=0; }

# ---------------------------------------------------------------- Broker-Zustand
section "Broker-Zustand (Reste frueherer Laeufe)"

if (( BROKERS_UP )); then

depth=$(mqsc "DIS QL(DEV.QUEUE.2) CURDEPTH" | grep -oE 'CURDEPTH\([0-9]+\)' | tr -dc '0-9')
[[ "$depth" == "0" ]] && ok "IBM MQ DEV.QUEUE.2 leer" \
    || warn "IBM MQ DEV.QUEUE.2 Tiefe: ${depth:-unbekannt} (Orchestratoren leeren vor jedem Lauf, trotzdem pruefen)"

maxdepth=$(mqsc "DIS QL(DEV.QUEUE.2) MAXDEPTH" | grep -oE 'MAXDEPTH\([0-9]+\)' | tr -dc '0-9')
(( ${maxdepth:-0} >= 1200000 )) && ok "IBM MQ MAXDEPTH: $maxdepth" \
    || { bad "IBM MQ MAXDEPTH: ${maxdepth:-unbekannt}"; hint "./setup.sh erneut ausfuehren"; }

mqsc "DIS TOPIC(DEV.BROADCAST.TOPIC) TOPICSTR" | grep -q "TOPICSTR(dev/broadcast)" \
    && ok "IBM MQ Topic-Objekt DEV.BROADCAST.TOPIC vorhanden" \
    || { bad "IBM MQ Topic-Objekt DEV.BROADCAST.TOPIC fehlt (UC3 scheitert mit 2035)"; hint "./setup.sh erneut ausfuehren"; }

mq_subs=$(mqsc "DIS SUB('uc3-*')" | grep -oE "SUB\(uc3-[^)]*\)" || true)
[[ -z "$mq_subs" ]] && ok "Keine verwaisten IBM-MQ-Subscriptions" \
    || warn "Verwaiste IBM-MQ-Subscriptions: $(echo "$mq_subs" | wc -l) (UC3-Orchestrator raeumt auf)"

rabbit=$(podman exec rabbitmq rabbitmqctl list_queues -q name messages 2>/dev/null || true)
orphans=$(echo "$rabbit" | awk '$1 ~ /^broadcast\./' | wc -l)
(( orphans == 0 )) && ok "Keine verwaisten RabbitMQ-Broadcast-Queues" \
    || warn "Verwaiste RabbitMQ-Broadcast-Queues: $orphans (UC3-Orchestrator raeumt auf)"
nonempty=$(echo "$rabbit" | awk '$2 ~ /^[0-9]+$/ && $2 > 0 {print $1"("$2")"}' | tr '\n' ' ')
[[ -z "$nonempty" ]] && ok "Alle RabbitMQ-Queues leer" || warn "Nicht leere RabbitMQ-Queues: $nonempty"

if [[ -s /tmp/pmc_topics ]]; then
    k_orphans=$(grep -cE '^(loadbalance|broadcast|aggregation)\.' /tmp/pmc_topics || true)
    (( k_orphans == 0 )) && ok "Keine verwaisten Kafka-Lauf-Topics" \
        || warn "Verwaiste Kafka-Lauf-Topics: $k_orphans (abgebrochene Laeufe; loeschen oder ignorieren)"
    grep -qx latency.test /tmp/pmc_topics && ok "Kafka-Topic latency.test vorhanden" \
        || { bad "Kafka-Topic latency.test fehlt (UC1)"; hint "./setup.sh erneut ausfuehren"; }
fi
else
    warn "Uebersprungen, da nicht alle Broker erreichbar sind"
fi

# ---------------------------------------------------------------- Python
section "Python-Umgebung"

[[ -n "${CONDA_PREFIX:-}" ]] && { warn "Conda-Umgebung aktiv ($CONDA_PREFIX)"; hint "conda deactivate"; }

if [[ -x "$VENV_PY" ]]; then
    pyver=$("$VENV_PY" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
    [[ "$pyver" == "$EXPECTED_PYTHON" ]] && ok "venv-Python: $pyver" \
        || bad "venv-Python: $pyver (erwartet: $EXPECTED_PYTHON, wie auf dem Laptop)"

    if [[ -f requirements.txt ]]; then
        installed=$("$VENV_PY" -m pip freeze 2>/dev/null)
        missing=""
        while read -r req; do
            [[ -z "$req" || "$req" == \#* ]] && continue
            echo "$installed" | grep -qix "$req" || missing+="$req "
        done < requirements.txt
        [[ -z "$missing" ]] && ok "Paketversionen entsprechen requirements.txt" \
            || { bad "Abweichend oder fehlend: $missing"; hint "venv/bin/pip install -r requirements.txt"; }
    fi
else
    bad "venv fehlt ($VENV_PY)"; hint "./setup.sh"
fi

# ---------------------------------------------------------------- Snapshot
{
    echo "Messumgebung, erfasst $(date -Iseconds)"
    echo
    echo "Host:        $(hostname)"
    echo "OS:          $(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME")"
    echo "Kernel:      $(uname -r)"
    echo "CPU:         $(awk -F: '/model name/ {print $2; exit}' /proc/cpuinfo | xargs)"
    echo "Threads:     $cores"
    echo "RAM gesamt:  $(awk '/MemTotal/ {printf "%.1f GB", $2/1048576}' /proc/meminfo)"
    echo "Governor:    $governors"
    echo "EPP:         ${epp:-n/a}"
    echo "Energieprof: ${profile:-n/a}"
    echo "Load (1min): $load1"
    echo "Platte:      $(df -h --output=source,fstype "$SCRIPT_DIR" | tail -1)"
    echo
    echo "Podman:      $(podman --version)"
    podman ps --format '  {{.Names}}: {{.Image}}'
    echo
    echo "Python venv: $("$VENV_PY" --version 2>&1)"
    "$VENV_PY" -m pip freeze 2>/dev/null | sed 's/^/  /'
    echo
    echo "Pruefergebnis: $FAILS FAIL, $WARNS WARN"
} > "$SNAPSHOT"

section "Ergebnis"
echo "  $FAILS FAIL, $WARNS WARN. Umgebungs-Snapshot: $SNAPSHOT"
if (( FAILS > 0 )); then
    echo "  NICHT messen, erst die FAIL-Punkte beheben."
    exit 1
fi
(( WARNS > 0 )) && echo "  Messung moeglich, WARN-Punkte bewusst pruefen." || echo "  Umgebung bereit."
exit 0
