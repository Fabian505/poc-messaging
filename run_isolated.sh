#!/usr/bin/env bash
#
# run_isolated.sh - fuehrt einen Messbefehl aus, waehrend nur der Broker der
# gemessenen Technologie laeuft. Die anderen beiden Broker-Container werden
# vorher gestoppt und danach wieder gestartet.
#
# Grund: podman stats zeigt auch im Leerlauf CPU-Last bei allen drei Brokern
# (RabbitMQ ~13%, Kafka ~12%, siehe Lessons Learned). Ohne Isolation misst
# jede Technologie unter Konkurrenz durch die beiden anderen.
#
# Nutzung:
#   ./run_isolated.sh <kafka|rabbitmq|ibmmq> <Befehl...>
#
# Beispiele:
#   ./run_isolated.sh kafka python run_uc1_measurement.py kafka 10000 5
#   ./run_isolated.sh rabbitmq python run_uc2_measurement.py rabbitmq 100000 10 1,2,4,8
#
# "all"-Aufrufe der Orchestratoren (z.B. run_uc1_measurement.py all ...)
# ergeben unter Isolation keinen Sinn, da immer nur EIN Broker laeuft.
# Fuer "alle Technologien isoliert nacheinander" dieses Skript dreimal mit
# der jeweiligen Einzeltechnologie aufrufen (siehe Anleitung, Abschnitt B3).
#
# Idempotent/robust: startet am Ende IMMER alle drei Broker wieder, auch bei
# Fehlern oder Abbruch (Ctrl-C) waehrend der Messung.

set -uo pipefail

ALL_BROKERS=(kafka rabbitmq ibmmq)
READY_TIMEOUT=180
KAFKA_STABILIZE_SECONDS="${KAFKA_STABILIZE_SECONDS:-5}"  # nur Netzwerk-/Socket-Anlauf,
                                                              # NICHT die JIT-Aufwaermung

if [[ $# -lt 2 || ! " ${ALL_BROKERS[*]} " =~ " $1 " ]]; then
    echo "Nutzung: $0 <kafka|rabbitmq|ibmmq> <Befehl...>" >&2
    exit 1
fi

TARGET="$1"; shift
OTHERS=()
for b in "${ALL_BROKERS[@]}"; do
    [[ "$b" != "$TARGET" ]] && OTHERS+=("$b")
done

wait_ready() {
    local tech="$1" waited=0
    echo "Warte auf $tech ..."
    until check_ready "$tech"; do
        (( waited >= READY_TIMEOUT )) && { echo "FEHLER: $tech nach ${READY_TIMEOUT}s nicht bereit." >&2; return 1; }
        sleep 2; waited=$((waited + 2))
    done
    echo "  $tech antwortet (${waited}s)."

    if [[ "$tech" == "kafka" ]]; then
        # kafka-topics --list antwortet, sobald der Broker Metadaten liefert,
        # das ist aber nicht dasselbe wie "eingeschwungen". Der eigentliche
        # Effekt (siehe Lessons Learned, 24.09.2026) ist NICHT zeitbasiert,
        # sondern durchsatzbasiert: die Kafka-JVM interpretiert Bytecode
        # zunaechst, der JIT-Compiler kompiliert haeufig durchlaufene
        # Codepfade erst nach ausreichend vielen Aufrufen (Hot-Spot-
        # Erkennung). Ein reines sleep() aendert daran nichts, ein zuvor
        # getesteter fixer 20s-Delay half nachweislich NICHT (Aufwaermlauf
        # mit 500 Nachrichten blieb durchgehend bei ~270 ms). Diese kurze
        # Wartezeit deckt nur das Anlaufen von Netzwerk/Sockets ab.
        # Die eigentliche Aufwaermung MUSS über echten Nachrichtendurchsatz
        # erfolgen (siehe README, Kafka-Aufwaermlauf mit ausreichend
        # Nachrichten, nicht nur ausreichend Wartezeit).
        echo "  kurze Anlaufzeit fuer Kafka: ${KAFKA_STABILIZE_SECONDS}s (JIT-Aufwaermung erfolgt ueber Durchsatz, siehe Aufwaermlauf) ..."
        sleep "$KAFKA_STABILIZE_SECONDS"
    fi

    if [[ "$tech" == "ibmmq" ]]; then
        # Beobachtet 24.09.2026: MAXDEPTH(1200000) war direkt nach setup.sh
        # bestaetigt gesetzt, stand nach mehreren Stopp/Start-Zyklen durch
        # dieses Skript aber wieder auf dem Image-Standard 5000 (Q_FULL bei
        # nur 10.000 Nachrichten). Ursache nicht abschliessend geklaert,
        # deshalb hier defensiv bei JEDEM ibmmq-Start neu erzwungen statt nur
        # einmalig in setup.sh, damit ein Rueckfall nicht unbemerkt bleibt.
        local maxdepth
        maxdepth=$(podman exec ibmmq bash -c "echo 'DIS QL(DEV.QUEUE.2) MAXDEPTH' | runmqsc QM1" 2>/dev/null)
        if ! echo "$maxdepth" | grep -q "MAXDEPTH(1200000)"; then
            echo "  WARNUNG: DEV.QUEUE.2 MAXDEPTH ist nicht 1200000, setze neu ..."
            podman exec ibmmq bash -c "echo 'ALTER QLOCAL(DEV.QUEUE.2) MAXDEPTH(1200000)' | runmqsc QM1" >/dev/null
        fi
        local topicstr
        topicstr=$(podman exec ibmmq bash -c "echo \"DIS TOPIC(DEV.BROADCAST.TOPIC) TOPICSTR\" | runmqsc QM1" 2>/dev/null)
        if ! echo "$topicstr" | grep -q "TOPICSTR(dev/broadcast)"; then
            echo "  WARNUNG: Topic-Objekt DEV.BROADCAST.TOPIC fehlt, lege es neu an ..."
            podman exec ibmmq bash -c "echo \"DEFINE TOPIC(DEV.BROADCAST.TOPIC) TOPICSTR('dev/broadcast') REPLACE\" | runmqsc QM1" >/dev/null
        fi
    fi
    echo "  $tech bereit (${waited}s + ggf. Stabilisierung)."
}

check_ready() {
    case "$1" in
        kafka)    podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 ;;
        rabbitmq) podman exec rabbitmq rabbitmq-diagnostics -q check_running >/dev/null 2>&1 ;;
        ibmmq)    local dspmq_out; dspmq_out=$(podman exec ibmmq dspmq -m QM1 2>/dev/null)
                  echo "$dspmq_out" | grep -q 'STATUS(Running)' ;;
    esac
}

cleanup() {
    echo
    echo "Starte gestoppte Broker wieder: ${OTHERS[*]}"
    for b in "${OTHERS[@]}"; do
        podman start "$b" >/dev/null
    done
    for b in "${OTHERS[@]}"; do
        wait_ready "$b" || echo "  WARNUNG: $b nicht rechtzeitig bereit, manuell pruefen."
    done
}
trap cleanup EXIT

echo "Isoliere $TARGET: stoppe ${OTHERS[*]} ..."
for b in "${OTHERS[@]}"; do
    podman stop "$b" >/dev/null
done

wait_ready "$TARGET" || exit 1

echo "Fuehre aus: $*"
echo
"$@"
STATUS=$?

echo
echo "Befehl beendet mit Exit-Code $STATUS."
exit $STATUS