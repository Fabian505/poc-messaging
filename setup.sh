#!/usr/bin/env bash
#
# setup.sh - einmalig vor dem allerersten `podman compose up` auf einem neuen
# Rechner ausfuehren (oder erneut, falls die Datenverzeichnisse geloescht und
# neu angelegt wurden).
#
# Loest das wiederkehrende UID-Mapping-Problem bei rootless Podman: die
# Bind-Mount-Verzeichnisse fuer Kafka (UID 1000) und IBM MQ (UID 1001)
# gehoeren sonst dem normalen Host-Benutzer, waehrend die Container mit ihren
# eigenen internen UIDs laufen und deshalb nicht hineinschreiben duerfen.
# Fuehrt beim Start zu "permission denied" (IBM MQ) bzw. einem Fehler beim
# Schreiben von meta.properties (Kafka), siehe Lessons Learned.
#
# RabbitMQ braucht kein vergleichbares Mapping und wird hier nicht angefasst.
#
# Idempotent: kann gefahrlos mehrfach ausgefuehrt werden.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Lege Datenverzeichnisse an, falls sie noch nicht existieren ..."
mkdir -p kafka/data rabbitmq/data ibmmq/data

echo "Setze UID-Mapping fuer Kafka (1000:0) ..."
podman unshare chown -R 1000:0 kafka/data

echo "Setze UID-Mapping fuer IBM MQ (1001:0) ..."
podman unshare chown -R 1001:0 ibmmq/data

echo "Fertig. Starte Container ..."
podman compose up -d

echo ""
echo "Status:"
podman ps -a

echo ""
echo "Falls Kafka oder IBM MQ trotzdem sofort wieder 'Exited' zeigen:"
echo "  podman logs --tail 50 kafka"
echo "  podman logs --tail 50 ibmmq"
