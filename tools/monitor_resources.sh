#!/bin/bash
# monitor_resources.sh <label> <device> -- <befehl...>
# Startet iostat + podman stats + psutil-CPU-Sampler parallel zu <befehl>,
# schreibt alles nach messungen/resources/<label>_*.log, beendet die
# Sampler sauber nach Abschluss von <befehl> und gibt dessen Exit-Code zurück.

set -uo pipefail

LABEL="$1"; DEVICE="$2"; shift 2
if [ "$1" != "--" ]; then echo "Usage: monitor_resources.sh <label> <device> -- <cmd...>"; exit 2; fi
shift

OUTDIR="messungen/resources"
mkdir -p "$OUTDIR"
TS=$(date +%Y%m%d_%H%M%S)
IOSTAT_LOG="$OUTDIR/${LABEL}_iostat_${TS}.log"
POD_LOG="$OUTDIR/${LABEL}_podstats_${TS}.jsonl"
PSUTIL_LOG="$OUTDIR/${LABEL}_psutil_${TS}.jsonl"

# 1) iostat, geraetebezogen, 1s-Intervall
iostat -x 1 "$DEVICE" > "$IOSTAT_LOG" &
IOSTAT_PID=$!

# 2) podman stats, 1s-Intervall, JSON pro Zeile
( while true; do
    podman stats --no-stream --format json kafka rabbitmq ibmmq 2>/dev/null \
      | sed "s/^/{\"ts\":\"$(date -Iseconds)\",\"stats\":/;s/\$/}/" >> "$POD_LOG"
    sleep 1
  done ) &
POD_PID=$!

# 3) psutil-CPU der Python-Clientprozesse (Producer/Consumer), 1s-Intervall
python3 - "$PSUTIL_LOG" <<'PYEOF' &
import sys, time, json, psutil
outfile = sys.argv[1]
with open(outfile, "a") as f:
    while True:
        for p in psutil.process_iter(["pid", "name", "cmdline"]):
            cmd = " ".join(p.info.get("cmdline") or [])
            if any(x in cmd for x in ("_producer", "_consumer", "run_uc")):
                try:
                    cpu = p.cpu_percent(interval=None)
                    rss = p.memory_info().rss
                    f.write(json.dumps({"ts": time.time(), "pid": p.pid, "cmd": cmd[:60],
                                         "cpu_percent": cpu, "rss": rss}) + "\n")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        f.flush()
        time.sleep(1)
PYEOF
PSUTIL_PID=$!

cleanup() {
  kill "$IOSTAT_PID" "$POD_PID" "$PSUTIL_PID" 2>/dev/null
  wait "$IOSTAT_PID" "$POD_PID" "$PSUTIL_PID" 2>/dev/null
}
trap cleanup EXIT

# eigentlichen Messbefehl ausfuehren, Exit-Code durchreichen
"$@"
CMD_EXIT=$?

exit "$CMD_EXIT"
