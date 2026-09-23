"""Contention-Monitoring waehrend der Latenzmessung.

Laeuft PARALLEL zu Producer/Consumer in einem eigenen Terminal und
protokolliert CPU-/RAM-Auslastung von Host und allen vier Podman-Containern
in ein CSV. Vor dem Start des eigentlichen Testlaufs starten, nach Ende
des Testlaufs mit Strg+C beenden.

Benoetigt: pip install psutil

WICHTIG vor dem ersten Einsatz: einmal manuell pruefen, ob
`podman stats --no-stream --format "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"`
bei euch die erwartete Ausgabe liefert (eine Zeile pro Container,
durch Pipe getrennt). Das Format-Template ist stabiler als JSON-Output,
kann sich aber je nach Podman-Version in Details unterscheiden.
"""

import csv
import subprocess
import time
from datetime import datetime, timezone

import psutil

OUTPUT_FILE = "contention_log.csv"
INTERVAL_SECONDS = 0.5
CONTAINERS = ["rabbitmq", "kafka", "kafka-ui", "ibmmq"]


def get_container_stats():
    """Liest aktuelle CPU-/Mem-Werte aller Container per `podman stats`."""
    result = subprocess.run(
        [
            "podman", "stats", "--no-stream",
            "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    stats_by_name = {}
    for line in result.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) != 3:
            continue
        name, cpu_perc, mem_usage = parts
        stats_by_name[name] = {"cpu_perc": cpu_perc, "mem_usage": mem_usage}
    return stats_by_name


def main():
    core_count = psutil.cpu_count()
    fieldnames = ["timestamp"]
    fieldnames += [f"host_cpu_core_{i}" for i in range(core_count)]
    fieldnames += ["host_cpu_total", "host_mem_percent"]
    for name in CONTAINERS:
        fieldnames += [f"{name}_cpu_perc", f"{name}_mem_usage"]

    print(
        f"Contention-Monitoring gestartet, schreibe nach {OUTPUT_FILE}.\n"
        "Jetzt in anderen Terminals zuerst den Consumer, dann den Producer "
        "starten.\nMit Strg+C beenden, wenn der Testlauf fertig ist."
    )

    # Erster Aufruf von cpu_percent() liefert immer 0.0 (Referenzwert),
    # daher einmal vorab "verbrauchen".
    psutil.cpu_percent(percpu=True)
    psutil.cpu_percent()

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        try:
            while True:
                timestamp = datetime.now(timezone.utc).isoformat()
                per_core = psutil.cpu_percent(percpu=True)
                total_cpu = psutil.cpu_percent()
                mem_percent = psutil.virtual_memory().percent

                row = {
                    "timestamp": timestamp,
                    "host_cpu_total": total_cpu,
                    "host_mem_percent": mem_percent,
                }
                for i, val in enumerate(per_core):
                    row[f"host_cpu_core_{i}"] = val

                try:
                    container_stats = get_container_stats()
                except subprocess.CalledProcessError as e:
                    print(f"podman stats fehlgeschlagen: {e}")
                    container_stats = {}

                for name in CONTAINERS:
                    stats = container_stats.get(name, {})
                    row[f"{name}_cpu_perc"] = stats.get("cpu_perc", "")
                    row[f"{name}_mem_usage"] = stats.get("mem_usage", "")

                writer.writerow(row)
                f.flush()
                time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("Monitoring beendet.")


if __name__ == "__main__":
    main()
