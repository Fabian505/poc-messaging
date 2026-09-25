"""UC1 Einfache Warteschlange - Orchestrator fuer komplette Messreihen.

Fuehrt N Wiederholungen einer Latenzmessung fuer eine oder alle drei
Technologien durch, leert vor jedem Lauf automatisch die Queue/das Topic
(wo notwendig, siehe Kapitel 5.2/6 zu den jeweiligen Eigenheiten), sammelt
die Ergebnisse und schreibt am Ende einen Markdown-Bericht.

Nutzung:
    python run_uc1_measurement.py <kafka|rabbitmq|ibmmq|all> <nachrichten_anzahl> <wiederholungen>

Beispiel:
    python run_uc1_measurement.py all 100000 5
    python run_uc1_measurement.py ibmmq 1000000 3

Voraussetzung: kafka_producer_sync_paced.py, kafka_consumer.py,
rabbitmq_producer_sync.py, rabbitmq_consumer.py, ibmmq_producer_paced.py,
ibmmq_consumer.py liegen im selben Verzeichnis und lesen MEASURE_COUNT aus
der gleichnamigen Umgebungsvariable (nicht aus einem Kommandozeilen-
argument, die Skripte nehmen keine sys.argv-Parameter entgegen).
"""

import os
import queue
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Producer-/Consumer-Skripte liegen im selben Verzeichnis wie dieser
# Orchestrator. Absoluter Pfad, damit der Aufruf unabhaengig vom aktuellen
# Arbeitsverzeichnis funktioniert (z.B. ueber run_isolated.sh von der
# Repo-Wurzel aus, oder aus einem anderen Terminal-Verzeichnis).
def script_path(name):
    return os.path.join(SCRIPT_DIR, name)

VALID_TECHNOLOGIES = ["kafka", "rabbitmq", "ibmmq"]

TECH_CONFIG = {
    "kafka": {
        "producer_script": script_path("kafka_producer_sync_paced.py"),
        "consumer_script": script_path("kafka_consumer.py"),
        "clear_cmd": None,  # nicht noetig, siehe Docstring von kafka_consumer.py
        # Kafka druckt "Bereit" bereits VOR Abschluss des asynchronen
        # Consumer-Group-Rebalance (siehe Kapitel 5.2), zusaetzlicher Puffer
        # nach der erkannten Bereitschaft noetig.
        "post_ready_buffer": 3.0,
    },
    "rabbitmq": {
        "producer_script": script_path("rabbitmq_producer_sync.py"),
        "consumer_script": script_path("rabbitmq_consumer.py"),
        "clear_cmd": ["podman", "exec", "rabbitmq", "rabbitmqctl", "purge_queue", "latency.test"],
        "post_ready_buffer": 0.5,
    },
    "ibmmq": {
        "producer_script": script_path("ibmmq_producer_paced.py"),
        "consumer_script": script_path("ibmmq_consumer.py"),
        "clear_cmd": ["podman", "exec", "ibmmq", "bash", "-c",
                       "echo 'CLEAR QLOCAL(DEV.QUEUE.2)' | runmqsc QM1"],
        "post_ready_buffer": 0.5,
    },
}

READY_SUBSTRING = "Bereit"
READY_TIMEOUT_SECONDS = 20.0

SECONDS_PER_MESSAGE_TIMEOUT = 0.01
MIN_TIMEOUT_SECONDS = 60

SUMMARY_PATTERN = re.compile(
    r"Anzahl Messwerte:\s*(?P<n>\d+)\s*"
    r"Min:\s*(?P<min>[\d.]+) ms\s*"
    r"Max:\s*(?P<max>[\d.]+) ms\s*"
    r"Mittel:\s*(?P<mean>[\d.]+) ms\s*"
    r"Median:\s*(?P<median>[\d.]+) ms\s*"
    r"P95:\s*(?P<p95>[\d.]+) ms\s*"
    r"P99:\s*(?P<p99>[\d.]+) ms\s*"
    r"(Stdabw:\s*(?P<stdabw>[\d.]+) ms)?\s*"
    r"(Drift:\s*(?P<drift>-?[\d.]+) ms)?",
    re.MULTILINE,
)

RATE_PATTERN = re.compile(
    r"Tatsaechliche Rate:\s*(?P<rate>[\d.]+) Nachrichten/s.*?"
    r"Rueckstand:\s*(?P<behind>\d+)"
)


def verified_depth(technology):
    """Fragt die tatsaechliche Tiefe ab (0 bei Kafka, nicht anwendbar)."""
    if technology == "rabbitmq":
        out = subprocess.run(
            ["podman", "exec", "rabbitmq", "rabbitmqctl", "list_queues", "name", "messages"],
            capture_output=True, text=True,
        ).stdout
        for line in out.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == "latency.test":
                return int(parts[1])
        return 0  # Queue existiert noch nicht -> leer
    if technology == "ibmmq":
        out = subprocess.run(
            ["podman", "exec", "ibmmq", "bash", "-c",
             "echo 'DIS QL(DEV.QUEUE.2) CURDEPTH' | runmqsc QM1"],
            capture_output=True, text=True,
        ).stdout
        m = re.search(r"CURDEPTH\((\d+)\)", out)
        return int(m.group(1)) if m else None
    return 0


def ibmmq_maxdepth():
    out = subprocess.run(
        ["podman", "exec", "ibmmq", "bash", "-c",
         "echo 'DIS QL(DEV.QUEUE.2) MAXDEPTH' | runmqsc QM1"],
        capture_output=True, text=True,
    ).stdout
    m = re.search(r"MAXDEPTH\((\d+)\)", out)
    return int(m.group(1)) if m else None


def clear_queue(technology, needed=0):
    config = TECH_CONFIG[technology]
    if config["clear_cmd"] is None:
        return
    # WICHTIG: check=False allein reicht nicht, ein fehlgeschlagenes CLEAR
    # (z.B. weil ein abgestuerzter Prozess noch ein Handle haelt) blieb
    # sonst unbemerkt, und alte Nachrichten haetten sich unbemerkt ueber
    # viele Laeufe hinweg angesammelt (beobachtet: DEV.QUEUE.2 lief bis
    # MAXDEPTH voll, MQRC_Q_FULL beim naechsten Producer). Tiefe danach
    # explizit verifizieren und laut abbrechen statt still weiterzulaufen.
    subprocess.run(config["clear_cmd"], check=False, capture_output=True)
    depth = verified_depth(technology)
    if depth != 0:
        raise RuntimeError(
            f"{technology}: Queue nach clear_cmd nicht leer (Tiefe: {depth}). "
            f"Vermutlich ein haengendes Handle von einem abgestuerzten Prozess. "
            f"Manuell pruefen, z.B. 'podman restart {technology}' und clear_cmd "
            f"erneut versuchen, bevor weitergemessen wird."
        )
    # Zusaetzlich zur Leer-Pruefung: MAXDEPTH selbst kann unabhaengig davon
    # zurueckfallen (beobachtet 24.09.2026, Ursache ungeklaert - moeglich
    # auch WAEHREND einer laufenden Isolation, nicht nur bei einem Neustart
    # von run_isolated.sh). Eine leere Queue mit zu niedrigem MAXDEPTH
    # besteht die Leer-Pruefung, crasht aber trotzdem mit MQRC_Q_FULL.
    if technology == "ibmmq" and needed > 0:
        maxdepth = ibmmq_maxdepth()
        if maxdepth is None or maxdepth < needed:
            raise RuntimeError(
                f"ibmmq: MAXDEPTH({maxdepth}) reicht nicht fuer {needed} Nachrichten. "
                f"'podman exec ibmmq bash -c \"echo 'ALTER QLOCAL(DEV.QUEUE.2) "
                f"MAXDEPTH(1200000)' | runmqsc QM1\"' und erneut versuchen."
            )


def start_reader_thread(proc):
    """Startet einen Daemon-Thread, der proc.stdout zeilenweise in eine
    Queue schreibt (None als Sentinel bei EOF). Robuster als select() auf
    einem gepufferten Text-Stream."""
    q = queue.Queue()

    def _reader():
        for line in iter(proc.stdout.readline, ""):
            q.put(line)
        q.put(None)

    thread = threading.Thread(target=_reader, daemon=True)
    thread.start()
    return q


def wait_for_ready_line(q, timeout=READY_TIMEOUT_SECONDS):
    lines = []
    deadline = time.time() + timeout
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise TimeoutError(
                f"Consumer hat innerhalb von {timeout}s keine Bereitschaft "
                f"gemeldet (erwartet: Zeile mit '{READY_SUBSTRING}'). "
                f"Bisherige Ausgabe: {''.join(lines)!r}"
            )
        try:
            line = q.get(timeout=remaining)
        except queue.Empty:
            continue
        if line is None:
            raise EOFError(
                f"Consumer hat sich beendet, bevor eine Bereitschaftszeile "
                f"kam. Bisherige Ausgabe: {''.join(lines)!r}"
            )
        lines.append(line)
        if READY_SUBSTRING in line:
            return lines


def drain_queue_until_eof(q, lines_so_far, timeout):
    lines = list(lines_so_far)
    deadline = time.time() + timeout
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise TimeoutError(f"Consumer hat nicht innerhalb von {timeout}s fertig gemeldet.")
        try:
            line = q.get(timeout=remaining)
        except queue.Empty:
            continue
        if line is None:
            break
        lines.append(line)
    return "".join(lines)


def run_single_measurement(technology, message_count):
    config = TECH_CONFIG[technology]
    env = os.environ.copy()
    env["MEASURE_COUNT"] = str(message_count)
    # Kindprozesse schreiben in eine Pipe (kein TTY) -> Python puffert
    # stdout blockweise, die "Bereit"-Zeile kaeme erst bei Prozessende an.
    env["PYTHONUNBUFFERED"] = "1"

    clear_queue(technology, needed=message_count + 10)  # +10 Warmup

    consumer_proc = subprocess.Popen(
        [sys.executable, "-u", config["consumer_script"]],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    producer_proc = None

    try:
        consumer_queue = start_reader_thread(consumer_proc)
        pre_ready_lines = wait_for_ready_line(consumer_queue)
        time.sleep(config["post_ready_buffer"])

        producer_proc = subprocess.Popen(
            [sys.executable, "-u", config["producer_script"]],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        producer_timeout = max(MIN_TIMEOUT_SECONDS, message_count * SECONDS_PER_MESSAGE_TIMEOUT)
        producer_output, _ = producer_proc.communicate(timeout=producer_timeout)
        if producer_proc.returncode != 0:
            raise RuntimeError(
                f"Producer mit Exit-Code {producer_proc.returncode} beendet. "
                f"Ausgabe:\n{producer_output}"
            )

        consumer_timeout = max(MIN_TIMEOUT_SECONDS, message_count * SECONDS_PER_MESSAGE_TIMEOUT) + 30
        consumer_output = drain_queue_until_eof(consumer_queue, pre_ready_lines, consumer_timeout)
        consumer_proc.wait(timeout=10)
    finally:
        for proc in (producer_proc, consumer_proc):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait()

    rate_match = RATE_PATTERN.search(producer_output)
    match = SUMMARY_PATTERN.search(consumer_output)
    if not match:
        print(f"WARNUNG: Zusammenfassung nicht erkannt, Rohausgabe:\n{consumer_output}")
        return None

    return {
        "n": int(match.group("n")),
        "min": float(match.group("min")),
        "max": float(match.group("max")),
        "mean": float(match.group("mean")),
        "median": float(match.group("median")),
        "p95": float(match.group("p95")),
        "p99": float(match.group("p99")),
        "stdabw": float(match.group("stdabw")) if match.group("stdabw") else None,
        "drift": float(match.group("drift")) if match.group("drift") else None,
        "rate": float(rate_match.group("rate")) if rate_match else None,
        "behind": int(rate_match.group("behind")) if rate_match else None,
    }


def run_technology(technology, message_count, repetitions):
    results = []
    for i in range(1, repetitions + 1):
        print(f"[{technology}] Lauf {i}/{repetitions} mit {message_count} Nachrichten ...")
        try:
            result = run_single_measurement(technology, message_count)
        except (TimeoutError, EOFError, subprocess.TimeoutExpired) as e:
            print(f"[{technology}] Lauf {i} abgebrochen: {e}")
            result = None
        except Exception as e:
            print(f"[{technology}] Lauf {i} abgebrochen (unerwarteter Fehler): {e}")
            result = None

        if result is None:
            print(f"[{technology}] Lauf {i} fehlgeschlagen, wird uebersprungen.")
            continue
        print(f"[{technology}] Lauf {i}: Mittel={result['mean']:.2f} ms, "
              f"P99={result['p99']:.2f} ms, Rate={result['rate']} msg/s, "
              f"Rueckstand={result['behind']}")
        results.append(result)
    return results


def write_report(all_results, message_count, repetitions):
    timestamp = datetime.now(timezone.utc).isoformat()
    target_rate = float(os.environ.get("TARGET_RATE", 500))
    report_path = f"uc1_bericht_{message_count}n_{target_rate:.0f}r_{int(time.time())}.md"

    with open(report_path, "w") as f:
        f.write("# UC1 Einfache Warteschlange - Messbericht\n\n")
        f.write(f"Erstellt: {timestamp}\n\n")
        f.write(f"Nachrichten pro Lauf: {message_count}, Wiederholungen: {repetitions}, "
                f"Soll-Last: {target_rate:.0f} Nachrichten/s\n\n")

        for technology, results in all_results.items():
            f.write(f"## {technology}\n\n")
            if not results:
                f.write("Keine erfolgreichen Laeufe.\n\n")
                continue

            f.write("| Lauf | Min (ms) | Max (ms) | Mittel (ms) | Median (ms) | P95 (ms) | P99 (ms) | Stdabw (ms) | Drift (ms) | Rate (msg/s) | Rueckstand |\n")
            f.write("|---|---|---|---|---|---|---|---|---|---|---|\n")
            for i, r in enumerate(results, start=1):
                stdabw = f"{r['stdabw']:.2f}" if r["stdabw"] is not None else "-"
                rate = f"{r['rate']:.1f}" if r["rate"] is not None else "-"
                drift = f"{r['drift']:.2f}" if r.get("drift") is not None else "-"
                behind = str(r["behind"]) if r["behind"] is not None else "-"
                f.write(f"| {i} | {r['min']:.2f} | {r['max']:.2f} | {r['mean']:.2f} | "
                        f"{r['median']:.2f} | {r['p95']:.2f} | {r['p99']:.2f} | {stdabw} | {drift} | {rate} | {behind} |\n")

            mean_of_means = sum(r["mean"] for r in results) / len(results)
            f.write(f"\n**Mittelwert über alle {len(results)} Läufe: {mean_of_means:.2f} ms**\n\n")

    print(f"\nBericht geschrieben: {report_path}")
    return report_path


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in VALID_TECHNOLOGIES + ["all"]:
        print(
            "Nutzung: python run_uc1_measurement.py "
            f"<{'|'.join(VALID_TECHNOLOGIES)}|all> <nachrichten_anzahl> <wiederholungen>"
        )
        sys.exit(1)

    technology_arg = sys.argv[1]
    message_count = int(sys.argv[2])
    repetitions = int(sys.argv[3])

    technologies = VALID_TECHNOLOGIES if technology_arg == "all" else [technology_arg]

    all_results = {}
    for technology in technologies:
        all_results[technology] = run_technology(technology, message_count, repetitions)

    write_report(all_results, message_count, repetitions)


if __name__ == "__main__":
    main()