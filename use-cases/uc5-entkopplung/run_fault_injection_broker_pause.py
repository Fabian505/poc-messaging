"""Use Case 4.2.4 - Orchestrator fuer die Fehlerinjektion "Broker kurz
pausiert" (Ersatz fuer eine Netzwerk-Unterbrechung auf Consumer-Seite,
siehe Kapitel 4.2.4/5).

Nutzung:
    python run_fault_injection_broker_pause.py <technologie> <semantik> [pause_sekunden]

<technologie>: kafka | rabbitmq | ibmmq
<semantik>: at-most-once | at-least-once | exactly-once
[pause_sekunden]: optional, Standard 5

Startet Consumer und Producer normal (keine resilienten Producer-Varianten
noetig, der Broker faellt hier nicht aus, er wird nur kurz eingefroren,
Verbindungen bleiben technisch bestehen und laufen nach dem Unpause weiter).
Pausiert den Broker-Container waehrend des laufenden Sendevorgangs und gibt
ihn danach wieder frei.
"""

import subprocess
import sys
import time

VALID_SEMANTICS = {"at-most-once", "at-least-once", "exactly-once"}

TECH_CONFIG = {
    "kafka": {
        "container": "kafka",
        "producer_cmd": lambda semantics: ["python", "zustellsemantik_kafka_producer.py", semantics],
        "consumer_cmd": lambda run_id, semantics: ["python", "zustellsemantik_kafka_consumer.py", run_id, semantics],
    },
    "rabbitmq": {
        "container": "rabbitmq",
        "producer_cmd": lambda semantics: ["python", "zustellsemantik_rabbitmq_producer.py"],
        "consumer_cmd": lambda run_id, semantics: ["python", "zustellsemantik_rabbitmq_consumer.py", run_id, semantics],
    },
    "ibmmq": {
        "container": "ibmmq",
        "producer_cmd": lambda semantics: ["python", "zustellsemantik_ibmmq_producer.py"],
        "consumer_cmd": lambda run_id, semantics: ["python", "zustellsemantik_ibmmq_consumer.py", run_id, semantics],
    },
}


def main():
    if len(sys.argv) not in (3, 4) or sys.argv[1] not in TECH_CONFIG or sys.argv[2] not in VALID_SEMANTICS:
        print(
            f"Nutzung: python run_fault_injection_broker_pause.py "
            f"<{'|'.join(TECH_CONFIG)}> <{'|'.join(VALID_SEMANTICS)}> [pause_sekunden]"
        )
        sys.exit(1)

    technology, semantics = sys.argv[1], sys.argv[2]
    pause_seconds = float(sys.argv[3]) if len(sys.argv) == 4 else 5.0
    config = TECH_CONFIG[technology]
    run_id = f"pausetest-{technology}-{semantics}-{int(time.time())}"
    print(f"run_id fuer diesen Lauf: {run_id}")

    consumer_proc = subprocess.Popen(config["consumer_cmd"](run_id, semantics))
    time.sleep(2.0)

    producer_proc = subprocess.Popen(config["producer_cmd"](semantics))

    time.sleep(0.5)
    print(f"Pausiere Container '{config['container']}' fuer {pause_seconds}s ...")
    subprocess.run(["podman", "pause", config["container"]], check=True)
    time.sleep(pause_seconds)
    subprocess.run(["podman", "unpause", config["container"]], check=True)
    print(f"Container '{config['container']}' wieder freigegeben.")

    producer_proc.wait()
    print("Producer fertig, warte auf Consumer-Abschluss (Idle-Timeout)...")
    consumer_proc.wait()

    print(f"Fehlerinjektion abgeschlossen. run_id: {run_id}")
    print(f"Auswertung: python analyze_semantics.py results_semantics.jsonl {run_id} 200")


if __name__ == "__main__":
    main()
