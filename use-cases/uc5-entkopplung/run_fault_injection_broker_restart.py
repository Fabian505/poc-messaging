"""Use Case 4.2.4 - Orchestrator fuer die Fehlerinjektion "Broker-Neustart
waehrend des Sendevorgangs".

Nutzung:
    python run_fault_injection_broker_restart.py <technologie> <semantik>

<technologie>: kafka | rabbitmq | ibmmq
<semantik>: at-most-once | at-least-once | exactly-once

Nutzt fuer RabbitMQ und IBM MQ bewusst die "_resilient"-Producer-Varianten
mit eingebauter Wiederverbindung, siehe deren Docstrings fuer die
Begruendung. Kafka nutzt den normalen Producer, da confluent-kafka
(librdkafka) Wiederverbindung und Retries bereits intern uebernimmt.

Ein echter Neustart eines Containers dauert je nach Technologie
unterschiedlich lang (IBM MQ typischerweise am laengsten, siehe eure
Podman-Logs vom Erststart), das Skript wartet daher grosszuegig, bevor es
auf den Abschluss von Producer/Consumer wartet.
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
        "producer_cmd": lambda semantics: ["python", "zustellsemantik_rabbitmq_producer_resilient.py"],
        "consumer_cmd": lambda run_id, semantics: ["python", "zustellsemantik_rabbitmq_consumer.py", run_id, semantics],
    },
    "ibmmq": {
        "container": "ibmmq",
        "producer_cmd": lambda semantics: ["python", "zustellsemantik_ibmmq_producer_resilient.py"],
        "consumer_cmd": lambda run_id, semantics: ["python", "zustellsemantik_ibmmq_consumer.py", run_id, semantics],
    },
}


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in TECH_CONFIG or sys.argv[2] not in VALID_SEMANTICS:
        print(
            f"Nutzung: python run_fault_injection_broker_restart.py "
            f"<{'|'.join(TECH_CONFIG)}> <{'|'.join(VALID_SEMANTICS)}>"
        )
        sys.exit(1)

    technology, semantics = sys.argv[1], sys.argv[2]
    config = TECH_CONFIG[technology]
    run_id = f"restarttest-{technology}-{semantics}-{int(time.time())}"
    print(f"run_id fuer diesen Lauf: {run_id}")

    consumer_proc = subprocess.Popen(config["consumer_cmd"](run_id, semantics))
    time.sleep(2.0)

    producer_proc = subprocess.Popen(config["producer_cmd"](semantics))

    time.sleep(0.5)
    print(f"Starte Container '{config['container']}' neu ...")
    subprocess.run(["podman", "restart", config["container"]], check=True)
    print(f"Container '{config['container']}' neu gestartet, Producer/Consumer versuchen sich wiederzuverbinden.")

    producer_proc.wait()
    print("Producer fertig, warte auf Consumer-Abschluss (Idle-Timeout)...")
    consumer_proc.wait()

    print(f"Fehlerinjektion abgeschlossen. run_id: {run_id}")
    print(f"Auswertung: python analyze_semantics.py results_semantics.jsonl {run_id} 200")


if __name__ == "__main__":
    main()
