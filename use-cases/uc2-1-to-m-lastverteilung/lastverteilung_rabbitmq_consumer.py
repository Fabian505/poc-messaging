"""Use Case 4.2.2.1 (Lastverteilung) - Consumer (RabbitMQ-Variante).

Wird vom Orchestrator run_uc2_measurement.py gestartet, Parameter siehe
uc2_common.py. Mehrere Consumer an derselben Queue teilen sich die Last
automatisch (round-robin, begrenzt durch prefetch_count).

at-least-once: manuelles Ack nach der Verarbeitung (identisch zu UC1).
"""

import time

import pika

import uc2_common as common

QUEUE_NAME = "loadbalance.test"
PREFETCH_COUNT = 50


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    print(f"[Instanz {common.INSTANCE_ID}] Bereit", flush=True)
    # connection.sleep statt time.sleep: verarbeitet Heartbeats weiter
    common.wait_for_start(idle_fn=lambda: connection.sleep(0.02))

    processed = 0
    first_at = last_at = None
    try:
        for method, _props, body in channel.consume(
            QUEUE_NAME, inactivity_timeout=common.IDLE_TIMEOUT_SECONDS
        ):
            if method is None:
                break
            if first_at is None:
                first_at = time.time()
            common.simulate_processing(body)
            channel.basic_ack(delivery_tag=method.delivery_tag)  # at-least-once
            processed += 1
            last_at = time.time()
    finally:
        channel.cancel()
        connection.close()

    common.write_result("rabbitmq", processed, first_at, last_at)


if __name__ == "__main__":
    main()
