"""Isolierter Test: RabbitMQ mit gruppierten Publisher Confirms statt einem
Confirm pro Nachricht. NICHT Teil der offiziellen Messreihe -- eigenstaendig
ausfuehren, Ergebnis nur als Zusatzbefund fuer Kapitel 7 verwenden.

Env: MEASURE_COUNT (Default 2000), WINDOW (Default 50, Batchgroesse pro Commit)
"""
import json
import os
import time
from datetime import datetime, timezone

import pika

QUEUE_NAME = "pipeline_test"
MEASURE_COUNT = int(os.environ.get("MEASURE_COUNT", 2000))
WINDOW = int(os.environ.get("WINDOW", 50))


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_purge(QUEUE_NAME)
    channel.confirm_delivery()

    print(f"Start: {MEASURE_COUNT} Nachrichten, Fenstergroesse {WINDOW}")
    t0 = time.perf_counter()
    for seq in range(1, MEASURE_COUNT + 1):
        channel.basic_publish(
            exchange="", routing_key=QUEUE_NAME,
            body=json.dumps({"seq": seq, "sent_at": datetime.now(timezone.utc).isoformat()}),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        if seq % WINDOW == 0:
            channel.wait_for_confirms()  # EIN Confirm-Wait fuer WINDOW Nachrichten
    channel.wait_for_confirms()  # Rest des letzten, unvollstaendigen Fensters
    elapsed = time.perf_counter() - t0
    connection.close()

    print(f"Fertig in {elapsed:.2f}s -> {MEASURE_COUNT / elapsed:.1f} Nachrichten/s "
          f"(zum Vergleich: CAP_RATE ohne Pipelining = 300/s)")


if __name__ == "__main__":
    main()
