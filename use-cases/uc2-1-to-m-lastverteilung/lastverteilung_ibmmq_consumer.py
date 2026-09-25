"""Use Case 4.2.2.1 (Lastverteilung) - Consumer (IBM MQ-Variante).

Wird vom Orchestrator run_uc2_measurement.py gestartet, Parameter siehe
uc2_common.py. Mehrere Consumer an derselben Queue teilen sich die Last
automatisch (jeder get() holt die naechste verfuegbare Nachricht).

at-least-once: MQGMO_SYNCPOINT plus qmgr.commit() nach der Verarbeitung
(identisch zu UC1).
"""

import time

import pymqi

import uc2_common as common

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
CONN_INFO = "localhost(1414)"
QUEUE_NAME = "DEV.QUEUE.2"
USER = "app"
PASSWORD = "app12345"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, CONN_INFO, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)
    gmo = pymqi.GMO(
        Options=(
            pymqi.CMQC.MQGMO_WAIT
            | pymqi.CMQC.MQGMO_SYNCPOINT
            | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        ),
        WaitInterval=int(common.IDLE_TIMEOUT_SECONDS * 1000),
    )

    print(f"[Instanz {common.INSTANCE_ID}] Bereit", flush=True)
    common.wait_for_start()

    processed = 0
    first_at = last_at = None
    try:
        while True:
            try:
                message = queue.get(None, pymqi.MD(), gmo)
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    break
                raise
            if first_at is None:
                first_at = time.time()
            common.simulate_processing(message)
            qmgr.commit()  # at-least-once
            processed += 1
            last_at = time.time()
    finally:
        queue.close()
        qmgr.disconnect()

    common.write_result("ibmmq", processed, first_at, last_at)


if __name__ == "__main__":
    main()
