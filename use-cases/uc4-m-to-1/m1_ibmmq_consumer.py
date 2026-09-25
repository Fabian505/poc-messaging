"""Use Case 4.2.3 (m:1) - Consumer (IBM MQ). Eine Instanz, MQGMO_SYNCPOINT
plus qmgr.commit() nach der Verarbeitung."""

import pymqi

import m1_common as common

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
        WaitInterval=500,
    )
    print("[Consumer] Bereit", flush=True)

    rec = common.Recorder()
    try:
        while not rec.complete:
            try:
                message = queue.get(None, pymqi.MD(), gmo)
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    if rec.timed_out():
                        break
                    continue
                raise
            rec.record(message)
            qmgr.commit()
    finally:
        queue.close()
        qmgr.disconnect()
    rec.write("ibmmq")


if __name__ == "__main__":
    main()
