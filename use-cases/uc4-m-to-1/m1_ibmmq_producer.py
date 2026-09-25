"""Use Case 4.2.3 (m:1) - Producer (IBM MQ). Persistent (MQPER_PERSISTENT
explizit), getaktet nach m1_common."""

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
    pmo = pymqi.PMO(Options=pymqi.CMQC.MQPMO_NEW_MSG_ID | pymqi.CMQC.MQPMO_FAIL_IF_QUIESCING)

    def send(body):
        md = pymqi.MD()
        md.Persistence = pymqi.CMQC.MQPER_PERSISTENT
        queue.put(body.encode(), md, pmo)

    common.run_paced_producer(send)
    queue.close()
    qmgr.disconnect()


if __name__ == "__main__":
    main()
