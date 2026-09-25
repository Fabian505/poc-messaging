"""UC5 - Producer (IBM MQ). Persistent (MD an put() uebergeben!),
Wiederverbindung bei MQMIError (pymqi hat keine eingebaute)."""

import pymqi

import uc5_common as c

QUEUE_MANAGER, CHANNEL, CONN_INFO = "QM1", "DEV.APP.SVRCONN", "localhost(1414)"
QUEUE_NAME, USER, PASSWORD = "DEV.QUEUE.2", "app", "app12345"
state = {}


def connect():
    for key in ("queue", "qmgr"):
        try:
            state[key].close() if key == "queue" else state[key].disconnect()
        except Exception:
            pass
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, CONN_INFO, USER, PASSWORD)
    state.update(qmgr=qmgr, queue=pymqi.Queue(qmgr, QUEUE_NAME))


def main():
    connect()
    pmo = pymqi.PMO(Options=pymqi.CMQC.MQPMO_NEW_MSG_ID | pymqi.CMQC.MQPMO_FAIL_IF_QUIESCING)

    def send(body):
        md = pymqi.MD()
        md.Persistence = pymqi.CMQC.MQPER_PERSISTENT
        state["queue"].put(body.encode(), md, pmo)

    c.run_producer(send, reconnect_fn=connect)
    state["queue"].close()
    state["qmgr"].disconnect()


if __name__ == "__main__":
    main()
