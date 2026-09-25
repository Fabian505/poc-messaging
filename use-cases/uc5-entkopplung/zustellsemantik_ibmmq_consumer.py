"""UC5 - Consumer (IBM MQ).
at-most-once: get() ohne Syncpoint (Nachricht beim Abholen endgueltig weg).
at-least-once/exactly-once: MQGMO_SYNCPOINT, commit() nach der Nacharbeit."""

import pymqi

import uc5_common as c

QUEUE_MANAGER, CHANNEL, CONN_INFO = "QM1", "DEV.APP.SVRCONN", "localhost(1414)"
QUEUE_NAME, USER, PASSWORD = "DEV.QUEUE.2", "app", "app12345"


def main():
    c.install_sigterm()
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, CONN_INFO, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)
    options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
    if c.SEMANTICS != "at-most-once":
        options |= pymqi.CMQC.MQGMO_SYNCPOINT
    gmo = pymqi.GMO(Options=options, WaitInterval=500)
    store = c.EffectStore()
    c.ready()
    ack = None if c.SEMANTICS == "at-most-once" else qmgr.commit

    try:
        while not c.STOP["flag"]:
            try:
                message = queue.get(None, pymqi.MD(), gmo)
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    continue
                raise
            c.process(store, message, ack)
    finally:
        try:
            queue.close()
            qmgr.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
