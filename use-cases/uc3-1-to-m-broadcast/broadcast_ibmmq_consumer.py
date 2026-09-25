"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Subscriber (IBM MQ).

Jeder Subscriber legt eine EIGENE, DAUERHAFTE (durable), von MQ verwaltete
(managed) Subscription an. Nicht mehr non-durable wie in der ersten
Fassung: Eine non-durable Subscription existiert nur, solange der
Subscriber verbunden ist; Veroeffentlichungen waehrend eines Neustarts
gingen verloren, das widerspricht at-least-once. Durable Subscriptions
nutzen eine permanente dynamische Queue, die persistente Nachrichten
sicher speichert.

Die Subscription wird am Laufende mit MQCO_REMOVE_SUB entfernt (und vom
Orchestrator zur Sicherheit nochmals), sonst sammelt sie weiter Kopien.

at-least-once: MQGMO_SYNCPOINT plus qmgr.commit() nach der Verarbeitung.
"""

import pymqi

import uc3_common as common

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
CONN_INFO = "localhost(1414)"
USER = "app"
PASSWORD = "app12345"
TOPIC_STRING = "dev/broadcast"
SUB_NAME = f"uc3-{common.RUN_ID}-{common.INSTANCE_ID}"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, CONN_INFO, USER, PASSWORD)

    sd = pymqi.SD()
    sd.Options = (
        pymqi.CMQC.MQSO_CREATE
        | pymqi.CMQC.MQSO_DURABLE
        | pymqi.CMQC.MQSO_MANAGED
        | pymqi.CMQC.MQSO_FAIL_IF_QUIESCING
    )
    sd.set_vs("SubName", SUB_NAME)
    sd.set_vs("ObjectString", TOPIC_STRING)
    subscription = pymqi.Subscription(qmgr)
    subscription.sub(sub_desc=sd)
    sub_queue = subscription.sub_queue

    gmo = pymqi.GMO(
        Options=(
            pymqi.CMQC.MQGMO_WAIT
            | pymqi.CMQC.MQGMO_SYNCPOINT
            | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        ),
        WaitInterval=500,
    )
    print(f"[Subscriber {common.INSTANCE_ID}] Bereit", flush=True)

    rec = common.Recorder()
    try:
        while not rec.complete:
            try:
                message = sub_queue.get(None, pymqi.MD(), gmo)
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    if rec.timed_out():
                        break
                    continue
                raise
            rec.record(message)
            qmgr.commit()
    finally:
        subscription.close(sub_close_options=pymqi.CMQC.MQCO_REMOVE_SUB,
                           close_sub_queue=True)
        qmgr.disconnect()
    rec.write("ibmmq")


if __name__ == "__main__":
    main()
