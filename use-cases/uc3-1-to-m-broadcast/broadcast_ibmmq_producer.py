"""Use Case 4.2.2.2 (Event-basierte Benachrichtigung) - Producer (IBM MQ).

Publiziert auf den Topic-String dev/broadcast, persistent
(MQPER_PERSISTENT explizit, nicht die Voreinstellung des Topics),
fahrplanbasiert getaktet wie UC1.
"""

import pymqi

from uc3_pacing import run_paced

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
CONN_INFO = "localhost(1414)"
USER = "app"
PASSWORD = "app12345"
TOPIC_STRING = "dev/broadcast"


def main():
    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, CONN_INFO, USER, PASSWORD)
    topic = pymqi.Topic(qmgr, topic_string=TOPIC_STRING)
    topic.open(open_opts=pymqi.CMQC.MQOO_OUTPUT)
    pmo = pymqi.PMO(Options=pymqi.CMQC.MQPMO_NEW_MSG_ID | pymqi.CMQC.MQPMO_FAIL_IF_QUIESCING)

    def send(body):
        md = pymqi.MD()
        md.Persistence = pymqi.CMQC.MQPER_PERSISTENT
        topic.pub(body.encode(), md, pmo)

    run_paced(send)
    topic.close()
    qmgr.disconnect()


if __name__ == "__main__":
    main()
