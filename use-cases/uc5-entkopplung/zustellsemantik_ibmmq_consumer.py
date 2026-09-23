"""Use Case 4.2.4 (Entkopplung bei Ausfall und Zustellsemantiken)
- Consumer (IBM MQ-Variante), konfigurierbar fuer alle drei Zustellsemantiken.

Nutzung:
    python zustellsemantik_ibmmq_consumer.py <run_id> <semantik>

<semantik> ist eine von: at-most-once | at-least-once | exactly-once

- at-most-once: get() OHNE MQGMO_SYNCPOINT, die Nachricht wird sofort und
  endgueltig aus der Queue entfernt, noch bevor sie verarbeitet wurde. Das
  entspricht genau eurem urspruenglichen ibmmq_consumer.py aus Kapitel
  6.1.1, dort war das (unbeabsichtigt) bereits die verwendete Semantik.
- at-least-once: MQGMO_SYNCPOINT, Commit ERST NACH Verarbeitung.
- exactly-once: wie at-least-once, zusaetzlich Deduplizierung auf
  Anwendungsebene ueber ein Set bereits verarbeiteter seq-Werte, da auch
  IBM MQs Syncpoint-Mechanismus allein keine echte exactly-once-Garantie
  auf Anwendungsebene liefert (nur, dass die Nachricht nicht aus der Queue
  verschwindet, bevor sie committed wurde).
"""

import json
import sys
from datetime import datetime, timezone

import pymqi

QUEUE_MANAGER = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
HOST = "localhost"
PORT = "1414"
QUEUE_NAME = "DEV.QUEUE.2"
USER = "app"
PASSWORD = "app12345"
RESULTS_FILE = "results_semantics.jsonl"
IDLE_TIMEOUT_MS = 10_000
VALID_SEMANTICS = {"at-most-once", "at-least-once", "exactly-once"}

conn_info = f"{HOST}({PORT})"


def main():
    if len(sys.argv) != 3 or sys.argv[2] not in VALID_SEMANTICS:
        print(f"Nutzung: python zustellsemantik_ibmmq_consumer.py <run_id> <{'|'.join(VALID_SEMANTICS)}>")
        sys.exit(1)

    run_id, semantics = sys.argv[1], sys.argv[2]
    use_syncpoint = semantics != "at-most-once"

    qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, conn_info, USER, PASSWORD)
    queue = pymqi.Queue(qmgr, QUEUE_NAME)

    options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
    if use_syncpoint:
        options |= pymqi.CMQC.MQGMO_SYNCPOINT
    gmo = pymqi.GMO(Options=options, WaitInterval=IDLE_TIMEOUT_MS)

    seen_seqs = set()  # nur fuer exactly-once relevant

    print(f"[{semantics}] Bereit, run_id '{run_id}'. Warte auf Nachrichten.")
    with open(RESULTS_FILE, "a") as f:
        try:
            while True:
                try:
                    message = queue.get(None, pymqi.md(), gmo)
                except pymqi.MQMIError as e:
                    if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        break
                    raise

                payload = json.loads(message)
                seq = payload["seq"]

                if semantics == "exactly-once" and seq in seen_seqs:
                    f.write(json.dumps({
                        "technology": "ibmmq", "run_id": run_id,
                        "semantics": semantics, "seq": seq,
                        "received_at": datetime.now(timezone.utc).isoformat(),
                        "duplicate_skipped": True,
                    }) + "\n")
                    if use_syncpoint:
                        qmgr.commit()
                    continue

                seen_seqs.add(seq)

                f.write(json.dumps({
                    "technology": "ibmmq", "run_id": run_id,
                    "semantics": semantics, "seq": seq,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "duplicate_skipped": False,
                }) + "\n")
                f.flush()

                if use_syncpoint:
                    # at-least-once und exactly-once: Commit NACH Verarbeitung
                    qmgr.commit()
        except KeyboardInterrupt:
            pass
        finally:
            queue.close()
            qmgr.disconnect()


if __name__ == "__main__":
    main()
