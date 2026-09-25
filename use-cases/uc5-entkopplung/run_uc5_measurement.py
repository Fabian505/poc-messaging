"""UC5 Entkopplung bei Ausfall und Zustellsemantiken - Orchestrator.

Szenarien:
  crash-pre       Consumer stuerzt bei Nachricht N/3 VOR dem Seiteneffekt ab
  crash-post      Consumer stuerzt bei Nachricht N/3 NACH dem Seiteneffekt,
                  vor der Bestaetigung ab
                  (beide: harter Absturz per SIGKILL durch den Consumer
                  selbst, Neustart nach AUSFALL_SEKUNDEN; Kapitel 4:
                  Consumer-Ausfall. Deterministisch, siehe uc5_common.py)
  broker-pause    Broker-Container wird eingefroren (podman pause) und
                  wieder freigegeben (Ersatz fuer eine Netzwerkunterbrechung)
  broker-restart  Broker-Container wird neu gestartet (podman restart)

Ausloesung Broker-Szenarien: nach 1/3 der bestaetigten Sendungen. Der
Zustand des Consumers in diesem Moment ist dabei nicht festgelegt, diese
Szenarien pruefen die Entkopplung auf Broker-Ebene, nicht einzelne
Verarbeitungsphasen.

Ein zufaelliger Modus (mehrere Abstuerze zu zufaelligen Zeitpunkten, Chaos-
Engineering) ist bewusst NICHT enthalten, Abstimmung mit Betreuer offen.

Supervisor: Beendet sich der Consumer unerwartet (z.B. Verbindungsabbruch
beim Broker-Neustart), startet ihn der Orchestrator neu, wie es ein
Betriebssystem-Dienst oder Kubernetes tun wuerde.

Ende eines Laufs: at-least-once/exactly-once, sobald fuer alle Nachrichten
ein Seiteneffekt existiert; at-most-once (Verlust erwartet) nach
LEERLAUF_SEKUNDEN ohne Zustellung bei bereitem Consumer. Dann SIGTERM.

Nutzung:
    python run_uc5_measurement.py <kafka|rabbitmq|ibmmq|all> <crash-pre|crash-post|broker-pause|broker-restart|all> <at-most-once|at-least-once|exactly-once|all> <wiederholungen>

Beispiel:
    python run_uc5_measurement.py all all all 5
"""

import json
import os
import queue
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
def script_path(name):
    return os.path.join(SCRIPT_DIR, name)

TECHNOLOGIES = ["kafka", "rabbitmq", "ibmmq"]
SCENARIOS = ["crash-pre", "crash-post", "broker-pause", "broker-restart"]
SEMANTICS = ["at-most-once", "at-least-once", "exactly-once"]
KAFKA_BOOTSTRAP = "localhost:9092"

MESSAGE_COUNT = int(os.environ.get("MESSAGE_COUNT", 200))
PRE_MS = float(os.environ.get("PRE_MS", 10))
POST_MS = float(os.environ.get("POST_MS", 10))
AUSFALL_SEKUNDEN = 3.0          # Ausfallzeit des Consumers bei crash-pre/crash-post
PAUSE_SEKUNDEN = 5.0            # Dauer der Broker-Pause
LEERLAUF_SEKUNDEN = 10.0        # Ende bei at-most-once (Verlust erwartet)
STILLSTAND_SEKUNDEN = 90.0      # at-least-once/exactly-once: so lange ohne Zustellung
                                # bei ausstehenden Nachrichten = Consumer-Stillstand
                                # (erste Fassung brach schon nach 10 s ab: falscher Verlust,
                                # als Kafka nach Broker-Neustart laenger zum Wiederanlauf brauchte)
RUN_TIMEOUT_SECONDS = 400
SUPERVISOR_RESTART_DELAY = 2.0
RESULTS_JSONL = "results_uc5.jsonl"

SCRIPTS = {t: (script_path(f"zustellsemantik_{t}_producer.py"), script_path(f"zustellsemantik_{t}_consumer.py"))
           for t in TECHNOLOGIES}


# ---------------------------------------------------------------- Infrastruktur

def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def broker_ready(tech):
    if tech == "kafka":
        return sh(["podman", "exec", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                   "--bootstrap-server", "localhost:9092", "--list"]).returncode == 0
    if tech == "rabbitmq":
        return sh(["podman", "exec", "rabbitmq", "rabbitmq-diagnostics", "-q",
                   "check_running"]).returncode == 0
    return "STATUS(Running)" in sh(["podman", "exec", "ibmmq", "dspmq", "-m", "QM1"]).stdout


def wait_broker_ready(tech, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if broker_ready(tech):
            return time.time()
        time.sleep(1.0)
    raise RuntimeError(f"{tech} nach {timeout}s nicht bereit")


def kafka_admin():
    from confluent_kafka.admin import AdminClient
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


def prepare(tech, run_id, env):
    if tech == "kafka":
        from confluent_kafka.admin import NewTopic
        topic = f"semantics.{run_id}"
        admin = kafka_admin()
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(timeout=30)
        for _ in range(100):
            t = admin.list_topics(topic=topic, timeout=5).topics.get(topic)
            if t is not None and t.error is None and t.partitions:
                break
            time.sleep(0.2)
        env["TOPIC"] = topic
    elif tech == "rabbitmq":
        env["QUEUE_NAME"] = f"semantics.{run_id}"
    else:
        sh(["podman", "exec", "ibmmq", "bash", "-c", "echo 'CLEAR QLOCAL(DEV.QUEUE.2)' | runmqsc QM1"])
        # WICHTIG: DEV.QUEUE.2 wird von UC1/UC2/UC4/UC5 gemeinsam genutzt.
        # check=False in sh() allein reicht nicht, ein fehlgeschlagenes CLEAR
        # (haengendes Handle eines abgestuerzten Prozesses) blieb sonst
        # unbemerkt und Nachrichten sammelten sich ueber viele Laeufe hinweg
        # an (beobachtet: Queue lief bis MAXDEPTH voll). Explizit verifizieren.
        out = sh(["podman", "exec", "ibmmq", "bash", "-c",
                  "echo 'DIS QL(DEV.QUEUE.2) CURDEPTH' | runmqsc QM1"]).stdout
        m = re.search(r"CURDEPTH\((\d+)\)", out)
        if not m or int(m.group(1)) != 0:
            raise RuntimeError(
                f"ibmmq: DEV.QUEUE.2 nach CLEAR nicht leer (Tiefe: {m.group(1) if m else 'unbekannt'}). "
                "Vermutlich ein haengendes Handle von einem abgestuerzten Prozess. "
                "'podman restart ibmmq' und erneut versuchen."
            )
        # Zusaetzlich: MAXDEPTH selbst kann unabhaengig von der Tiefe
        # zurueckfallen (beobachtet 24.09.2026, Ursache ungeklaert - auch
        # WAEHREND einer laufenden Isolation moeglich). Eine leere Queue mit
        # zu niedrigem MAXDEPTH besteht die obige Pruefung trotzdem.
        maxdepth_out = sh(["podman", "exec", "ibmmq", "bash", "-c",
                           "echo 'DIS QL(DEV.QUEUE.2) MAXDEPTH' | runmqsc QM1"]).stdout
        mm = re.search(r"MAXDEPTH\((\d+)\)", maxdepth_out)
        maxdepth = int(mm.group(1)) if mm else None
        if maxdepth is None or maxdepth < MESSAGE_COUNT:
            raise RuntimeError(
                f"ibmmq: MAXDEPTH({maxdepth}) reicht nicht fuer {MESSAGE_COUNT} "
                f"Nachrichten. 'ALTER QLOCAL(DEV.QUEUE.2) MAXDEPTH(1200000)' erneut setzen."
            )


def cleanup(tech, env):
    try:
        if tech == "kafka":
            admin = kafka_admin()  # Referenz halten, sonst wird der Handle vor dem Ergebnis zerstoert (_DESTROY)
            admin.delete_topics([env["TOPIC"]])[env["TOPIC"]].result(timeout=30)
        elif tech == "rabbitmq":
            sh(["podman", "exec", "rabbitmq", "rabbitmqctl", "delete_queue", env["QUEUE_NAME"]])
        else:
            sh(["podman", "exec", "ibmmq", "bash", "-c", "echo 'CLEAR QLOCAL(DEV.QUEUE.2)' | runmqsc QM1"])
    except Exception as e:
        print(f"  Hinweis beim Aufraeumen: {e}")


# ---------------------------------------------------------------- Prozesse

class Proc:
    def __init__(self, script, env):
        self.p = subprocess.Popen([sys.executable, "-u", script], env=env,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, bufsize=1)
        self.q = queue.Queue()
        self.log = []
        self.ready = False
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in iter(self.p.stdout.readline, ""):
            self.q.put(line)
        self.q.put(None)

    def lines(self):
        out = []
        while True:
            try:
                line = self.q.get_nowait()
            except queue.Empty:
                return out
            if line is None:
                return out
            self.log.append(line)
            out.append(line)

    def alive(self):
        return self.p.poll() is None


def effects_complete(db_path):
    if not os.path.exists(db_path):
        return False
    try:
        con = sqlite3.connect(db_path, timeout=5)
        n = con.execute("SELECT COUNT(DISTINCT seq) FROM effects").fetchone()[0]
        con.close()
        return n >= MESSAGE_COUNT
    except sqlite3.Error:
        return False


# ---------------------------------------------------------------- Lauf

def run_single(tech, scenario, semantics, rep):
    producer_script, consumer_script = SCRIPTS[tech]
    run_id = f"{tech}-{scenario}-{semantics}-r{rep}-{int(time.time())}"
    workdir = tempfile.mkdtemp(prefix=f"uc5_{run_id}_")
    db_path = os.path.join(workdir, "store.sqlite")
    env = os.environ.copy()
    env.update({"PYTHONUNBUFFERED": "1", "RUN_ID": run_id, "RESULT_DIR": workdir,
                "SEMANTICS": semantics, "MESSAGE_COUNT": str(MESSAGE_COUNT)})

    wait_broker_ready(tech)
    prepare(tech, run_id, env)

    events = []
    consumer_starts = 0
    deliveries = 0
    confirmed = 0
    fault_done = False
    fault_window = None      # (Beginn, Ende) des Ausfalls
    restart_consumer_at = None
    crash_scenario = scenario.startswith("crash-")
    stalled = False
    first_delivery_after_fault = None
    last_activity = time.time()
    producer = consumer = None

    def start_consumer():
        nonlocal consumer, consumer_starts
        cenv = dict(env)
        if crash_scenario and consumer_starts == 0:
            # nur die erste Instanz stuerzt ab
            cenv.update(CRASH_AT_SEQ=str(MESSAGE_COUNT // 3),
                        CRASH_PHASE=scenario.split("-", 1)[1])
        consumer = Proc(consumer_script, cenv)
        consumer_starts += 1

    try:
        start_consumer()
        t0 = time.time()
        while not consumer.ready:
            for line in consumer.lines():
                if line.startswith("Bereit"):
                    consumer.ready = True
            if not consumer.alive() or time.time() - t0 > 120:
                raise RuntimeError(f"Consumer nicht bereit: {''.join(consumer.log)[-500:]}")
            time.sleep(0.05)

        producer = Proc(producer_script, env)
        t_start = time.time()

        while True:
            now = time.time()
            if now - t_start > RUN_TIMEOUT_SECONDS:
                raise TimeoutError("Laufzeitgrenze erreicht")

            confirmed += sum(1 for l in producer.lines() if l.startswith("S "))
            if consumer is not None:
                for line in consumer.lines():
                    if line.startswith("D "):
                        deliveries += 1
                        last_activity = now
                        if (fault_window and fault_window[1] is not None
                                and first_delivery_after_fault is None and now >= fault_window[1]):
                            first_delivery_after_fault = now
                    elif line.startswith("Bereit"):
                        consumer.ready = True
                        last_activity = now
                        if fault_window and fault_window[1] is None and crash_scenario:
                            fault_window = (fault_window[0], now)

            # --- Fehlerinjektion
            if not fault_done:
                if crash_scenario and consumer is not None and not consumer.alive():
                    crash_lines = [l.strip() for l in consumer.lines() + consumer.log
                                   if l.startswith("CRASH")]
                    # Nur die erste Instanz kann sich per SIGKILL selbst beenden;
                    # die CRASH-Zeile kann dem Prozessende hinterherlaufen.
                    if consumer.p.returncode == -signal.SIGKILL:
                        consumer = None
                        fault_window = (now, None)
                        restart_consumer_at = now + AUSFALL_SEKUNDEN
                        detail = crash_lines[0][6:] if crash_lines else \
                            f"seq={MESSAGE_COUNT // 3} phase={scenario.split('-', 1)[1]}"
                        events.append(f"Absturz {detail}")
                        fault_done = True
                elif scenario in ("broker-pause", "broker-restart") and confirmed >= MESSAGE_COUNT // 3:
                    fault_done = True
                    begin = time.time()
                    if scenario == "broker-pause":
                        sh(["podman", "pause", tech])
                        time.sleep(PAUSE_SEKUNDEN)
                        sh(["podman", "unpause", tech])
                        end = time.time()
                    else:
                        sh(["podman", "restart", tech])
                        end = wait_broker_ready(tech)
                    fault_window = (begin, end)
                    last_activity = end
                    events.append(f"{scenario} nach {confirmed} Bestaetigungen, "
                                  f"Ausfall {end - begin:.1f}s")

            # --- Supervisor: Consumer (neu) starten
            if consumer is None and restart_consumer_at and now >= restart_consumer_at:
                restart_consumer_at = None
                start_consumer()
            elif consumer is not None and not consumer.alive():
                consumer.lines()
                events.append(f"Consumer beendet (Exit {consumer.p.returncode}), Neustart")
                consumer = None
                restart_consumer_at = now + SUPERVISOR_RESTART_DELAY

            # --- Ende
            producer_done = not producer.alive()
            if producer_done and producer.p.returncode != 0:
                raise RuntimeError(f"Producer Exit {producer.p.returncode}: {''.join(producer.log)[-500:]}")
            if producer_done and consumer is not None and consumer.ready:
                if semantics != "at-most-once" and effects_complete(db_path):
                    break
                idle_limit = LEERLAUF_SEKUNDEN if semantics == "at-most-once" else STILLSTAND_SEKUNDEN
                if now - last_activity > idle_limit:
                    if semantics != "at-most-once":
                        stalled = True
                        events.append(f"Abbruch: {idle_limit:.0f}s keine Zustellung (Consumer-Stillstand)")
                    break
            time.sleep(0.02)

        consumer.p.send_signal(signal.SIGTERM)
        try:
            consumer.p.wait(timeout=15)
        except subprocess.TimeoutExpired:
            consumer.p.kill()
        with open(os.path.join(workdir, "producer.json")) as f:
            prod = json.load(f)
        recovery = (first_delivery_after_fault - fault_window[1]) \
            if first_delivery_after_fault and fault_window and fault_window[1] else None
        result = evaluate(tech, scenario, semantics, run_id, db_path, prod,
                          consumer_starts, fault_window, events, stalled, recovery)
    finally:
        for proc in (producer, consumer):
            if proc is not None and proc.alive():
                proc.p.kill()
        sh(["podman", "unpause", tech])  # falls ein Abbruch waehrend der Pause kam
        cleanup(tech, env)
        shutil.rmtree(workdir, ignore_errors=True)
    return result


# ---------------------------------------------------------------- Auswertung

def evaluate(tech, scenario, semantics, run_id, db_path, prod, consumer_starts, fault_window,
             events, stalled=False, recovery=None):
    con = sqlite3.connect(db_path)
    q = lambda sql: con.execute(sql).fetchone()[0]
    effects = q("SELECT COUNT(*) FROM effects")
    effects_unique = q("SELECT COUNT(DISTINCT seq) FROM effects")
    skipped = q("SELECT COUNT(*) FROM skipped")
    delivered = q("SELECT COUNT(*) FROM deliveries")
    delivered_unique = q("SELECT COUNT(DISTINCT seq) FROM deliveries")
    effect_seqs = {r[0] for r in con.execute("SELECT DISTINCT seq FROM effects")}
    con.close()

    missing = MESSAGE_COUNT - effects_unique
    duplicates = effects - effects_unique
    ok = None if stalled else {
        "at-most-once": duplicates == 0,
        "at-least-once": missing == 0,
        "exactly-once": missing == 0 and duplicates == 0,
    }[semantics]

    # Entkopplung: waehrend des Ausfalls bestaetigte Nachrichten und wie viele davon verarbeitet wurden
    during = processed_during = None
    if fault_window and fault_window[1]:
        seqs = [int(s) for s, ts in prod["confirmed"].items()
                if fault_window[0] <= ts <= fault_window[1]]
        during = len(seqs)
        processed_during = sum(1 for s in seqs if s in effect_seqs)

    return {
        "technology": tech, "scenario": scenario, "semantics": semantics, "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message_count": MESSAGE_COUNT, "pre_ms": PRE_MS, "post_ms": POST_MS,
        "missing": missing, "duplicates_processed": duplicates, "duplicates_skipped": skipped,
        "redeliveries": delivered - delivered_unique,
        "never_delivered": MESSAGE_COUNT - delivered_unique,
        "producer_resends": prod["resends"], "consumer_starts": consumer_starts,
        "outage_seconds": (fault_window[1] - fault_window[0]) if fault_window and fault_window[1] else None,
        "recovery_seconds": recovery,
        "stalled": stalled,
        "sent_during_outage": during, "processed_of_those": processed_during,
        "semantics_ok": ok, "events": events,
    }


def fmt(v, d=1):
    return "-" if v is None else (f"{v:.{d}f}" if isinstance(v, float) else str(v))


def write_report(results, repetitions):
    path = f"uc5_bericht_{int(time.time())}.md"
    with open(path, "w") as f:
        f.write("# UC5 Entkopplung bei Ausfall und Zustellsemantiken - Messbericht\n\n")
        f.write(f"Erstellt: {datetime.now(timezone.utc).isoformat()}\n\n")
        f.write(f"Nachrichten pro Lauf: {MESSAGE_COUNT}, Wiederholungen: {repetitions}, "
                f"Verarbeitung: {PRE_MS} ms vor + {POST_MS} ms nach dem Seiteneffekt\n\n")
        f.write("## Zusammenfassung\n\n")
        f.write("| Technologie | Szenario | Semantik | Laeufe | Semantik eingehalten | Verlust (Summe) "
                "| Duplikate verarbeitet | Duplikate erkannt | Erneute Zustellungen | Producer-Wiederholungen "
                "| Mittl. Ausfall (s) | Mittl. Wiederanlauf (s) | Stillstaende | Waehrend Ausfall gesendet / verarbeitet |\n")
        f.write("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
        for (tech, scen, sem), rs in results.items():
            if not rs:
                f.write(f"| {tech} | {scen} | {sem} | 0 | - | - | - | - | - | - | - | - | - | - |\n")
                continue
            s = lambda k: sum(r[k] for r in rs)
            outages = [r["outage_seconds"] for r in rs if r["outage_seconds"] is not None]
            recs = [r["recovery_seconds"] for r in rs if r.get("recovery_seconds") is not None]
            during = [r for r in rs if r["sent_during_outage"] is not None]
            dur = (f"{sum(r['sent_during_outage'] for r in during)} / "
                   f"{sum(r['processed_of_those'] for r in during)}") if during else "-"
            f.write(f"| {tech} | {scen} | {sem} | {len(rs)} | {sum(bool(r['semantics_ok']) for r in rs)}/{sum(r['semantics_ok'] is not None for r in rs)} | "
                    f"{s('missing')} | {s('duplicates_processed')} | {s('duplicates_skipped')} | "
                    f"{s('redeliveries')} | {s('producer_resends')} | "
                    f"{fmt(sum(outages) / len(outages)) if outages else '-'} | "
                    f"{fmt(sum(recs) / len(recs)) if recs else '-'} | {sum(r.get('stalled', False) for r in rs)} | {dur} |\n")

        f.write("\n## Einzellaeufe\n\n")
        f.write("| Technologie | Szenario | Semantik | Verlust | Dupl. verarbeitet | Dupl. erkannt "
                "| Erneut zugestellt | Nie zugestellt | Consumer-Starts | Ereignisse |\n")
        f.write("|---|---|---|---|---|---|---|---|---|---|\n")
        for (tech, scen, sem), rs in results.items():
            for r in rs:
                f.write(f"| {tech} | {scen} | {sem} | {r['missing']} | {r['duplicates_processed']} | "
                        f"{r['duplicates_skipped']} | {r['redeliveries']} | {r['never_delivered']} | "
                        f"{r['consumer_starts']} | {'; '.join(r['events'])} |\n")

        f.write("\nSemantik eingehalten: at-most-once = keine verarbeiteten Duplikate (Verlust erlaubt), "
                "at-least-once = kein Verlust (Duplikate erlaubt), exactly-once = weder Verlust noch "
                "verarbeitete Duplikate. Verlust = Nachrichten ohne ausgefuehrten Seiteneffekt. "
                "Wiederanlauf = Zeit vom Ende des Ausfalls bis zur ersten erneuten Zustellung. "
                "Stillstand = Lauf abgebrochen, weil der Consumer trotz ausstehender Nachrichten "
                f"{STILLSTAND_SEKUNDEN:.0f}s nichts zugestellt bekam (kein Semantikbefund, sondern Verfuegbarkeitsbefund). "
                "Nie zugestellt = Nachrichten, die den Consumer gar nicht erreicht haben (z.B. im "
                "Client-Puffer verloren).\n")
    print(f"\nBericht geschrieben: {path}")


def main():
    args = sys.argv[1:]
    if (len(args) != 4 or args[0] not in TECHNOLOGIES + ["all"]
            or args[1] not in SCENARIOS + ["all"] or args[2] not in SEMANTICS + ["all"]):
        print("Nutzung: python run_uc5_measurement.py <kafka|rabbitmq|ibmmq|all> "
              "<crash-pre|crash-post|broker-pause|broker-restart|all> "
              "<at-most-once|at-least-once|exactly-once|all> <wiederholungen>")
        sys.exit(1)
    techs = TECHNOLOGIES if args[0] == "all" else [args[0]]
    scens = SCENARIOS if args[1] == "all" else [args[1]]
    sems = SEMANTICS if args[2] == "all" else [args[2]]
    reps = int(args[3])

    results = {}
    for tech in techs:
        for scen in scens:
            for sem in sems:
                results[(tech, scen, sem)] = []
                for rep in range(1, reps + 1):
                    print(f"[{tech}] {scen} / {sem}, Lauf {rep}/{reps} ...")
                    try:
                        r = run_single(tech, scen, sem, rep)
                    except Exception as e:
                        print(f"[{tech}] Lauf abgebrochen: {type(e).__name__}: {e}")
                        continue
                    with open(RESULTS_JSONL, "a") as f:
                        f.write(json.dumps(r) + "\n")
                    print(f"[{tech}] Verlust={r['missing']}, Dupl. verarbeitet={r['duplicates_processed']}, "
                          f"Dupl. erkannt={r['duplicates_skipped']}, erneut zugestellt={r['redeliveries']}, "
                          f"Semantik {'STILLSTAND' if r['semantics_ok'] is None else ('eingehalten' if r['semantics_ok'] else 'VERLETZT')} | "
                          f"{'; '.join(r['events'])}")
                    results[(tech, scen, sem)].append(r)
    write_report(results, reps)


if __name__ == "__main__":
    main()