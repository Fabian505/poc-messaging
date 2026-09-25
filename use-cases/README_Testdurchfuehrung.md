# Testdurchführung – Anleitung für den Messtag

Stand: 24.09.2026. Ersetzt die bisherige `README_Testdurchfuehrung.md`.
Gilt für die finale Datenerhebung auf dem Home-PC. Laptop-Läufe sind nur
Funktionsnachweis.

---

## Teil A – Vorbereitung (vor dem Messtag, auf dem Home-PC)

### A1. Repository aktualisieren

Neue bzw. ersetzte Dateien je Ordner:

| Ordner | Dateien |
|---|---|
| Wurzel | `setup.sh`, `requirements.txt`, `pre_measurement_check.sh`, `run_isolated.sh`, `compose.yaml` |
| uc1 | `run_uc1_measurement.py`, `latency_stats.py`, `kafka_consumer.py`, `kafka_producer_sync_paced.py`, `rabbitmq_producer_sync.py`, `ibmmq_producer_paced.py` |
| uc2 | `run_uc2_measurement.py`, `uc2_common.py`, alle `lastverteilung_*.py` |
| uc3 | `run_uc3_measurement.py`, `uc3_common.py`, `uc3_pacing.py`, alle `broadcast_*.py` |
| uc4 | `run_uc4_measurement.py`, `m1_common.py`, alle `m1_*.py` |
| uc5 | `run_uc5_measurement.py`, `uc5_common.py`, alle `zustellsemantik_*.py` |

Veraltet und zu löschen (damit nichts versehentlich damit gemessen wird):
`analyze_loadbalance.py`, `analyze_broadcast.py`, `analyze_m1.py`,
`analyze_semantics.py`, alle `run_fault_injection*.py`,
`zustellsemantik_*_producer_resilient.py`, `run_multi_*.py` sowie die alten
`results_loadbalance.jsonl`, `results_broadcast.jsonl`, `results_m1.jsonl`,
`results_semantics.jsonl`.

Danach committen, damit der gemessene Code-Stand eindeutig ist. Den Commit-Hash
am Messtag notieren.

### A2. Voraussetzungen installieren

1. **Python 3.13** (dieselbe Minor-Version wie auf dem Laptop). Kein Conda.
2. **IBM MQ Redistributable Client** nach `/opt/mqm` kopieren (Header unter
   `/opt/mqm/inc`, Bibliotheken unter `/opt/mqm/lib64`). **Nicht** über den
   regulären Installer, kein Eintrag unter `/etc/ld.so.conf.d/`
   (siehe Lessons Learned, `mqm.conf`-Vorfall).
3. `compose.yaml` (aktualisierte Fassung mitgeliefert) prüfen: `MQ_APP_PASSWORD`
   und `MQ_ADMIN_PASSWORD` müssen **vor dem ersten Start** gesetzt sein.
   Nachträgliches Setzen wirkt nicht auf einen bestehenden Queue Manager.
   Beide Admin-Oberflächen (RabbitMQ Management-Plugin, IBM MQ Web Console)
   sind jetzt deaktiviert, damit keine Technologie mit einer zusätzlichen
   Admin-Oberfläche im Hintergrund gemessen wird (Vergleichbarkeit).
   `MQ_ENABLE_EMBEDDED_WEB_SERVER` gehört unter `environment:`, nicht auf
   Service-Ebene — dort wird es von Compose stillschweigend ignoriert.

### A3. Umgebung einrichten

```bash
conda deactivate          # falls aktiv
PYTHON=python3.13 ./setup.sh
```

Erwartet am Ende: Kontrollzeilen `MAXDEPTH(1200000)` und
`TOPICSTR(dev/broadcast)`, Importtest mit den drei Versionsnummern.

### A4. Persistenz am Broker verifizieren

Nicht aus dem Code ablesen, sondern am Broker prüfen (Lessons Learned: IBM MQ
war trotz Code-Änderung nicht persistent).

```bash
source venv/bin/activate

# IBM MQ (im uc1-Ordner)
MEASURE_COUNT=3 python ibmmq_producer_paced.py
podman exec ibmmq /opt/mqm/samp/bin/amqsbcg DEV.QUEUE.2 QM1 | grep Persistence   # alle: 1
podman exec ibmmq bash -c "echo 'CLEAR QLOCAL(DEV.QUEUE.2)' | runmqsc QM1"

# RabbitMQ (im uc1-Ordner)
MEASURE_COUNT=3 python rabbitmq_producer_sync.py
podman exec rabbitmq rabbitmqctl list_queues name messages messages_persistent   # latency.test: 13 13
podman exec rabbitmq rabbitmqctl purge_queue latency.test
```

### A5. Funktionstest aller Use Cases (kleine Parameter)

Jeder Befehl muss ohne abgebrochenen Lauf durchgehen, alle Läufe „vollständig“.

```bash
cd uc1 && python run_uc1_measurement.py all 100 1
TARGET_RATE=2000 python run_uc1_measurement.py all 2000 1   # Stufenreihe: Bericht mit Soll-Last 2000
cd ../uc2 && python run_uc2_measurement.py all 10000 1 1,2
cd ../uc3 && python run_uc3_measurement.py all 100 1 1,2
cd ../uc4 && python run_uc4_measurement.py all 100 1 1,2
cd ../uc5 && python run_uc5_measurement.py all crash-post at-least-once 1
cd ../uc5 && python run_uc5_measurement.py rabbitmq broker-restart at-least-once 1
```

UC5 zusätzlich einmal vollständig klein testen, weil er am meisten Neues enthält
(Kafka + consumer-kill zeigte im ersten Laptop-Test einen Fehler im
Testaufbau, behoben am 24.09.2026):

```bash
python run_uc5_measurement.py all all all 1
```

Erwartung UC5 (deterministisch, siehe Teil C): at-least-once und exactly-once
immer „eingehalten“, at-most-once zeigt bei `crash-pre` Verlust. Weicht das
ab, **vor dem Messtag klären**.

### A6. Zeitplan

Grobe Schätzung der reinen Laufzeiten (plus Auf- und Abbau):

| Block | Befehl | Dauer (Schätzung) |
|---|---|---|
| UC1 10k | `all 10000 5` | ~10 min |
| UC1 100k | `all 100000 5` | ~90 min |
| UC1 1M | `all 1000000 3` | ~5 h → **über Nacht** |
| UC2 | `all 100000 10 1,2,4,8` | ~60–90 min |
| UC2 mit Verarbeitung | `PROCESSING_MS=2 … all 20000 5 1,2,4,8` | ~30 min |
| UC3 | `all 10000 5 1,2,4,8` | ~40 min |
| UC4 | `all 10000 5 1,2,4,8` | ~40 min |
| UC5 | `all all all 5` | ~90 min (IBM-MQ-Neustarts dauern) |
| UC1-Stufenreihe | 5 Stufen × `all 20000 3` | ~30 min |

UC1 mit 1M und 5 Wiederholungen wären etwa 8 Stunden (1M bei 500/s = 33 min
pro Lauf). 3 Wiederholungen reichen für die Dauerlast-Aussage.

---

## Teil B – Am Messtag

### B0. Aufwärmen und Isolation — für jeden Messblock

Für **jeden** Block (UC1 10k, UC1 100k, UC1-Stufenreihe, UC2, UC3, UC4, UC5,
UC1 1M) in dieser Reihenfolge:

1. **Aufwärmlauf**, klein und ungewertet, IMMER unmittelbar vor dem eigentlichen
   Lauf, mit denselben Parametern (Technologie, Rate) wie der Messlauf selbst,
   nur mit weniger Nachrichten/Wiederholungen. Grund: Aufwärmeffekt nach jedem
   Konfigurationswechsel oder Broker-Neustart (siehe UC1-Persistenzvergleich,
   Lessons Learned) — nicht nur einmal am Tagesanfang, sondern vor jedem Block,
   weil `run_isolated.sh` die Broker zwischen den Blocks stoppt und neu startet.
   Bericht/Ausgabe des Aufwärmlaufs verwerfen (z. B. in `warmup/` verschieben
   statt löschen, für den Fall einer Nachprüfung).

   **Den Aufwärmbericht selbst prüfen, bevor der Messlauf beginnt:** Rate nahe
   der Soll-Last und Drift nahe 0. Ist das NICHT der Fall (beobachtet am
   24.09.2026 bei Kafka nach einem Container-Neustart: Aufwärmlauf selbst
   noch bei 230 ms Mittel statt ~0,8 ms), ist der Broker noch nicht
   eingeschwungen — dann den Aufwärmlauf wiederholen, bevor gemessen wird.
   `run_isolated.sh` wartet bei Kafka deshalb nach der Bereitschaftsprüfung
   zusätzlich `KAFKA_STABILIZE_SECONDS` (Standard 20 s), weil `kafka-topics
   --list` antwortet, bevor sich der KRaft-Controller intern stabilisiert
   hat. Reicht das nicht, den Wert erhöhen: `KAFKA_STABILIZE_SECONDS=40
   ./run_isolated.sh kafka …`.
2. **Isoliert messen** mit `run_isolated.sh` (siehe unten), außer bei UC-Läufen,
   die mehrere Technologien gleichzeitig brauchen (UC1–UC4 mit `all`): dort
   ohne Isolation, siehe Hinweis unten.

```bash
cd uc1
../../run_isolated.sh kafka python run_uc1_measurement.py kafka 500 1        # Aufwärmen
../../run_isolated.sh kafka python run_uc1_measurement.py kafka 10000 5      # Messung
```

`run_isolated.sh <technologie> <befehl...>` stoppt die beiden anderen Broker,
wartet, bis der gemessene bereit ist, führt den Befehl aus und startet die
beiden anderen danach garantiert wieder (auch bei Fehler oder Abbruch).

**Wichtiger Kompromiss:** Isolation und `all`-Aufrufe schließen sich aus, weil
`all` alle drei Technologien nacheinander braucht, aber immer nur ein Broker
läuft. Zwei Möglichkeiten:
- **Isoliert, aber pro Technologie einzeln aufrufen** (dreimal `run_isolated.sh
  <tech> python run_uc1_measurement.py <tech> …`) — sauberer, aber die
  automatische Berichtszusammenfassung über alle drei Technologien entfällt,
  Ergebnisse liegen in getrennten Berichten.
- **Mit `all`, ohne Isolation** — ein Bericht, aber mit der gemessenen
  Leerlauflast der beiden wartenden Broker (~12–13 %, siehe Lessons Learned)
  als Störgröße.

  Empfehlung: **isoliert und einzeln**, weil Vergleichbarkeit zwischen den
  Technologien hier wichtiger ist als ein gemeinsamer Bericht. In Kapitel 6
  entsprechend dokumentieren.

### B0a. Konkrete Aufwärm- und Messbefehle je UC

| UC | Aufwärmen | Messung |
|---|---|---|
| UC1 | `run_uc1_measurement.py <tech> <warmup_n> 1` | `run_uc1_measurement.py <tech> 10000 5` |
| UC1-Stufenreihe | `TARGET_RATE=$r run_uc1_measurement.py <tech> <warmup_n> 1` | `TARGET_RATE=$r run_uc1_measurement.py <tech> 20000 3` |
| UC2 | `run_uc2_measurement.py <tech> 10000 1 1,2,4,8` | `run_uc2_measurement.py <tech> 100000 10 1,2,4,8` |
| UC3 | `run_uc3_measurement.py <tech> <warmup_n_uc3> 1 1,2,4,8` | `run_uc3_measurement.py <tech> 10000 5 1,2,4,8` |
| UC4 | `run_uc4_measurement.py <tech> <warmup_n_uc4> 1 1,2,4,8` | `run_uc4_measurement.py <tech> 10000 5 1,2,4,8` |
| UC5 | `run_uc5_measurement.py <tech> all all 1` | `run_uc5_measurement.py <tech> all all 5` |

`<tech>` = `kafka` / `rabbitmq` / `ibmmq`. **`<warmup_n>` ist NICHT für alle
drei gleich:**

- RabbitMQ, IBM MQ: 500 reicht.
- **Kafka: mindestens 5000, nicht 500.** Befund vom 24.09.2026: Der
  Kafka-Broker (JVM) braucht nach einem (Neu-)Start keine Wartezeit,
  sondern Durchsatz — die JIT-Kompilierung häufig durchlaufener
  Codepfade („Hot-Spot“-Erkennung) springt erst nach einer Mindestzahl
  an Nachrichten an, nicht nach einer Mindestzeit. Ein reines `sleep()`
  vorher ändert daran nichts, auch nicht mit 20 Sekunden (getestet, half
  nicht). Empirisch war 3000 NICHT ausreichend (blieb bei ~120 ms statt
  ~0,8 ms), 5000 direkt danach ausreichend (folgender Messlauf ab Lauf 1
  sauber). Die genaue Schwelle ist nicht scharf (kumulativer Effekt über
  mehrere Läufe hinweg beobachtet) — 5000 ist der erste bestätigt
  ausreichende Wert, kein Sicherheitsspielraum ist darin. Auf dem Home-PC
  (andere CPU) erneut verifizieren.
  `run_isolated.sh` wartet bei Kafka deshalb nur noch kurz (Standard 5 s,
  nur für den Netzwerk-/Socket-Anlauf) und verlässt sich für die
  eigentliche Aufwärmung auf den hier dokumentierten Aufwärmlauf mit
  ausreichend Nachrichten.
- **Zweite Prüfebene: nicht nur den Aufwärmbericht ansehen, auch Lauf 1
  der echten Messung.** Weicht Lauf 1 noch von Lauf 2–5 ab (höheres
  Mittel, niedrigere Rate, höherer Rückstand), war der Aufwärmlauf zu
  klein — der automatisch berechnete "Mittelwert über alle N Läufe" im
  Bericht ist dann durch Lauf 1 nach oben verzerrt. In dem Fall: mit
  größerem Aufwärmlauf wiederholen, nicht den verzerrten Mittelwert
  übernehmen.
- **Reihenfolge am Messtag: pro Technologie über alle UC, nicht pro UC
  über alle Technologien.** `run_isolated.sh` startet Kafka bei jedem
  Wechsel zu einer anderen Technologie und zurück neu — die JVM verliert
  dabei den kompletten JIT-Zustand, die teure Aufwärmung wäre für jeden
  UC erneut fällig. Wird stattdessen Kafka für UC1 bis UC5 am Stück
  gemessen (ohne zwischendurch RabbitMQ/IBM MQ zu isolieren), bleibt die
  JVM durchgehend warm, nur EINE Kafka-Aufwärmung zu Beginn des
  Kafka-Blocks nötig. Siehe B3, angepasste Reihenfolge.
- Bei UC3/UC4 mit mehreren Subscribern/Producern reicht die Gesamtsumme,
  `<warmup_n_uc3>`/`<warmup_n_uc4>` also z. B. 3000 statt 100 setzen, wenn
  Kafka Teil des Laufs ist (bei `all` oder wenn `<tech>`=kafka).
- **Den Aufwärmbericht immer selbst prüfen** (Rate ≈ Soll-Last, Drift ≈ 0),
  bevor der Messlauf beginnt — das ist der einzige verlässliche Nachweis,
  dass genug durchgesetzt wurde. Reicht die Menge nicht, den Aufwärmlauf
  mit mehr Nachrichten wiederholen, nicht einfach länger warten.

Für UC1–UC4, isoliert, alle drei Technologien nacheinander (Beispiel UC1,
analog für UC2–UC4 mit den Parametern aus der Tabelle; bei Kafka `500` durch
`3000` ersetzen):

```bash
cd uc1
declare -A WARMUP=( [kafka]=5000 [rabbitmq]=500 [ibmmq]=500 )
for tech in kafka rabbitmq ibmmq; do
  ../../run_isolated.sh $tech python run_uc1_measurement.py $tech ${WARMUP[$tech]} 1   # Aufwärmen
  # Bericht UND Lauf 1 der Messung pruefen: Rate ≈ Soll, Drift ≈ 0 — sonst
  # Aufwärmlauf mit mehr Nachrichten wiederholen (v.a. bei Kafka)
  ../../run_isolated.sh $tech python run_uc1_measurement.py $tech 10000 5              # Messung
done
```

**Besser, wegen der JIT-Erkenntnis oben: über den ganzen Tag pro Technologie
statt pro UC schleifen**, damit Kafka nur einmal aufgewärmt werden muss statt
fünfmal (einmal je UC):

```bash
# Kafka: EIN Aufwärmen, dann UC1 bis UC5 am Stück, ohne zwischendurch
# RabbitMQ/IBM MQ zu isolieren (das würde Kafka neu starten und die
# Aufwärmung zunichtemachen)
../../run_isolated.sh kafka bash -c '
  cd uc1  && python run_uc1_measurement.py kafka 5000 1 && python run_uc1_measurement.py kafka 10000 5
  cd ../uc2 && python run_uc2_measurement.py kafka 100000 10 1,2,4,8
  cd ../uc3 && python run_uc3_measurement.py kafka 10000 5 1,2,4,8
  cd ../uc4 && python run_uc4_measurement.py kafka 10000 5 1,2,4,8
'
# danach analog fuer rabbitmq, dann ibmmq (dort reicht der kleine Aufwärmlauf
# aus B0a je Block, kein JIT-Effekt)
```

UC5 läuft OHNE `run_isolated.sh` (Orchestrator steuert die Broker-
Fehlerinjektion selbst):

```bash
cd ../uc5
for tech in kafka rabbitmq ibmmq; do
  systemd-inhibit --what=idle:sleep --why=Messung python run_uc5_measurement.py $tech all all 1   # Aufwärmen
  systemd-inhibit --what=idle:sleep --why=Messung python run_uc5_measurement.py $tech all all 5   # Messung
done
```

### B1. Ausgangszustand

1. Rechner **neu starten**. Keine anderen Programme öffnen (Browser, IDE,
   Cloud-Sync, Updates).
2. Terminal öffnen, dann:

```bash
conda deactivate
cd ~/…/poc-messaging && source venv/bin/activate
sudo cpupower frequency-set -g performance
powerprofilesctl set performance            # falls vorhanden
podman compose up -d
./pre_measurement_check.sh
```

3. **Nur bei 0 FAIL weitermachen.** WARN-Punkte bewusst prüfen.
   Den erzeugten `messumgebung_<zeitstempel>.txt` aufheben.

### B2. (entfällt — siehe B0, Aufwärmen ist jetzt Teil jedes Blocks)

### B3. Messläufe

Jeden Lauf **einzeln** und immer mit `systemd-inhibit` starten (verhindert
Leerlauf und Suspend nur für die Dauer des Laufs). **Nie zwei Läufe parallel**,
auch keine kurzen Tests zwischendurch. Isolation (`run_isolated.sh`) und
Aufwärmen laut B0 vor jedem Block.

```bash
systemd-inhibit --what=idle:sleep --why=Messung \
  ./run_isolated.sh kafka python run_uc1_measurement.py kafka 10000 5
```

UC5 (Fehlerinjektion mit `podman pause`/`restart`) läuft technisch mit allen
drei Brokern gleichzeitig, weil der Orchestrator selbst den gemessenen Broker
gezielt stört — hier NICHT mit `run_isolated.sh` kombinieren, die beiden
Skripte würden sich gegenseitig in die Quere kommen (Broker stoppen sich
gegenseitig). UC5 bleibt wie in B3 ursprünglich beschrieben: ganz normal mit
`systemd-inhibit`, ohne `run_isolated.sh`.

Empfohlene Reihenfolge: UC1 10k → UC1 100k → UC1-Stufenreihe (B3a) → UC2 →
UC3 → UC4 → UC5, dann UC1 1M über Nacht (vorher erneut
`./pre_measurement_check.sh`).

### B3a. UC1-Stufenreihe: Grenze der synchronen Clients

Frage: Ab welcher Soll-Last kommt die Konfiguration aus UC1 (synchrones
Senden, Commit/Ack pro Nachricht, ein Producer, ein Consumer) nicht mehr mit?
Die Soll-Last wird über `TARGET_RATE` gesetzt (Standard 500), alles andere
bleibt identisch zu UC1.

```bash
cd uc1
for r in 500 1000 1500 2000 3000; do
  TARGET_RATE=$r systemd-inhibit --what=idle:sleep --why=Messung \
    python run_uc1_measurement.py all 20000 3
done
```

Die Berichte heißen `uc1_bericht_20000n_<rate>r_<zeit>.md`. Auswertung je
Technologie und Stufe:

| Beobachtung | Bedeutung |
|---|---|
| Rate ≈ Soll, Drift ≈ 0, Latenz wie bei 500 | Stufe wird bewältigt |
| Rate ≈ Soll, **Drift deutlich positiv**, Latenz wächst | **Consumer** ist der Engpass (Warteschlange baut sich im Lauf auf) |
| **Rate deutlich unter Soll**, viel Rückstand | **Producer** ist der Engpass (synchrones Senden dauert länger als das Intervall) |

Die letzte bewältigte Stufe ist die Kapazität dieser Client-Konfiguration,
**nicht** die des Brokers. So auch in der Arbeit formulieren. Die
Broker-Grenzen (Herstellerwerkzeuge) sind eine offene Frage an den Betreuer.

Erwartung (Laptop-Diagnose): Kafka und IBM MQ um ~1.400/s am Consumer;
RabbitMQ vermutlich früher am Producer, weil jedes Publisher-Confirm auf den
Fsync wartet.

Nach jedem Block kurz den Bericht prüfen (Teil C). Bei Auffälligkeiten nicht
weitermessen, sondern erst `./pre_measurement_check.sh` und Ursache klären.

### B4. Abschluss

```bash
mkdir -p messungen/$(date +%Y-%m-%d)
mv uc*/uc*_bericht_*.md uc*/results_*.jsonl messumgebung_*.txt messungen/$(date +%Y-%m-%d)/
git add messungen && git commit -m "Messdaten Home-PC $(date +%Y-%m-%d)"
sudo cpupower frequency-set -g schedutil     # bzw. vorherigen Governor
```

---

## Teil C – Plausibilitätsprüfung je Bericht

| UC | Muss erfüllt sein | Verdächtig |
|---|---|---|
| UC1 | Rate ≈ 500 msg/s bei allen drei, Drift ≈ 0 | Rückstand > 5 % der Nachrichten; Drift deutlich positiv; ein Lauf um Größenordnungen abweichend (Queue nicht geleert? bei Kafka als erster Lauf nach Neustart: Stabilisierung, siehe B0) |
| UC2 | alle Läufe „vollständig“, Verteilung Kafka exakt gleich | Speedup sinkt über die Wiederholungen (Throttling, Hintergrundlast) |
| UC3 | alle Subscriber vollständig, 0 fehlend | Rückstand wächst stark mit S (bei RabbitMQ als Befund erwartet, sonst prüfen) |
| UC4 | 0 Reihenfolge-Verletzungen je Producer, vollständig | Verletzungen > 0 → Befund, Lauf wiederholen und prüfen |
| UC5 | at-least-once/exactly-once: Semantik immer eingehalten | at-least-once mit Verlust oder exactly-once mit verarbeiteten Duplikaten = Befund, Einzellauf ansehen |

UC5, erwartete Ergebnisse bei gezieltem Absturz (Consumer beendet sich bei
Nachricht N/3 selbst per SIGKILL):

| Semantik | crash-pre (vor Seiteneffekt) | crash-post (nach Seiteneffekt, vor Bestätigung) |
|---|---|---|
| at-most-once | 1 Verlust | kein Effekt sichtbar |
| at-least-once | 1 erneute Zustellung, kein Duplikat | 1 Duplikat verarbeitet |
| exactly-once | 1 erneute Zustellung, kein Duplikat | 1 Duplikat erkannt |

Bei RabbitMQ at-most-once können es mehr als eine verlorene Nachricht sein
(`auto_ack`, prefetch wirkungslos, der Client-Puffer geht mit verloren), wenn
sich ein Rückstand aufgebaut hat. Jede Abweichung von der Tabelle ist ein
Befund und muss im Einzellauf nachvollzogen werden.

Broker-Szenarien (`broker-pause`, `broker-restart`): Der Zustand des Consumers
im Ausfallmoment ist nicht festgelegt, Ergebnisse variieren zwischen Läufen.
Während des Ausfalls blockiert der Producer (Broker = Single Point of Failure
ohne Cluster).
- „Während Ausfall gesendet / verarbeitet“: Beleg der zeitlichen Entkopplung.
  Bei at-least-once/exactly-once müssen beide Zahlen gleich sein.

---

## Teil D – Bekannte Fehlerbilder

| Symptom | Ursache | Behebung |
|---|---|---|
| `2035 MQRC_NOT_AUTHORIZED` beim Connect | `MQ_APP_PASSWORD` fehlt oder Queue Manager vor dem Setzen angelegt | Datenverzeichnis `ibmmq/data` löschen, `./setup.sh` |
| `2035` in UC3 (IBM MQ) | Topic-Objekt `DEV.BROADCAST.TOPIC` fehlt | `./setup.sh` |
| „Consumer hat keine Bereitschaft gemeldet“ | Container nicht bereit oder Skript stürzt ab | Rohausgabe in der Meldung lesen, `podman logs --tail 50 <container>` |
| UC1-Latenz um Größenordnungen zu hoch | Reste in der Queue | Orchestrator leert automatisch; sonst `CLEAR QLOCAL(DEV.QUEUE.2)` bzw. `purge_queue` |
| `pymqi`-Build schlägt fehl | Header nicht unter `/opt/mqm/inc` | Client nach `/opt/mqm` kopieren, nicht `MQ_INSTALLATION_PATH` |
| Kafka-Consumer steht nach Neustart lange still | Session-Timeout des abgestürzten Mitglieds | in UC5 auf 6 s gesetzt; länger = Befund, nicht Fehler |
| UC5: offene Frage | zufällige Fehlerinjektion (Chaos-Modus) | mit Betreuer abstimmen, bisher nicht umgesetzt |
| Kafka-Lauf direkt nach Broker-(Neu-)Start deutlich zu langsam (zweistellig ms statt < 1 ms) | KRaft-Controller intern noch nicht stabilisiert, `kafka-topics --list` meldet fälschlich bereit | `run_isolated.sh` wartet automatisch `KAFKA_STABILIZE_SECONDS` (Standard 20 s); Aufwärmbericht prüfen, notfalls wiederholen |