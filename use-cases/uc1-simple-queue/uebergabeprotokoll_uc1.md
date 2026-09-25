# Übergabeprotokoll: UC1-Testdurchführung Bachelorarbeit

Kontext für den neuen Chat: Bachelorarbeit "Proof of Concept für den
Einsatz von Messaging-Lösungen in verteilten Systemen" (Kafka, RabbitMQ,
IBM MQ). Kapitel 1-5 sind inhaltlich fertig geschrieben (liegt im
Projekt-Gedächtnis). Aktuelle Phase: **Experimentdurchführung**, nicht
Schreibarbeit. Läuft gerade auf einem Reise-Laptop (IdeaPad Pro 5
14AKP10), nicht dem Home-PC, der später für die finalen Zahlen genutzt
wird.

## Was gerade bearbeitet wird

UC1 "Einfache Warteschlange" (Kapitel 6.1.1-Referenzmessung). Ziel:
Latenzmessung für alle drei Technologien bei 10.000 / 100.000 /
1.000.000 Nachrichten, je 5 Wiederholungen.

## Wichtige methodische Entscheidungen (bereits final, nicht neu diskutieren)

- **Zustellsemantik**: einheitlich at-least-once für alle Use Cases außer
  UC5 (Zustellsemantiken), dort ist die Semantik selbst die Testvariable.
- **UC1-Referenzkonfiguration je Technologie**:
  - Kafka: `kafka_producer_sync_paced.py` + `kafka_consumer.py`
  - RabbitMQ: `rabbitmq_producer_sync.py` + `rabbitmq_consumer.py` (ohne
    Drosselung, siehe unten)
  - IBM MQ: `ibmmq_producer_paced.py` + `ibmmq_consumer.py`
- **Producer/Consumer-Geschwindigkeits-Mismatch entdeckt und behoben**:
  ungebremstes Senden lässt bei Kafka und IBM MQ eine Warteschlange
  innerhalb eines Laufs entstehen (Korrelation seq/Latenz nahe 1 statt 0),
  weil der Consumer mit explizitem Commit/Syncpoint langsamer ist als der
  Producer senden kann (~1400 Nachrichten/s Grenze auf diesem Laptop).
  Behoben durch 2 ms Pause zwischen Nachrichten in den `_paced`-Producern.
  **RabbitMQ braucht das NICHT**, `confirm_delivery()` mit
  `delivery_mode=2` bremst dort schon von selbst ausreichend (Fsync-
  Effekt).
- **`delivery_mode=2` (persistent) ist Pflicht** in
  `rabbitmq_producer_sync.py`, nicht `delivery_mode=1`, sonst nicht
  vergleichbar mit Kafka/IBM MQ. Das ist schon mal versehentlich falsch
  gewesen, bitte vor jedem größeren Lauf kurz kontrollieren.
- **`MEASURE_COUNT` wird über eine Umgebungsvariable gesteuert**, nicht
  über ein Kommandozeilenargument. `python kafka_consumer.py 100` setzt
  NICHTS, das Argument wird ignoriert. Richtig:
  `MEASURE_COUNT=100000 python kafka_consumer.py` (oder über den
  Orchestrator, der das automatisch macht).

## Bekannte Stolpersteine (bereits gelöst, zur Erinnerung)

- **pymqi-Build ignoriert `MQ_INSTALLATION_PATH`**, sucht Header/Libs nur
  unter `/opt/mqm` oder `./inc`/`./lib64`. MQ Redistributable Client
  entsprechend dorthin kopieren oder Symlinks anlegen.
- **Kafka-Consumer-Group-Rebalancing**: `kafka_consumer.py` druckt
  "Bereit" bereits vor Abschluss des asynchronen Rebalance. Bei
  automatisierten Läufen zusätzlichen Puffer nach der Bereitschaftszeile
  einplanen (im Orchestrator: 3 Sekunden extra für Kafka).
- **IBM-MQ-Queue-Backlog**: `DEV.QUEUE.2` MUSS vor jedem einzelnen Lauf
  geleert werden (`CLEAR QLOCAL(DEV.QUEUE.2)` via `runmqsc`), sonst
  vermischen sich alte Nachrichten mit neuen und die Latenz wird um
  Größenordnungen verfälscht (ist mehrfach passiert). RabbitMQ ebenso
  (`rabbitmqctl purge_queue latency.test`). Kafka braucht das nicht
  (zeitstempelbasierte Consumer-Group + `auto.offset.reset=latest`).
- **`DEV.QUEUE.2` MAXDEPTH** wurde auf 1.200.000 hochgesetzt (Standard war
  5.000, zu niedrig für die großen Laufstufen).

## Bereits validierte Ergebnisse (10.000 Nachrichten, auf diesem Laptop)

| Technologie | Mittelwert |
|---|---|
| Kafka (gedrosselt) | 0,57 ms |
| IBM MQ (gedrosselt) | ~0,53 ms |
| RabbitMQ (ungedrosselt) | ~0,64-0,70 ms |

Alle drei Korrelationen seq/Latenz nahe 0 bestätigt (kein Warteschlangen-
Aufbau mehr). Diese Werte sind nur vorläufig (Laptop, nicht Home-PC),
dienen aber als Funktionsnachweis, dass die Methodik jetzt korrekt ist.

## Offener Punkt, an dem die letzte Session endete

Ein Orchestrator-Skript (`run_uc1_measurement.py`) soll die komplette
Messreihe automatisieren: Queue leeren, Consumer starten, auf
Bereitschaft warten, Producer starten, Ergebnis parsen, über N
Wiederholungen sammeln, Markdown-Bericht schreiben. Erste Version hatte
zwei Bugs (feste statt tatsächlicher Wartezeit auf die Bereitschaftszeile;
fehleranfällige `selectors`-basierte Leseimplementierung), beide wurden
behoben (jetzt Thread+Queue-basiertes Lesen). **Letzter Stand ist noch
NICHT bestätigt funktionsfähig**, der nächste Schritt ist:

```bash
python run_uc1_measurement.py kafka 100 2
```

Falls das durchläuft, mit `all 10000 5`, dann `all 100000 5`, dann
`all 1000000 5` fortfahren.

## Dateien, die für den neuen Chat relevant sind

**UC1-Kernskripte** (aktueller, korrigierter Stand):
- `kafka_producer_sync_paced.py`, `kafka_consumer.py`
- `rabbitmq_producer_sync.py`, `rabbitmq_consumer.py`
- `ibmmq_producer_paced.py`, `ibmmq_consumer.py`
- `latency_stats.py` (unverändert, wird von allen Consumern importiert)

**Orchestrator**:
- `run_uc1_measurement.py`

**Diagnose-Werkzeuge** (falls nochmal ein Mismatch-Verdacht aufkommt):
- `kafka_consumer_seqdiagnose.py`, `rabbitmq_consumer_seqdiagnose.py`,
  `ibmmq_consumer_seqdiagnose.py`

**Frühere Use Cases** (uc2-uc5, unverändert seit ihrer Fertigstellung,
nur relevant sobald UC1 abgeschlossen ist): alle `lastverteilung_*`,
`broadcast_*`, `m1_*`, `zustellsemantik_*`- und `run_fault_injection*`-
Skripte, plus die zugehörigen `analyze_*.py`- und `run_multi_*.py`-
Werkzeuge.

**Setup/Infrastruktur**: `compose.yaml`, `setup.sh`, `.gitignore`,
`README_Testdurchfuehrung.md`

## Was NICHT neu geklärt werden muss

Alle Kapitel-3-bis-5-Inhalte, Konzeptentscheidungen, Betreuer-Fragen und
Kapitelstruktur liegen im Projekt-Gedächtnis und sind im neuen Chat direkt
abrufbar, dafür muss nichts hochgeladen werden.
