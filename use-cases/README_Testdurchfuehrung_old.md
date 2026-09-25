# Test-Durchführung: Use Cases 4.2.1 - 4.2.4

Diese Datei begleitet die Testdurchführung aller Use Cases. Reihenfolge der
Abschnitte entspricht der empfohlenen Reihenfolge der Durchführung, von
einfach nach komplex.

---

## 0. Globale Vorbereitung (einmalig, vor allem anderen)

- [ ] Virtuelle Umgebung aktivieren, `requirements.txt` installiert
      (`pika==1.3.2`, `confluent-kafka==2.5.0`, `pymqi==1.12.11`)
- [ ] `podman compose up -d` (compose.yaml mit den Ressourcenlimits, ohne
      `cpuset`, siehe Kapitel-4-Begründung)
- [ ] Idle-Check: `podman stats --no-stream` → alle vier Container sichtbar,
      `MEM USAGE / LIMIT` zeigt die gesetzten Limits, nicht die volle
      Host-Kapazität
- [ ] `podman stop kafka-ui` (nur zur Entwicklung/Fehlersuche, nicht während
      Messungen laufen lassen)
- [ ] Störlasten schließen: Browser, Cloud-Sync, andere Container/VMs (siehe
      Methodik-Notiz zur Reproduzierbarkeit)

source ~/Desktop/poc-messaging/clients/venv/bin/activate

### Topics/Queues einmalig anlegen

| Name | Technologie | Partitionen/Typ | Für Use Case |
|---|---|---|---|
| `latency.test` | Kafka | 1 Partition | Einfache Warteschlange |
| `loadbalance.test` | Kafka | 8 Partitionen | Lastverteilung |
| `broadcast.test` | Kafka | 1 Partition | Event-basierte Benachrichtigung |
| `aggregation.test` | Kafka | 1 Partition | m:1 |
| `semantics.test` | Kafka | 1 Partition | Entkopplung/Zustellsemantiken |

```bash
podman exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
    --topic <NAME> --bootstrap-server localhost:9092 \
    --partitions <N> --replication-factor 1
```

RabbitMQ und IBM MQ legen ihre Queues automatisch beim ersten Verbinden an
(`queue_declare` bzw. Standard-Devloper-Queue `DEV.QUEUE.2`), dafür ist
nichts händisch nötig.

### Vor JEDEM einzelnen Testlauf (nicht nur einmalig)

- [ ] RabbitMQ-Queue leer? `podman exec rabbitmq rabbitmqctl list_queues name messages`
      Falls nicht: `podman exec rabbitmq rabbitmqctl purge_queue <queue>`
- [ ] IBM-MQ-Queue leer? `podman exec ibmmq bash -c "echo 'DIS QL(DEV.QUEUE.2) CURDEPTH' | runmqsc QM1"`
Leeren: `podman exec ibmmq bash -c "echo 'CLEAR QLOCAL(DEV.QUEUE.2)' | runmqsc QM1"`
- [ ] Für Kafka: neue, noch nicht verwendete `run_id`/Consumer-Group pro Lauf
      (sonst werden alte Offsets fortgesetzt statt bei `latest` zu beginnen)

---

## 1. Einfache Warteschlange (1:1)

**Dateien**: `kafka_producer.py` / `kafka_producer_sync.py` / `kafka_producer_delayed.py`,
`kafka_consumer.py`, die jeweiligen `rabbitmq_*`- und `ibmmq_*`-Varianten,
`latency_stats.py`

**Ablauf**:
1. Consumer starten, warten bis "Bereit" gemeldet wird
2. Producer starten (Sync-Variante ist die für Kapitel 6.1.1 relevante
   Referenzkonfiguration, siehe Punkt unten)
3. Latenz-Zusammenfassung am Ende des Consumer-Laufs ablesen

**Besonderheiten**:
- Drei Producer-Varianten existieren aus historischen Gründen (Burst → Delay
  → Sync), für die finale Methodik zählt **nur die Sync-Variante**
  (`*_producer_sync.py` bzw. `ibmmq_producer.py`, das war ohnehin schon
  synchron). Burst und Delay bleiben als Belege für den
  Batching-Artefakt-Befund erhalten, fließen aber nicht als Hauptzahlen in
  Kapitel 6.1.1 ein.
- Bei mehreren Wiederholungen (empfohlen: 5-10 Läufe pro Technologie für
  belastbare Mittelwerte) jeweils eine frische Consumer-Group nötig
  (passiert bei Kafka automatisch über den zeitstempelbasierten Namen).
- Optionaler Zusatzcheck: `monitor_contention.py` parallel mitlaufen lassen
  und mit `analyze_contention.py` auswerten, um Ressourcenkonkurrenz
  auszuschließen (bereits für Kafka mit 1.000.000 Nachrichten validiert,
  RabbitMQ/IBM MQ stehen hierfür noch aus).

---

## 2. Lastverteilung (1:m, Teil 1)

**Dateien**: `lastverteilung_<tech>_producer.py`, `lastverteilung_<tech>_consumer.py`,
`analyze_loadbalance.py`

**Ablauf** (Beispiel 3 Consumer-Instanzen):
1. Drei Terminals: `python lastverteilung_<tech>_consumer.py <run_id> 1` / `2` / `3`
   (dieselbe `run_id`, unterschiedliche Instanz-Nummer)
2. Producer starten: `python lastverteilung_<tech>_producer.py`
3. Nach Abschluss aller Instanzen: `python analyze_loadbalance.py results_loadbalance.jsonl <run_id>`
4. Für die nächste Konfiguration (andere Anzahl Instanzen) neue `run_id`
   verwenden, empfohlen technologie-präfixiert (`kafka-run-3`, `rabbitmq-run-5`, ...)

**Besonderheiten**:
- **Kafka-Obergrenze**: mit 8 Partitionen sind maximal 8 gleichzeitige
  Consumer-Instanzen sinnvoll testbar, mehr bleiben untätig. Für höhere
  Stufen muss das Topic mit mehr Partitionen neu angelegt werden
  (Partitionsanzahl lässt sich nachträglich nur erhöhen, nicht verringern).
- RabbitMQ und IBM MQ haben diese Obergrenze nicht, dort skaliert die
  Lastverteilung ohne Sonderkonfiguration.
- Zustellsemantik ist fest auf **at-least-once** gesetzt (Konzept-Entscheidung),
  nicht variieren.
- `analyze_loadbalance.py` bricht mit Fehler ab, wenn eine `run_id`
  versehentlich für mehrere Technologien verwendet wurde.

---

## 3. Event-basierte Benachrichtigung (1:m, Teil 2)

**Dateien**: `broadcast_<tech>_producer.py`, `broadcast_<tech>_consumer.py`,
`analyze_broadcast.py`

**Ablauf** (Beispiel 3 unabhängige Subscriber):
1. Drei Terminals: `python broadcast_<tech>_consumer.py <run_id> 1` / `2` / `3`
2. Producer starten: `python broadcast_<tech>_producer.py`
3. Auswertung: `python analyze_broadcast.py results_broadcast.jsonl <run_id> <MESSAGE_COUNT>`
   (`MESSAGE_COUNT` steht im jeweiligen Producer-Skript, Standard 1000)

**Besonderheiten**:
- **Reihenfolge der Schritte ist hier strikt**: alle Consumer/Subscriber
  müssen VOR dem Producer laufen und ihre Bindung/Subscription aktiv haben.
  Anders als bei einer Queue gibt es bei Fanout (RabbitMQ) und Pub/Sub
  (IBM MQ) keinen Puffer, der auf einen später startenden Subscriber wartet,
  zu früh gesendete Nachrichten sind für ihn schlicht verloren.
- **IBM MQ ist ungetestet**: die Pub/Sub-API von `pymqi` (`Subscription`,
  `MQSO_MANAGED`) konnte nicht gegen eine echte Instanz verifiziert werden.
  Erst mit `MESSAGE_COUNT = 10` und einem einzelnen Subscriber testen. Bei
  Fehlern zu einem nicht auflösbaren Topic-String oder abweichenden
  pymqi-Methodennamen: Fehlermeldung mitschicken statt selbst zu raten.
- Auswertung prüft **pro Instanz** gegen die Gesamtzahl (nicht summiert wie
  bei Lastverteilung), da jede Instanz alles bekommen soll.

---

## 4. m:1 (mehrere Producer, ein Consumer)

**Dateien**: `m1_<tech>_producer.py`, `m1_<tech>_consumer.py`, `analyze_m1.py`

**Ablauf** (Beispiel 5 Producer):
1. EIN Consumer: `python m1_<tech>_consumer.py <run_id>`
2. Fünf Terminals, möglichst gleichzeitig gestartet:
   `python m1_<tech>_producer.py <run_id> 1` bis `5`
3. Auswertung: `python analyze_m1.py results_m1.jsonl <run_id> <producer_anzahl> <nachrichten_pro_producer>`

**Besonderheiten**:
- Skalierungsvariable ist die **Anzahl der Producer**, konkrete Stufen noch
  mit Betreuer abzustimmen (Vorschlag aus Kapitel 4.2.3: 2, 5, 10, 20).
- Auswertung prüft Verlust **exakt** (jede einzelne Sequenznummer pro
  Producer, nicht nur die Gesamtzahl, sonst könnte ein Verlust durch ein
  zufälliges Duplikat kaschiert werden).
- Reihenfolge wird **nur pro Producer** geprüft, nicht global über alle
  Producer hinweg (dafür gibt es kein definiertes Sollverhalten). Bei
  Reihenfolge-Verletzungen zeigt die Auswertung konkrete Beispielstellen,
  nicht nur ein Ja/Nein.
- Offen laut Konzeptkapitel: ob die Herkunft einer Nachricht (welcher
  Producer) für den Consumer nachvollziehbar sein muss, ist noch nicht
  entschieden, aktuell wird `producer_id` im Payload mitgeschickt, aber
  nicht ausgewertet außer für die Verlust-/Reihenfolgeprüfung selbst.

---

## 5. Entkopplung bei Ausfall und Zustellsemantiken

**Dateien**: `zustellsemantik_<tech>_producer.py` (+ `_resilient.py`-Varianten
für RabbitMQ/IBM MQ), `zustellsemantik_<tech>_consumer.py`,
`run_fault_injection.py` (Kafka, Consumer-Absturz),
`run_fault_injection_rabbitmq.py`, `run_fault_injection_ibmmq.py`,
`run_fault_injection_broker_pause.py`, `run_fault_injection_broker_restart.py`,
`analyze_semantics.py`

Das ist die aufwendigste Testmatrix: 3 Technologien × 3 Semantiken × 3
Fehlerarten = 27 Kombinationen.

### Fehlerart A: Consumer-Absturz

```bash
python run_fault_injection.py <at-most-once|at-least-once|exactly-once>            # Kafka
python run_fault_injection_rabbitmq.py <semantik>                                  # RabbitMQ
python run_fault_injection_ibmmq.py <semantik>                                     # IBM MQ
```

### Fehlerart B: Broker kurz pausiert (Ersatz für Netzwerk-Unterbrechung)

```bash
python run_fault_injection_broker_pause.py <kafka|rabbitmq|ibmmq> <semantik> [pause_sekunden]
```

### Fehlerart C: Broker-Neustart während des Sendens

```bash
python run_fault_injection_broker_restart.py <kafka|rabbitmq|ibmmq> <semantik>
```

Jeder Aufruf gibt am Ende die passende Auswertungszeile aus:

```bash
python analyze_semantics.py results_semantics.jsonl <run_id> 200
```

**Besonderheiten, unbedingt vor der Interpretation lesen**:

- **Was als "Fehler" zählt, hängt von der Semantik ab**, das Auswertungsskript
  ordnet das automatisch ein:
  - `at-most-once`: fehlende Nachrichten sind das **erwartete** Verhalten,
    kein Fehler
  - `at-least-once`: Duplikate sind das **erwartete** Verhalten, fehlende
    Nachrichten wären dagegen ein echter Regelverstoß
  - `exactly-once`: weder Verlust noch unerkannte Duplikate sind akzeptabel
- **IBM MQs bisheriger `at-most-once`-Fund**: euer ursprünglicher
  `ibmmq_consumer.py` aus Kapitel 6.1.1 (ohne Syncpoint) war unbeabsichtigt
  bereits at-most-once. Das ist ein guter Beleg für Kapitel 6.1.4, dass IBM
  MQs naives Standardverhalten nicht automatisch die stärkste Garantie
  liefert.
- **Broker-Neustart braucht die `_resilient`-Producer** für RabbitMQ und IBM
  MQ (`pika`/`pymqi` haben keine eingebaute Wiederverbindung, ohne die
  robusten Varianten würde der Test nur abstürzen statt etwas zu messen).
  Kafka nutzt den normalen Producer, `librdkafka` regelt das intern.
- **IBM-MQ-Wiederverbindungszeit knapp bemessen**: `MAX_RECONNECT_ATTEMPTS`
  (15 × 1s) könnte bei einem echten Container-Neustart zu kurz sein. Bei
  Abbruch mit "erfolglose Wiederverbindungsversuche": Wert in
  `zustellsemantik_ibmmq_producer_resilient.py` erhöhen.
- Alle drei Orchestratoren für Fehlerart A verwenden `SIGKILL` mit
  zufälliger Verzögerung (0,5-2s) nach Sendebeginn, für belastbare Aussagen
  mehrere Wiederholungen pro Zelle der Matrix einplanen, nicht nur einen
  einzelnen Lauf.
- Producerseitiger Ausfall ist laut Konzeptkapitel optional und noch nicht
  mit dem Betreuer abgestimmt, dafür existiert aktuell kein Skript.

---

## Reihenfolge-Empfehlung für die Gesamtdurchführung

1. Einfache Warteschlange (bereits größtenteils erledigt)
2. Lastverteilung
3. Event-basierte Benachrichtigung (IBM-MQ-Teil zuerst im Kleinen testen)
4. m:1
5. Entkopplung bei Ausfall und Zustellsemantiken (aufwendigste Matrix, zuletzt)

Innerhalb von Punkt 5 empfiehlt sich diese Reihenfolge: erst Kafka (am
robustesten, wenigste Überraschungen), dann RabbitMQ, dann IBM MQ (am
meisten offene Verifikationspunkte).
