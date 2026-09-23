# Datentransformation durch Kafka und Feature-Vergleich der Messaging-Technologien

## 1. Datentransformation durch Kafka

Im Gegensatz zu IBM MQ und RabbitMQ, die reine Transportmechanismen für Nachrichten sind, bietet Kafka mit seinem Ökosystem native Möglichkeiten zur Verarbeitung und Transformation von Daten direkt im Stream. Das ist einer der zentralen konzeptionellen Unterschiede und ergibt sich unmittelbar aus der Framing-Entscheidung, Kafka als Event-Streaming-Plattform statt als klassischen Message Broker zu behandeln.

### 1.1 Kafka Streams

Kafka Streams ist eine Java-Bibliothek zur Stream-Verarbeitung, die direkt auf Kafka-Topics arbeitet, ohne dass ein separates Verarbeitungscluster (wie bei Spark oder Flink) betrieben werden muss. Typische Operationen:

- **Filterung**: Nachrichten anhand von Inhalt oder Header aussortieren
- **Mapping/Transformation**: Umwandlung von Datenformaten oder -strukturen (z. B. JSON zu Avro, Feldumbenennung, Anreicherung)
- **Aggregation**: Fenster-basierte Aggregationen (z. B. Summen, Zählungen über Zeitfenster)
- **Joins**: Verknüpfung mehrerer Streams oder eines Streams mit einer Tabelle (KTable)

Die Verarbeitung erfolgt applikationsseitig innerhalb der Kafka-Streams-Anwendung, nicht im Broker selbst. Der Broker bleibt dabei zustandslos gegenüber der eigentlichen Transformationslogik; Kafka Streams verwaltet Zustand (State Stores) lokal und fehlertolerant über Changelog-Topics.

### 1.2 ksqlDB

ksqlDB baut auf Kafka Streams auf und bietet eine SQL-ähnliche Abfragesprache für Stream-Verarbeitung, ohne dass dafür Java-Code geschrieben werden muss. Damit lassen sich Transformationen, Filter und Aggregationen deklarativ direkt auf Topics definieren. Für eine Bachelorarbeit ist das vor allem als Beleg dafür relevant, dass Kafka Datenverarbeitung auch für Nutzer ohne tiefe Programmierkenntnisse zugänglich macht.

### 1.3 Kafka Connect und Single Message Transforms (SMTs)

Kafka Connect ist das Integrations-Framework für den Import/Export von Daten zwischen Kafka und externen Systemen (Datenbanken, Dateisysteme, andere Message-Systeme). SMTs erlauben dabei leichtgewichtige Transformationen einzelner Nachrichten beim Import (Source Connector) oder Export (Sink Connector), z. B.:

- Feld-Maskierung (z. B. für sensible Daten)
- Umbenennung oder Entfernen von Feldern
- Typumwandlungen
- Routing basierend auf Nachrichteninhalt

### 1.4 Abgrenzung zu IBM MQ und RabbitMQ

| Aspekt | Kafka | IBM MQ | RabbitMQ |
|---|---|---|---|
| Native Stream-Verarbeitung | Ja (Kafka Streams, ksqlDB) | Nein | Nein |
| Integrations-Framework | Ja (Kafka Connect) | Begrenzt (proprietäre Adapter, IIB/App Connect als separates Produkt) | Begrenzt (Plugins, z. B. Shovel/Federation für Weiterleitung, keine Transformation) |
| Transformation im Broker/Ökosystem | Ja | Nein (reine Zustellung) | Nein (reine Zustellung) |
| Konsequenz für PoC | Kafka kann Datenanreicherung/-transformation als Teil der Pipeline übernehmen, ohne zusätzliches System | Transformation erfordert zusätzliche Middleware (z. B. IBM App Connect) | Transformation erfordert zusätzliche Anwendungslogik außerhalb von RabbitMQ |

Für die Bachelorarbeit lässt sich daraus ableiten: Wird in der Zielarchitektur perspektivisch auch Datenverarbeitung (nicht nur -transport) benötigt, verschiebt sich der Vergleich zugunsten von Kafka, da IBM MQ und RabbitMQ dafür zusätzliche Komponenten in der Systemlandschaft erfordern würden.

## 2. Feature-Vergleich Kafka vs. IBM MQ vs. RabbitMQ

### 2.1 Grundmodell und Architektur

- **Kafka**: Verteiltes, partitioniertes Commit-Log. Nachrichten werden an Topics angehängt (Append-only) und in Partitionen über Broker verteilt. Konsumenten lesen anhand eines Offsets, unabhängig von anderen Konsumenten.
- **IBM MQ**: Klassischer Message Broker nach Point-to-Point- und Publish/Subscribe-Modell. Warteschlangen (Queues) als zentrales Konzept, Nachrichten werden nach erfolgreicher Zustellung aus der Queue entfernt.
- **RabbitMQ**: Broker nach dem AMQP-Modell. Producer senden an Exchanges, die Nachrichten anhand von Routing-Regeln an gebundene Queues weiterleiten. Sehr flexibles Routing durch verschiedene Exchange-Typen.

### 2.2 Persistenz und Nachrichtenlebenszyklus

- **Kafka**: Retention-basiert (Zeit- oder Größenlimit), Nachrichten bleiben nach dem Konsum erhalten. Dadurch ist Replay möglich (mehrfaches Lesen derselben Nachrichten durch unterschiedliche Konsumenten oder erneutes Lesen nach Fehlern).
- **IBM MQ**: Nachricht wird nach erfolgreicher Zustellung (bzw. Bestätigung durch den Konsumenten) aus der Queue gelöscht. Kein Replay im eigentlichen Sinn vorgesehen.
- **RabbitMQ**: Analog zu IBM MQ, Nachricht wird nach Acknowledgement aus der Queue entfernt. Kein natives Replay.

### 2.3 Routing und Zustellmuster

- **Kafka**: Routing über Topic- und Partitionswahl (durch Partitionierungsschlüssel gesteuert). Consumer Groups ermöglichen Lastverteilung (jede Partition wird nur von einem Consumer der Gruppe gelesen) sowie Fan-out (mehrere unabhängige Consumer Groups lesen dasselbe Topic vollständig).
- **IBM MQ**: Point-to-Point über Queues, Publish/Subscribe über Topics (mit Subscriptions). Routing-Logik ist eher statisch konfiguriert.
- **RabbitMQ**: Sehr flexibel durch vier Exchange-Typen:
  - *Direct*: exakte Routing-Key-Übereinstimmung
  - *Topic*: Muster-basiertes Routing (Wildcards)
  - *Fanout*: Broadcast an alle gebundenen Queues (klassisches 1:m-Muster)
  - *Headers*: Routing anhand von Nachrichten-Headern statt Routing-Key

### 2.4 Delivery-Garantien (Kurzüberblick)

Eine detaillierte experimentelle Untersuchung der drei Zustellsemantiken (at-most-once, at-least-once, exactly-once) je Technologie ist als eigenes Experiment vorgesehen. An dieser Stelle nur die konzeptionelle Einordnung:

- **Kafka**: Alle drei Semantiken konfigurierbar (Offset-Commit-Zeitpunkt, Idempotent Producer, Transactions API für exactly-once)
- **IBM MQ**: Exactly-once als Standardfall bei transaktionaler Nutzung (Syncpoint, XA-Transaktionen), at-most-once bei nicht-persistenten, nicht-transaktionalen Nachrichten
- **RabbitMQ**: At-most-once (Auto-Ack) und at-least-once (manuelles Ack mit Requeue) nativ unterstützt, exactly-once nur über zusätzliche Deduplizierungslogik auf Konsumentenseite erreichbar

### 2.5 Skalierbarkeit und Durchsatz

- **Kafka**: Horizontale Skalierung über Partitionen und zusätzliche Broker, konzipiert für sehr hohen Durchsatz (ursprünglich für LinkedIns Aktivitäts-Feeds und Metriken entwickelt)
- **IBM MQ**: Vertikale Skalierung im Vordergrund, horizontale Skalierung über Clustering möglich, aber komplexer zu betreiben; historisch auf Enterprise-/Mainframe-Workloads mit hohen Zuverlässigkeitsanforderungen ausgelegt, nicht primär auf Massendurchsatz
- **RabbitMQ**: Clustering und Federation für horizontale Skalierung vorhanden, Durchsatz i. d. R. niedriger als Kafka bei vergleichbarer Nachrichtengröße, da jede Nachricht pro Queue einzeln verwaltet wird statt log-basiert angehängt

### 2.6 Stream-/Datenverarbeitung

Siehe Abschnitt 1. Nur Kafka bietet native Verarbeitungsmöglichkeiten (Kafka Streams, ksqlDB). IBM MQ und RabbitMQ benötigen dafür zusätzliche Systeme.

### 2.7 Ökosystem und Integration

- **Kafka**: Kafka Connect mit einer Vielzahl vorgefertigter Konnektoren (Datenbanken, Cloud-Speicher, andere Messaging-Systeme), großes Open-Source-Ökosystem
- **IBM MQ**: Starke Integration in IBM-Mainframe- und Enterprise-Umgebungen (z-Systeme, CICS), etablierte Enterprise-Support-Strukturen
- **RabbitMQ**: Client-Bibliotheken für viele Sprachen, Plugin-System (Management-UI, Shovel, Federation), kleineres Integrations-Ökosystem als Kafka

### 2.8 Betriebs- und Lizenzmodell

- **Kafka**: Apache 2.0 Lizenz (Open Source), kommerzieller Support optional über Confluent oder andere Anbieter
- **IBM MQ**: Kommerzielles Produkt mit Lizenzkosten (relevant für die im Thesis-Scope vorgesehene Lizenzkostenanalyse), Community Edition mit eingeschränktem Funktionsumfang verfügbar
- **RabbitMQ**: Mozilla Public License (Open Source), kommerzieller Support optional über VMware/Broadcom (Tanzu)

### 2.9 Zusammenfassende Vergleichstabelle

| Kriterium | Kafka | IBM MQ | RabbitMQ |
|---|---|---|---|
| Grundmodell | Verteiltes Commit-Log | Message Queue (P2P/Pub-Sub) | AMQP-Broker mit Exchange-Routing |
| Persistenz | Retention-basiert, Replay möglich | Löschung nach Zustellung | Löschung nach Zustellung |
| Routing-Flexibilität | Partition/Topic-basiert | Eher statisch | Sehr hoch (vier Exchange-Typen) |
| Stream-Verarbeitung | Ja (Streams, ksqlDB) | Nein | Nein |
| Durchsatz | Sehr hoch | Moderat | Hoch, aber unter Kafka |
| Delivery-Garantien | Alle drei Semantiken konfigurierbar | Exactly-once als Standardfall (transaktional) | At-most/at-least-once nativ, exactly-once nur mit Zusatzlogik |
| Horizontale Skalierung | Sehr gut (Partitionen) | Eingeschränkt (Clustering komplex) | Gut (Clustering, Federation) |
| Integrations-Ökosystem | Sehr groß (Kafka Connect) | Stark im Enterprise-/Mainframe-Umfeld | Mittel (Plugins) |
| Lizenzmodell | Open Source (Apache 2.0) | Kommerziell, lizenzpflichtig | Open Source (MPL) |
| Typischer Einsatzzweck | Event-Streaming, Datenpipelines, hoher Durchsatz | Kritische Enterprise-Transaktionen, garantierte Zustellung | Flexibles Routing, klassisches Messaging mittlerer Last |
