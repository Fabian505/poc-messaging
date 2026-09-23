# Latenzmessung (Kapitel 6.1.1 / 6.2)

## Messverfahren

Zeitstempel-basiert: Der Producer schreibt den Sendezeitpunkt (UTC, ISO 8601)
in die Nachricht selbst. Der Consumer berechnet die Latenz als Differenz
zwischen Empfangszeitpunkt und diesem Sendezeitpunkt. Da Producer und
Consumer auf demselben Host laufen, teilen sie sich dieselbe Systemuhr,
Uhrenabweichung (Clock Skew) faellt als Fehlerquelle weg.

## Warmup-Phase

Die ersten 10 Nachrichten (WARMUP_COUNT) werden gesendet, aber vom Consumer
verworfen. Sie dienen dem Aufwaermen von Verbindungsaufbau, JIT-Compilation
(relevant v. a. bei Kafka, das intern auf der JVM laeuft) und internen
Caches. Erst danach beginnt die eigentliche Messung ueber 100 Nachrichten
(MEASURE_COUNT).

## Reihenfolge (kritisch)

Der Consumer MUSS vor dem Producer gestartet werden und aktiv auf
Nachrichten warten. Andernfalls misst man die Zeit, die der Bediener
zwischen den beiden Befehlen braucht, statt echter Systemlatenz. Bei Kafka
zusaetzlich wichtig: der Consumer muss seiner Consumer Group bereits
beigetreten sein (Rebalancing abgeschlossen), bevor der Producer startet,
sonst verfaelscht der Rebalancing-Aufwand die ersten Messwerte zusaetzlich
zur Warmup-Phase.

## Ausgewertete Kennzahlen

Minimum, Maximum, Mittelwert, Median, P95, P99, Standardabweichung. Perzentile
sind fuer Messaging-Systeme aussagekraeftiger als der reine Mittelwert, weil
gelegentliche Ausreisser (z. B. durch GC-Pausen bei Kafka) den Mittelwert
verzerren, ohne das typische Verhalten widerzuspiegeln.

## Bekannte Einschraenkungen

- Producer und Consumer laufen auf demselben Host wie die Broker
  (Podman-Container, lokales Netzwerk). Die gemessene Latenz spiegelt daher
  Broker- und Client-Bibliotheks-Overhead wider, nicht Netzwerklatenz in
  einer verteilten Produktivumgebung. Fuer die Einordnung der Ergebnisse in
  Kapitel 7.3 (Grenzen des PoC) relevant.
- Einzelner Producer/Consumer pro Testlauf, keine Nebenlast durch andere
  Prozesse. Realistische Produktionslast (mehrere Producer/Consumer,
  Hintergrundlast) wird hier nicht abgebildet.

## Testablauf

Fuer einen methodisch konsistenten Vergleich werden alle drei Technologien
mit derselben Vorgehensweise gemessen: 50 ms Abstand zwischen den
Nachrichten, kein Client-seitiges Batching. Dafuer die `*_producer_delayed.py`-
Varianten verwenden, nicht die urspruenglichen `*_producer.py` ohne
Verzoegerung (Hintergrund siehe Abschnitt "Kontrolltest" unten). Die
Consumer bleiben in allen drei Faellen unveraendert.

Pro Technologie, in dieser Reihenfolge, in zwei Terminals:

```bash
# Terminal 1: Consumer zuerst starten
python rabbitmq_consumer.py
# Ausgabe "Bereit. ... Jetzt den Producer starten." abwarten

# Terminal 2: erst dann den verzoegerten Producer starten
python rabbitmq_producer_delayed.py
```

Gleiches Muster fuer `kafka_producer_delayed.py`/`kafka_consumer.py` und
`ibmmq_producer_delayed.py`/`ibmmq_consumer.py`. Die Zusammenfassung
erscheint im Consumer-Terminal, sobald alle 110 Nachrichten (10 Warmup +
100 Messung) verarbeitet sind. Bei 50 ms Abstand dauert ein Durchlauf rund
5,5 Sekunden.

Vor jedem Testlauf: RabbitMQ-Queue und IBM-MQ-Queue leeren (siehe
Haupt-README), damit keine Nachrichten aus vorherigen Laeufen mitgezaehlt
werden. Kafka nutzt eine zeitstempelbasierte Consumer Group pro Lauf, das
ist dort nicht zwingend noetig.

## Ergebnisse

| Technologie | Min (ms) | Max (ms) | Mittel (ms) | Median (ms) | P95 (ms) | P99 (ms) | Stdabw (ms) |
|---|---|---|---|---|---|---|---|
| RabbitMQ (ohne Verzoegerung, verworfen) | 0.97 | 1.66 | 1.42 | 1.50 | 1.65 | 1.65 | 0.21 |
| RabbitMQ (mit 50ms Verzoegerung, gueltig) | 0.22 | 0.67 | 0.41 | 0.43 | 0.54 | 0.58 | 0.09 |
| Kafka (ohne Verzoegerung, verworfen) | 3.56 | 3.57 | 3.56 | 3.56 | 3.57 | 3.57 | 0.01 |
| Kafka (mit 50ms Verzoegerung, gueltig) | 0.49 | 1.71 | 0.81 | 0.80 | 1.04 | 1.19 | 0.16 |
| IBM MQ (ohne Verzoegerung, vorlaeufig) | 0.05 | 0.50 | 0.22 | 0.17 | 0.42 | 0.48 | 0.14 |
| IBM MQ (mit 50ms Verzoegerung, gueltig) | 0.20 | 0.59 | 0.30 | 0.30 | 0.38 | 0.47 | 0.05 |

Fuer den finalen Vergleich in Kapitel 6.3 werden ausschliesslich die drei
Zeilen "mit Verzoegerung" verwendet. Rangfolge nach Mittelwert: IBM MQ
(0.30 ms) vor RabbitMQ (0.41 ms) vor Kafka (0.81 ms). Alle drei bewegen
sich in derselben Groessenordnung (unter 1 ms), die relativen Unterschiede
(Faktor 2 bis 2.7 zwischen schnellstem und langsamstem System) sind jedoch
konsistent und nicht durch Messrauschen erklaerbar (Stdabw jeweils deutlich
kleiner als der Abstand zwischen den Mittelwerten).

## Interpretation (vorlaeufig, vor Aufnahme in Kapitel 7.1 pruefen)

Der Kontrolltest bestaetigt den Verdacht: Ohne Verzoegerung und ohne
explizites `flush()` je Nachricht puffert der Kafka-Producer-Client
mehrere Nachrichten und sendet sie gebuendelt. Die urspruenglich gemessene,
fast konstante Latenz von 3.56 ms (Stdabw 0.01 ms) spiegelte diesen
Batching- bzw. Flush-Mechanismus wider, nicht die tatsaechliche
Zustellzeit einzelner Nachrichten. Nach Einfuegen von Verzoegerung und
explizitem `flush()` je Nachricht sinkt der Mittelwert auf 0.81 ms bei
einer deutlich realistischeren Streuung (Stdabw 0.16 ms), vergleichbar mit
RabbitMQ und IBM MQ.

Methodische Konsequenz fuer Kapitel 6.2: Bei Kafka muss die Messmethode
(Verzoegerung zwischen Nachrichten, expliziter Flush) explizit dokumentiert
werden, da naive Messungen sonst client-seitige Batching-Effekte statt
Systemlatenz abbilden. Das ist selbst ein inhaltlich relevanter Befund,
kein reines Mess-Detail: er zeigt, dass Kafkas Producer-Client per Default
auf Durchsatz statt Einzel-Latenz optimiert ist, waehrend RabbitMQ und IBM
MQ pro Aufruf synchron senden. Relevant fuer die Diskussion in Kapitel 7.2
(geeignete Anwendungsfaelle: Kafka fuer hohen Durchsatz, nicht fuer
Szenarien mit Anspruch auf niedrige Einzelnachrichten-Latenz).

Ueberraschender Zusatzbefund: Auch RabbitMQ zeigt einen deutlichen
Unterschied zwischen Burst-Versand ohne Verzoegerung (1.42 ms Mittelwert)
und Versand mit Abstand (0.41 ms Mittelwert) – in die entgegengesetzte
Richtung wie bei Kafka (dort sank der Wert ebenfalls, aber von einem
hoeheren Ausgangswert). Ohne Verzoegerung sendet `pika`s
`BlockingConnection` viele TCP-Schreibvorgaenge unmittelbar hintereinander,
was vermutlich zu Stau-Effekten auf Verbindungsebene fuehrt. IBM MQ zeigt
dieses Verhalten praktisch nicht (0.22 ms vs. 0.30 ms, Unterschied im
Rahmen der Messgenauigkeit). Schlussfolgerung: Burst-ohne-Pause ist als
Testmethode fuer Einzelnachrichten-Latenz grundsaetzlich ungeeignet, nicht
nur bei Kafka. Das rechtfertigt zusaetzlich, warum in Kapitel 6.2 die
Messmethode mit Verzoegerung als Standardvorgehen begruendet werden sollte,
nicht nur als Kafka-spezifische Ausnahme.

IBM MQ zeigt in der finalen, konsistenten Messung weiterhin die niedrigsten
und stabilsten Werte (kleinste Standardabweichung), erklaerbar durch das
schlanke, binaere MQI-Protokoll und eine einfache Put/Get-Operation ohne
Acknowledgement-Handshake wie bei RabbitMQ (`basic_ack`). Kafka liegt am
langsamsten, konsistent mit seinem Design fuer Durchsatz und Replay statt
niedriger Einzel-Latenz.