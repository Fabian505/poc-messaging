# Vorbereitung Betreuergespräch — Use Cases der Experimentdurchführung

Stand: 24.09.2026. Für das Gespräch mit dem Betreuer, nicht Teil der
Bachelorarbeit selbst. Ziel: sicher erklären können, was jeder Use Case misst,
warum er so aufgebaut ist, was dabei schiefgehen kann, und wo eine
methodische Entscheidung ansteht.

---

## Methodische Randnotiz: Warum immer aufgewärmt wird

Vor jedem Messblock steht ein kleiner, nicht gewerteter Aufwärmlauf. Vier
unabhängige Gründe, warum das nötig ist, nicht nur einer:

1. **Kalte Betriebssystem-Caches.** Die ersten Zugriffe auf Log-/Journal-
   Dateien eines Brokers gehen auf die Platte, nicht auf den Seiten-Cache
   des Betriebssystems — erst nach ein paar Zugriffen ist der Pfad "warm".
2. **Verbindungs- und Metadaten-Aufbau.** Erste Verbindung, erste
   Consumer-Group-Zuweisung, erstes TCP-Handshake — das kostet einmalig
   Zeit, die in einer Latenzmessung sonst als Ausreißer der ersten
   Nachrichten erscheint.
3. **Persistenz-Konfiguration ändert das Verhalten strukturell**, nicht nur
   graduell (siehe UC1-Persistenzbefund) — nach jeder Konfigurationsänderung
   ist ein frischer Nachweis nötig, dass die neue Konfiguration tatsächlich
   im eingeschwungenen Zustand gemessen wird.
4. **Kafka speziell: Just-in-Time-Kompilierung der Broker-JVM.** Das ist der
   auffälligste Fall und eine eigene Lehre wert: Die erste Annahme war, ein
   Neustart brauche eine gewisse *Wartezeit*, um sich einzuschwingen — ein
   fester Sicherheitsabstand (20 Sekunden) wurde eingebaut und empirisch
   getestet. Er half NICHT: Ein Aufwärmlauf blieb trotz vorherigem Warten
   durchgehend bei ~270 ms statt ~0,8 ms. Der tatsächliche Mechanismus ist
   *Durchsatz*-basiert, nicht *zeit*-basiert: die JVM interpretiert Code
   zunächst, und erst nach einer Mindestzahl an Methodenaufrufen kompiliert
   der JIT-Compiler die heißen Pfade zu nativem Code. 3000 Nachrichten
   reichten dafür noch nicht, 5000 direkt danach schon. Diese Korrektur
   (falsche Hypothese erkannt, widerlegt, durch die richtige ersetzt) ist
   selbst ein gutes Beispiel für den iterativen Charakter der
   Experimentdurchführung.
5. **Kafka-Aufwärmung überlebt einen Technologiewechsel nicht.** Weil die
   Broker isoliert gemessen werden (die jeweils anderen zwei Container
   werden während einer Messung gestoppt), startet Kafka jedes Mal neu,
   wenn zwischendurch eine andere Technologie gemessen wird — die JVM
   verliert dabei ihren kompletten JIT-Zustand. Deshalb werden alle
   Kafka-Messungen (UC1 bis UC5) am Stück durchgeführt, mit einer
   Aufwärmung zu Beginn dieses Blocks, statt für jeden Use Case einzeln.

---



**Was gemessen wird:** Ende-zu-Ende-Latenz einer einzelnen Nachricht (ein
Producer, ein Consumer) unter konstanter, kontrollierter Last, für alle drei
Technologien in ihrer jeweils dauerhaft-persistenten Standardkonfiguration.

**Aufbau:** Producer sendet nach festem Fahrplan (Soll-Last, Standard
500 Nachrichten/s), nicht mit Pause *nach* dem Senden — sonst hängt die
tatsächliche Last von der technologiespezifischen Sendedauer ab, und die drei
Technologien wären nicht unter derselben Last gemessen. Consumer bestätigt
erst nach der Verarbeitung (at-least-once, konsistent zu allen UCs außer
UC5). Latenz = Empfangszeit minus Sendezeit pro Nachricht.

**Warum diese Last und nicht einfach maximale Last:** Ungebremstes Senden
misst nicht die Technologie, sondern die Verarbeitungsgrenze des eigenen
Python-Clients (siehe UC1-Stufenreihe). Eine kontrollierte Last unterhalb
dieser Grenze ist nötig, um überhaupt "Latenz" statt "Warteschlangenaufbau"
zu messen.

**Wichtigster Befund bisher:** IBM MQ war zunächst versehentlich nicht
persistent (Persistenz im Code gesetzt, aber nicht an den Sendeaufruf
übergeben). Persistent kostet IBM MQ ca. 0,35 ms/Nachricht (≈ 80 %) gegenüber
nicht-persistent. Kafka erreicht seine niedrige Latenz teils dadurch, dass
`acks=all` bei Replikationsfaktor 1 nur auf den Page-Cache wartet, nicht auf
einen synchronen Log-Schreibvorgang wie IBM MQ und RabbitMQ — kein reiner
Geschwindigkeitsvorteil, sondern auch ein Unterschied in der
Dauerhaftigkeitsgarantie.

**Offene Frage an den Betreuer — UC1b (Lastgrenze):** Die Stufenreihe (siehe
unten) zeigt nur die Kapazitätsgrenze der eigenen Client-Implementierung
(ein Python-Prozess, synchroner Commit pro Nachricht), nicht die des
Brokers. Um echte Broker-Lastgrenzen zu zeigen, bräuchte es entweder
Hersteller-Lastwerkzeuge (`kafka-producer-perf-test`, RabbitMQ `PerfTest`,
IBM MQ `cphtestp`) oder einen aufwendigeren eigenen Client (mehrere
Prozesse/Batches). Frage: Lohnt sich das für die Arbeit, und sind die realen
Nachrichtenraten bei DPSC überhaupt in einer Größenordnung, wo das relevant
wird?

---

## UC1-Stufenreihe — Kapazitätsgrenze der Client-Implementierung

**Was gemessen wird:** Ab welcher Soll-Last (500 → 1000 → 1500 → 2000 →
3000/s) kommt die UC1-Konfiguration (ein Producer, ein Consumer, synchron,
Bestätigung pro Nachricht) nicht mehr mit.

**Zwei unterscheidbare Engpässe:**
- **Consumer-Engpass:** Rate ≈ Soll-Last, aber die "Drift" (Latenzanstieg
  vom Anfang zum Ende des Laufs) wächst deutlich — es baut sich eine
  Warteschlange auf.
- **Producer-Engpass:** die tatsächlich erreichte Rate bleibt deutlich unter
  der Soll-Last, weil ein einzelner Sendeaufruf länger dauert als das
  Taktintervall.

**Erster Befund (Laptop, nur Diagnose, nicht final):** Bei 2000/s war
RabbitMQ am Producer begrenzt (jedes Publisher-Confirm wartet auf den
Fsync), IBM MQ nahe an seiner Consumer-Grenze. Muss auf dem Home-PC bei
höheren CPU-Kapazitäten neu eingeordnet werden.

**Wichtig für das Gespräch:** Dieses Experiment ist bewusst als Ergänzung zu
UC1 entstanden, nicht in der ursprünglichen Konzeption vorgesehen. Sollte
kurz erwähnt und in Kapitel 5 nachgetragen werden, falls es in die Arbeit
übernommen wird.

---

## UC2 — Lastverteilung (1:m, mehrere Consumer teilen sich eine Queue/Group)

**Was gemessen wird:** Durchsatz-Skalierung, wenn N Consumer-Instanzen sich
denselben Nachrichtenbestand teilen (Kafka: Partitionen einer Consumer
Group; RabbitMQ/IBM MQ: mehrere Consumer an derselben Queue).

**Aufbau:** Der Nachrichtenbestand wird VOR dem Start der Consumer komplett
vorbefüllt ("Vorbefüllung"), nicht live gesendet — die Sendezeit soll nicht
Teil der gemessenen Verarbeitungszeit sein. Gemessen wird ausschließlich das
Zeitfenster vom ersten bis zum letzten verarbeiteten Element. Kafka braucht
dafür ein Topic mit mehreren (8) Partitionen und expliziter, gleichmäßiger
Partitionierung — sonst verteilt Kafkas Standard-Partitioner ungleich.

**Kennzahlen:** Durchsatz, Speedup und Effizienz gegenüber einer Instanz,
Fairness der Verteilung (Variationskoeffizient), Vollständigkeit. Keine
Latenz — bei Vorbefüllung misst die Latenz nur, wie lange eine Nachricht im
Bestand "gewartet" hat, das sagt nichts über die Technologie.

**Befund:** Auf dem Laptop bricht die Effizienz bei acht Instanzen bei
allen drei Technologien ein — CPU-Grenze des Testrechners (Broker +
acht Python-Prozesse teilen sich dieselben Kerne), nicht ein
Technologie-Effekt. Mit simulierter Verarbeitungszeit (`PROCESSING_MS`, kein
CPU-Verbrauch, nur Wartezeit) skaliert es dagegen fast linear — das zeigt,
dass CPU-Wettbewerb die Ursache ist, nicht Koordinationsoverhead.

**Nichts offen für den Betreuer**, aber im Gespräch gut erklärbar: warum
Vorbefüllung statt Live-Senden, und warum Latenz hier keine sinnvolle
Kennzahl ist.

---

## UC3 — Event-basierte Benachrichtigung (Publish/Subscribe, 1:n Broadcast)

**Was gemessen wird:** Ob und wie schnell JEDER von S unabhängigen
Subscribern JEDE Nachricht erhält, bei fester Gesamtlast (500/s) und
wachsender Zahl an Subscribern.

**Architektonischer Kernpunkt:** RabbitMQ und IBM MQ kopieren jede Nachricht
für jeden Subscriber einzeln (RabbitMQ: eine durable Queue pro Subscriber am
Fanout-Exchange; IBM MQ: eine durable, verwaltete Subscription pro
Subscriber). Kafka schreibt einmal ins Log, jede Consumer-Group liest davon
unabhängig — die Kosten pro zusätzlichem Subscriber sollten bei Kafka daher
kaum steigen, bei den anderen beiden schon. Das ist die zentrale erwartete
Aussage dieses Use Case.

**Wichtigste Korrektur gegenüber der ersten Fassung:** Ursprünglich waren
die Subscriptions bei RabbitMQ und IBM MQ NICHT dauerhaft (exklusive Queue
bzw. non-durable Subscription). Das hätte die Persistenz-Einstellung des
Producers wirkungslos gemacht und würde bei einem Neustart eines Subscribers
Nachrichten verlieren — ein Widerspruch zur festgelegten at-least-once-
Semantik. Jetzt sind beide Subscriptions dauerhaft. Sollte im Text von
Kapitel 4/5 entsprechend stehen (falls dort "non-durable" beschrieben ist,
anpassen).

**Konfigurationsstolperstein (behoben, gehört in die Lessons Learned):**
IBM MQ verweigerte die Subscription mit Fehler 2035, weil der verwendete
Topic-String (`dev/broadcast`) auf kein eigenes Topic-Objekt abgebildet war
und dadurch auf ein Systemobjekt ohne Berechtigung zurückfiel. Nötig war ein
eigenes Topic-Objekt mit passendem Namenspräfix.

**Kennzahlen:** Vollständigkeit pro Subscriber (eindeutige Sequenznummern,
nicht nur Anzahl — sonst verschleiert ein Verlust plus ein Duplikat
denselben Zählerstand), Latenz über die Zahl der Subscriber, Versatz
zwischen den Subscribern (wie stark unterscheidet sich der Empfangszeitpunkt
derselben Nachricht), Rückstand des Producers (bei RabbitMQ wartet das
Confirm auf ALLE gebundenen Queues gleichzeitig — Kosten steigen mit S).

---

## UC4 — m:1 (mehrere Producer, ein Consumer, Aggregation)

**Was gemessen wird:** Verlustfreiheit und Erhalt der Nachrichtenreihenfolge
je einzelnem Producer, wenn M Producer gleichzeitig an einen Consumer senden
— ausdrücklich NICHT die globale Reihenfolge über alle Producer hinweg
(die ist bei nebenläufigem Senden nicht sinnvoll definierbar, siehe Kapitel
4.2.3).

**Zentrale Konstruktionsentscheidung:** Die Gesamtlast bleibt bei jeder
Producer-Zahl M konstant 500 Nachrichten/s — nicht 500×M. Jeder der M
Producer sendet im Abstand M×2 ms, versetzt zueinander, sodass die
Sendezeitpunkte lückenlos ineinandergreifen. Grund: Wäre die Last pro
Producer konstant (also Gesamtlast wachsend mit M), würde ab wenigen
Producern wieder die Client-Kapazitätsgrenze aus der UC1-Stufenreihe
gemessen, nicht der Effekt der Nebenläufigkeit selbst.

**Ergebnis (Laptop, Funktionstest mit 10.000 Nachrichten, 5 Wiederholungen):**
0 verlorene Nachrichten, 0 Duplikate, 0 Reihenfolge-Verletzungen bei allen
drei Technologien und M ∈ {1,2,4,8} — die Kerngarantie ist bestätigt.
Latenz und Fairness (Spreizung zwischen Producern) steigen mit M, am
stärksten bei Kafka; vermutlich CPU-Konkurrenz der M gleichzeitigen
Python-Prozesse auf dem Testrechner, auf dem Home-PC erneut zu prüfen.

**Nichts offen für den Betreuer.**

---

## UC5 — Entkopplung bei Ausfall und Zustellsemantiken

**Was gemessen wird:** Zwei Dinge gleichzeitig — (a) ob die drei
Zustellsemantiken (at-most-once, at-least-once, exactly-once) das tun, was
ihre Definition verspricht, unter einem gezielt herbeigeführten Ausfall, und
(b) ob und wie lange ein Producer weiterarbeiten kann, während der Consumer
oder der Broker ausfällt (Entkopplung).

**Verarbeitungsmodell:** Jede Nachricht durchläuft Zustellung → Arbeit vor
dem Seiteneffekt → Seiteneffekt (persistiert in einer kleinen SQLite-
Datenbank, überlebt also einen Absturz) → Arbeit danach → Bestätigung beim
Broker. Die Semantik bestimmt NUR, wann bestätigt wird:
- at-most-once: sofort nach Zustellung, vor jeder Arbeit
- at-least-once/exactly-once: erst nach der kompletten Verarbeitung

**Zwei Fehlerinjektionsarten:**
1. **Gezielt** (`crash-pre`/`crash-post`): Der Consumer beendet sich SELBST
   per SIGKILL bei einer festgelegten Nachricht, deterministisch entweder
   vor oder nach dem Seiteneffekt. Ergebnis ist damit für jede Kombination
   aus Szenario und Semantik exakt vorhersagbar (siehe Tabelle unten) —
   das ist die schärfste Prüfung, ob eine Garantie hält.
2. **Broker-Ausfall** (`broker-pause`/`broker-restart`): Der Broker-
   Container wird eingefroren bzw. neu gestartet, während der Producer
   sendet. Zeigt, ob und wie lange der Producer entkoppelt weiterarbeiten
   kann, und wie schnell sich jede Technologie nach dem Ausfall erholt
   ("Wiederanlaufzeit").

**Erwartungstabelle für den gezielten Absturz** (n-te Nachricht, N/3):

| Semantik | crash-pre (vor Seiteneffekt) | crash-post (nach Seiteneffekt) |
|---|---|---|
| at-most-once | 1 Nachricht verloren | kein sichtbarer Effekt |
| at-least-once | 1 erneute Zustellung | 1 Duplikat verarbeitet |
| exactly-once | 1 erneute Zustellung | 1 Duplikat erkannt, nicht verarbeitet |

Auf dem Laptop bei allen drei Technologien exakt bestätigt (18 von 18
Kombinationen).

**Wichtige Korrektur der ersten Fassung:** Ursprünglich wurde der Absturz
von AUSSEN durch den Orchestrator ausgelöst, nach einer zufälligen kurzen
Wartezeit. Da der Consumer die meiste Zeit auf die nächste Nachricht
wartet, traf der Absturz nur zufällig die Verarbeitungsphase — die
Ergebnisse waren dadurch nicht eindeutig einer Ursache zuzuordnen. Jetzt
löst sich der Consumer selbst zum garantiert richtigen Zeitpunkt aus.

**Wichtiger Befund zur Entkopplung:** Während eines Broker-Ausfalls
blockiert der Producer vollständig (nur 3–4 Nachrichten wurden während
einer 5-Sekunden-Pause "gesendet", der Rest wartet). Das bedeutet: Die
Entkopplung durch Messaging schützt vor dem Ausfall des Consumers, NICHT
vor dem Ausfall des Brokers selbst — ein einzelner Broker bleibt ein Single
Point of Failure. Für die Empfehlung an DATEV relevant: Produktiver Einsatz
bräuchte einen Cluster oder Producer-seitige Pufferung (z. B.
Outbox-Pattern), wenn Entkopplung auch bei Broker-Ausfällen gelten soll.

**Was "exactly-once" hier bedeutet:** Verarbeitung genau einmal durch einen
idempotenten Consumer (Prüfung auf bereits verarbeitete Sequenznummer, in
derselben Transaktion wie der Seiteneffekt selbst — Muster "Idempotent
Receiver"), NICHT die native Transactions-API von Kafka. Das ist ein in der
Praxis gängiger, pragmatischer Ansatz, sollte im Text aber explizit benannt
werden, um Missverständnisse zu vermeiden.

**Offene Frage an den Betreuer — Chaos-Modus:** Der gezielte Absturz zeigt,
OB eine Garantie im ungünstigsten Fall hält. Er sagt nichts darüber, WIE
OFT es im laufenden Betrieb tatsächlich zu Verlust oder Duplikaten kommt,
weil ein einzelner Absturz mitten in einem langen, störungsfreien Lauf
selten ist. Eine Ergänzung mit mehreren zufälligen Abstürzen über einen
längeren Lauf (ähnlich "Chaos Engineering"/Netflix Chaos Monkey) würde
diese Häufigkeit liefern. Frage: Ist dieser zusätzliche Aufwand für die
Arbeit gerechtfertigt, oder reicht der Nachweis "hält/hält nicht" für die
Zielsetzung aus Kapitel 4?

---

## Zusammenfassung der zwei offenen methodischen Fragen

1. **UC1b — Broker-Lastgrenzen** mit Hersteller-Lastwerkzeugen statt der
   eigenen Client-Implementierung: lohnt sich der Aufwand angesichts der
   bei DPSC realistischen Nachrichtenraten?
2. **UC5 — Chaos-Modus** (zufällige statt nur gezielte Fehlerinjektion):
   zusätzlicher Erkenntnisgewinn gegenüber dem gezielten Nachweis?

Beide Erweiterungen sind technisch vorbereitet bzw. vorbereitbar, aber noch
nicht umgesetzt — die Entscheidung hängt von Aufwand-Nutzen-Abwägung und
Praxisrelevanz für DATEV ab, nicht von technischer Machbarkeit.