# PoC Messaging-Loesungen: Setup

## 1. Host vorbereiten (Fedora)

```bash
sudo dnf install -y podman podman-compose podman-docker
mkdir -p rabbitmq/data kafka/data ibmmq/data
```

Podman legt fehlende Bind-Mount-Verzeichnisse teils nicht automatisch an;
`mkdir -p` vorher vermeidet Startfehler.

## 2. Container starten

```bash
podman-compose up -d
podman ps
```

Falls ein Service nicht hochkommt (haeufigster Kandidat: ibmmq wegen RAM):

```bash
podman-compose logs -f ibmmq
```

Zum Stoppen: `podman-compose down` (Daten bleiben in den `./*/data`-Ordnern
erhalten, solange du die Verzeichnisse nicht loeschst).

## 3. Python-Umgebung fuer die Client-Skripte

```bash
cd clients
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### IBM MQ Client fuer pymqi

`pymqi` ist nur ein Python-Wrapper. Er braucht die IBM MQ C-Client-
Bibliotheken auf dem Host, sonst schlaegt schon `pip install pymqi`
oder spaetestens der Import fehl. Zwei Wege:

1. **Redistributable MQ Client** von IBM herunterladen (kostenlos,
   Registrierung noetig) und die Umgebungsvariablen `MQ_INSTALLATION_PATH`
   entsprechend setzen, bevor du `pip install pymqi` ausfuehrst.
2. Alternativ die Bibliotheken aus dem laufenden Container kopieren:
   ```bash
   podman cp ibmmq:/opt/mqm/lib64 ./mqm-lib64
   export LD_LIBRARY_PATH=$PWD/mqm-lib64:$LD_LIBRARY_PATH
   ```
   Das ist fragiler (Versionsabhaengigkeit), aber fuer einen lokalen
   PoC ohne Registrierung ausreichend.

Dokumentiere in Kapitel 5.1, welchen Weg du gewaehlt hast.

## 4. Smoke-Test je Technologie

```bash
# RabbitMQ
python rabbitmq_consumer.py &   # laeuft im Hintergrund
python rabbitmq_producer.py

# Kafka
python kafka_consumer.py &
python kafka_producer.py

# IBM MQ
python ibmmq_producer.py
python ibmmq_consumer.py
```

Bei RabbitMQ und Kafka Consumer vor dem Producer starten (Consumer
blockiert und wartet). Bei IBM MQ ist die Reihenfolge egal, da die
Queue Nachrichten persistent puffert.

## 5. Web-UIs zur Kontrolle

- RabbitMQ Management: http://localhost:15672 (guest/guest)
- IBM MQ Web Console: https://localhost:9443 (admin/passw0rd,
  selbstsigniertes Zertifikat, Browser-Warnung ignorieren)
- Kafka hat keine mitgelieferte Web-UI; fuer manuelle Inspektion notfalls
  `podman exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`

## Offene Punkte fuer die Arbeit

- Ressourcenverbrauch messen (RAM/CPU je Container) fuer die
  Komplexitaets-Bewertung in Kapitel 6.1.4
- Welchen pymqi-Setup-Weg gewaehlt und warum, in 5.1 dokumentieren
- Diese Single-Node-Konfiguration als Einschraenkung in 7.3 benennen
