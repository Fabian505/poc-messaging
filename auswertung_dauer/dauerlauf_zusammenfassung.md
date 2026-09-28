# Dauerlauf-Zusammenfassung (UC1, 1.000.000 Nachrichten x 3)

Erstellt: 2026-09-28T20:40:38

## 1. Log-Check

Keine Fehler-/Warnzeilen gefunden.


## 2. Berichte je Wiederholung

| Tech | Wdh | Soll | Rate | Rate/Soll | Verpasste Slots | Median ms | P95 | P99 | Max | Drift ms | SD Mittel | SD Median | SD P99 | Dauer s* |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kafka | 1 | 500 | 500.0 | 100.0 % | 0.0 % | 0.07 | 0.10 | 0.13 | 48.84 | 0.00 | 0.05 | 0.05 | 0.09 | 2007 |
| kafka | 2 | 500 | 500.0 | 100.0 % | 0.0 % | 0.07 | 0.10 | 0.13 | 7.05 | -0.00 | 0.05 | 0.05 | 0.09 | 2007 |
| kafka | 3 | 500 | 500.0 | 100.0 % | 0.0 % | 0.07 | 0.10 | 0.13 | 3.52 | 0.00 | 0.05 | 0.05 | 0.09 | 2006 |
| rabbitmq | 1 | 300 | 296.0 | 98.7 % | 14.5 % | 0.13 | 0.16 | 0.19 | 1.38 | -0.00 | 2.97 | 2.82 | 3.93 | 3379 |
| rabbitmq | 2 | 300 | 296.2 | 98.7 % | 14.3 % | 0.12 | 0.16 | 0.19 | 2.15 | -0.00 | 2.97 | 2.82 | 3.92 | 3378 |
| rabbitmq | 3 | 300 | 289.9 | 96.6 % | 14.2 % | 0.12 | 0.16 | 0.20 | 2.09 | 0.00 | 3.04 | 2.82 | 3.94 | 3452 |
| ibmmq | 1 | 130 | 126.3 | 97.2 % | 1.1 % | 3.79 | 6.45 | 114.89 | 5928.60 | 12.02 | 4.23 | 3.70 | 8.16 | 7918 |
| ibmmq | 2 | 130 | 126.3 | 97.2 % | 1.1 % | 3.79 | 6.46 | 180.24 | 8263.71 | -0.62 | 4.23 | 3.70 | 8.41 | 7921 |
| ibmmq | 3 | 130 | 126.0 | 96.9 % | 1.1 % | 3.79 | 6.45 | 162.55 | 9793.54 | -0.86 | 4.24 | 3.70 | 8.36 | 7938 |

*Dauer = Bericht-Zeitstempel minus Start der Vorwiederholung (bei Wdh. 1: Monitor-Start), enthaelt Start/Ende-Overhead von einigen Sekunden. Verpasste Slots = Producer-Intervalle im Rueckstand, bezogen auf 1.000.010 Nachrichten.

## 3. Platte und CPU (iostat) je Wiederholung

| Tech | Wdh | Sekunden | w_await Mittel | P95 | Max | f_await Mittel | w/s | wkB/s | Schreib-IOs/Nachr. | kB/Nachr. | %util | CPU idle % |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kafka | 1 | 2006 | 0.51 | 0.75 | 4.47 | 1.88 | 32.1 | 405.8 | 0.06 | 0.8 | 0.9 | 99.6 |
| kafka | 2 | 2007 | 0.52 | 0.76 | 8.10 | 1.89 | 29.8 | 367.8 | 0.06 | 0.7 | 0.9 | 99.2 |
| kafka | 3 | 2006 | 0.50 | 0.65 | 5.33 | 1.88 | 29.1 | 358.0 | 0.06 | 0.7 | 0.8 | 99.4 |
| rabbitmq | 1 | 3378 | 0.38 | 0.41 | 1.30 | 1.90 | 2414.8 | 26582.0 | 8.16 | 89.8 | 83.4 | 94.1 |
| rabbitmq | 2 | 3378 | 0.38 | 0.41 | 1.02 | 1.90 | 2412.7 | 26542.3 | 8.15 | 89.6 | 83.3 | 94.1 |
| rabbitmq | 3 | 3452 | 0.50 | 0.53 | 48.25 | 2.22 | 2361.6 | 25987.0 | 8.15 | 89.6 | 83.5 | 94.1 |
| ibmmq | 1 | 7917 | 0.74 | 2.99 | 216.08 | 2.31 | 3780.7 | 48811.1 | 29.93 | 386.5 | 87.2 | 94.1 |
| ibmmq | 2 | 7921 | 0.68 | 2.96 | 187.43 | 2.31 | 3780.2 | 48812.3 | 29.93 | 386.5 | 87.2 | 94.1 |
| ibmmq | 3 | 7938 | 0.72 | 3.03 | 116.26 | 2.34 | 3761.7 | 48532.9 | 29.85 | 385.2 | 87.2 | 94.0 |

Schreib-IOs und kB je Nachricht = w/s bzw. wkB/s des ganzen Geraets geteilt durch die Rate der Wiederholung (enthaelt Hintergrundlast des Systems, siehe Kafka als Grundrauschen).

**Schreibpausen** (Sekunden mit w/s unter 10 % des Mittels der Wiederholung; sichtbar sind nur Stalls, in denen das Geraet still war - ein reiner Consumer-Stall waehrend der Producer weiterschreibt faellt hier nicht auf; bei Kafka nicht aussagekraeftig, weil es ohne fsync gebuendelt im Hintergrund schreibt):

| Tech | Wdh | Sekunden | Episoden | laengste (s) | erste Zeitpunkte |
|---|---|---|---|---|---|
| kafka | 1 | 0 | 0 | 0 | - |
| kafka | 2 | 0 | 0 | 0 | - |
| kafka | 3 | 0 | 0 | 0 | - |
| rabbitmq | 1 | 0 | 0 | 0 | - |
| rabbitmq | 2 | 0 | 0 | 0 | - |
| rabbitmq | 3 | 30 | 5 | 8 | 06:46:48 (8 s), 07:06:04 (8 s), 07:31:39 (8 s), 07:39:38 (3 s) |
| ibmmq | 1 | 113 | 52 | 8 | 07:55:05 (1 s), 07:55:08 (2 s), 07:55:11 (1 s), 07:59:20 (2 s) |
| ibmmq | 2 | 117 | 51 | 9 | 09:58:09 (1 s), 09:58:12 (2 s), 09:58:15 (1 s), 10:02:44 (2 s) |
| ibmmq | 3 | 78 | 38 | 9 | 12:09:16 (2 s), 12:14:09 (1 s), 12:14:11 (3 s), 12:18:53 (1 s) |

**Abgleich IBM MQ Fehlerlog (AMQ6729W = Log-Schreibvorgang > 1 s) mit iostat** (Toleranz +-10 s):

| Wdh | AMQ-Warnungen | davon mit Schreibpause | Pausen >= 3 s | davon mit Warnung | w_await ms (Pausen mit Warnung) | w_await ms (Pausen ohne Warnung) |
|---|---|---|---|---|---|---|
| 1 | 7 | 7 | 11 | 3 | 5.65 | 4.04 |
| 2 | 3 | 3 | 18 | 3 | 4.54 | 4.10 |
| 3 | 7 | 7 | 7 | 2 | 6.06 | 4.48 |

w_await = Median der Sekundenmittel waehrend der Pausen. Hoch (deutlich ueber dem Wiederholungsmittel) = die wenigen Schreibvorgaenge waren langsam (Geraet); niedrig = es wurde kaum etwas geschrieben (Broker/Clients haben nichts abgeschickt).

**Verlauf innerhalb der Wiederholung** (Anfang / Mitte / Ende; w_await / f_await in ms, dann Schreiboperationen/s):

| Tech | Wdh | w_await A | w_await M | w_await E | w/s A | w/s M | w/s E |
|---|---|---|---|---|---|---|---|
| kafka | 1 | 0.51 / 1.87 | 0.52 / 1.87 | 0.52 / 1.88 | 33.0 | 33.2 | 30.2 |
| kafka | 2 | 0.54 / 1.91 | 0.51 / 1.88 | 0.50 / 1.87 | 31.2 | 27.9 | 30.2 |
| kafka | 3 | 0.50 / 1.88 | 0.50 / 1.88 | 0.50 / 1.87 | 27.9 | 31.2 | 28.3 |
| rabbitmq | 1 | 0.38 / 1.90 | 0.38 / 1.90 | 0.38 / 1.90 | 2413.1 | 2417.8 | 2413.5 |
| rabbitmq | 2 | 0.38 / 1.90 | 0.38 / 1.90 | 0.38 / 1.90 | 2406.3 | 2417.0 | 2414.7 |
| rabbitmq | 3 | 0.50 / 2.22 | 0.47 / 2.23 | 0.52 / 2.22 | 2359.6 | 2360.2 | 2365.1 |
| ibmmq | 1 | 0.56 / 2.24 | 0.93 / 2.36 | 0.73 / 2.33 | 3797.5 | 3772.5 | 3772.2 |
| ibmmq | 2 | 0.60 / 2.28 | 0.76 / 2.33 | 0.68 / 2.32 | 3791.2 | 3772.4 | 3777.0 |
| ibmmq | 3 | 0.73 / 2.38 | 0.71 / 2.31 | 0.73 / 2.31 | 3751.2 | 3766.2 | 3767.6 |

## 4. Clientprozesse (psutil)

| Tech | Wdh | Prozess | PID | CPU % Mittel | P99 | Max | RSS MB Anfang | RSS MB Ende | RSS MB Max |
|---|---|---|---|---|---|---|---|---|---|
| kafka | 1 | Client 1 (Consumer?) | 282605 | 1.5 | 3.0 | 3.0 | 28 | 55 | 62 |
| kafka | 1 | Client 2 (Producer?) | 282750 | 0.8 | 2.0 | 2.0 | 28 | 55 | 62 |
| kafka | 2 | Client 1 (Consumer?) | 325176 | 1.5 | 2.0 | 3.0 | 28 | 55 | 62 |
| kafka | 2 | Client 2 (Producer?) | 325311 | 0.8 | 2.0 | 2.0 | 28 | 55 | 61 |
| kafka | 3 | Client 1 (Consumer?) | 367719 | 1.5 | 3.0 | 3.0 | 29 | 55 | 62 |
| kafka | 3 | Client 2 (Producer?) | 367852 | 0.8 | 2.0 | 2.0 | 28 | 55 | 61 |
| rabbitmq | 1 | Client 1 (Consumer?) | 412479 | 2.1 | 3.0 | 4.0 | 32 | 59 | 65 |
| rabbitmq | 1 | Client 2 (Producer?) | 412507 | 2.1 | 3.0 | 4.0 | 32 | 59 | 65 |
| rabbitmq | 2 | Client 1 (Consumer?) | 484535 | 2.0 | 3.0 | 4.0 | 32 | 58 | 65 |
| rabbitmq | 2 | Client 2 (Producer?) | 484547 | 2.1 | 3.0 | 4.0 | 32 | 59 | 65 |
| rabbitmq | 3 | Client 1 (Consumer?) | 556508 | 2.0 | 3.0 | 4.0 | 32 | 58 | 65 |
| rabbitmq | 3 | Client 2 (Producer?) | 556534 | 2.0 | 3.0 | 4.0 | 32 | 59 | 65 |
| ibmmq | 1 | Client 1 (Consumer?) | 633005 | 0.8 | 2.0 | 2.0 | 32 | 59 | 66 |
| ibmmq | 1 | Client 2 (Producer?) | 633010 | 0.8 | 2.0 | 2.0 | 32 | 59 | 66 |
| ibmmq | 2 | Client 1 (Consumer?) | 800974 | 0.7 | 2.0 | 2.0 | 32 | 59 | 66 |
| ibmmq | 2 | Client 2 (Producer?) | 800983 | 0.8 | 2.0 | 2.0 | 32 | 59 | 66 |
| ibmmq | 3 | Client 1 (Consumer?) | 973917 | 0.7 | 2.0 | 2.0 | 32 | 59 | 66 |
| ibmmq | 3 | Client 2 (Producer?) | 973932 | 0.8 | 2.0 | 2.0 | 32 | 59 | 66 |

Rollenzuordnung nach PID-Reihenfolge (Consumer wird vor dem Producer gestartet); die Kommandozeile ist im Monitor auf 60 Zeichen gekuerzt und nicht unterscheidbar. CPU % ist pro Kern (100 % = ein voller Kern).

## 5. Container (podman stats, best effort)

| Container | Wdh | CPU % Mittel | CPU % Max | Mem MB Anfang | Mem MB Ende | Mem MB Max |
|---|---|---|---|---|---|---|
| kafka | 1 | 1.3 | 1.7 | 988 | 1051 | 1068 |
| kafka | 2 | 2.0 | 2.2 | 1087 | 1150 | 1167 |
| kafka | 3 | 2.4 | 2.6 | 1170 | 1228 | 1231 |
| rabbitmq | 1 | 3.5 | 47.3 | 143 | 142 | 189 |
| rabbitmq | 2 | 2.8 | 2.9 | 142 | 142 | 147 |
| rabbitmq | 3 | 2.8 | 2.8 | 142 | 142 | 147 |
| ibmmq | 1 | 2.8 | 9.6 | 126 | 126 | 128 |
| ibmmq | 2 | 2.7 | 2.7 | 126 | 126 | 128 |
| ibmmq | 3 | 2.7 | 2.7 | 126 | 126 | 128 |

Alle Schreibpausen-Episoden: auswertung_dauer/dauerlauf_zusammenfassung_schreibpausen.csv
