# Ablaufplan Messtag – Home-PC

Stand: 26.09.2026. Ergänzt `README_Testdurchfuehrung.md` um die UC1-Aufteilung
(Referenz/Kapazität/nicht-persistent), die Ressourcenmessung und die
Automatisierung. Ersetzt nicht die Detailerklärungen im README, nur die
Reihenfolge und die neuen Schritte.

---

## Teil 0 – Vor dem Messtag

- [ ] `git status` sauber, Commit-Hash notiert
- [ ] MAXDEPTH-Fix eingebaut (`ibmmq/config/99-poc.mqsc` gemountet), mit
      `podman restart ibmmq` + `DIS QL(DEV.QUEUE.2) MAXDEPTH` verifiziert
      (muss 1200000 zeigen, **nicht** 5000)
  - Falls noch nicht verifiziert: das zuerst erledigen, alles andere hängt daran
- [ ] IBM-MQ-Image auf Digest gepinnt (`podman image inspect ... --format '{{index .RepoDigests 0}}'`)
- [ ] `monitor_resources.sh` und `check_report.py` ins Repo gelegt (z. B. `./tools/`),
      ausführbar (`chmod +x`)
- [ ] `$DEVICE` in beiden Skripten auf die echte SSD gesetzt (`lsblk` prüfen)
- [ ] Kurzer Testlauf von `monitor_resources.sh` mit einem trivialen Befehl,
      um zu sehen, dass `iostat`, `podman stats` und der psutil-Sampler
      tatsächlich Dateien unter `messungen/resources/` erzeugen

---

## Teil A – Ausgangszustand (Messtag, B1 aus README)

```bash
conda deactivate
cd ~/…/poc-messaging && source venv/bin/activate
sudo cpupower frequency-set -g performance
powerprofilesctl set performance
podman compose up -d
./pre_measurement_check.sh
```

**Nur bei 0 FAIL weitermachen.** WARN-Punkte bewusst durchsehen (Firefox/IDE
schließen). `messumgebung_<zeitstempel>.txt` aufheben.

---

## Teil B – UC1b zuerst: Kapazitätsgrenzen ermitteln

Muss **vor** der eigentlichen UC1-Messung stehen, weil die Kapazitätswerte für
Teil C daraus kommen.

```bash
cd uc1
for tech in kafka rabbitmq ibmmq; do
  warmup=500; [ "$tech" = "kafka" ] && warmup=5000
  ../run_isolated.sh $tech python run_uc1_measurement.py $tech $warmup 1   # Aufwärmen
  # Aufwärmbericht prüfen: Rate ≈ Soll, Drift ≈ 0 — sonst wiederholen
  for r in 500 1000 1500 2000 3000; do
    TARGET_RATE=$r ../tools/monitor_resources.sh uc1b_${tech}_${r} \
      ../run_isolated.sh $tech python run_uc1_measurement.py $tech 20000 3
  done
done
```

Auswertung je Bericht (`uc1_bericht_20000n_<rate>r_*.md`) nach Teil C der
README-Tabelle (Rate ≈ Soll + Drift ≈ 0 = Stufe bewältigt; Rate < Soll = Producer
Engpass; Drift positiv bei Rate ≈ Soll = Consumer Engpass).

**Ergebnis eintragen**, letzte bewältigte Stufe minus Sicherheitsabstand:

| Technologie | Letzte bewältigte Stufe | Kapazitätswert für Teil C (≈ 85–90 %) |
|---|---|---|
| Kafka | _____ | _____ (i. d. R. = Referenz 500, kein zweiter Lauf nötig) |
| RabbitMQ | _____ | _____ |
| IBM MQ | _____ | _____ |

---

## Teil C – UC1: Referenz, Kapazität, nicht-persistent (mit Ressourcenmonitor)

Reihenfolge je Technologie, jeweils mit vorherigem Aufwärmen (falls Broker seit
Teil B neu gestartet wurde) und automatischem Report-Check nach jedem Lauf.

```bash
cd uc1
declare -A CAP=( [rabbitmq]=<aus Teil B> [ibmmq]=<aus Teil B> )

for tech in kafka rabbitmq ibmmq; do
  # Referenz, 500/s, persistent
  ../tools/monitor_resources.sh uc1_ref_${tech} \
    ../run_isolated.sh $tech python run_uc1_measurement.py $tech 10000 5
  python3 ../tools/check_report.py $(ls -t uc1_bericht_10000n_500r_*.md | head -1) 500

  if [ "$tech" != "kafka" ]; then
    cap=${CAP[$tech]}
    # Kapazität, fix unterhalb Grenze, persistent
    TARGET_RATE=$cap ../tools/monitor_resources.sh uc1_cap_${tech} \
      ../run_isolated.sh $tech python run_uc1_measurement.py $tech 10000 5
    python3 ../tools/check_report.py $(ls -t uc1_bericht_10000n_${cap}r_*.md | head -1) $cap

    # Nicht-persistent, 500/s (Sensitivitätsanalyse)
    persist_var=RABBITMQ_PERSISTENT; [ "$tech" = "ibmmq" ] && persist_var=MQ_PERSISTENT
    env ${persist_var}=0 ../tools/monitor_resources.sh uc1_nonpersist_${tech} \
      ../run_isolated.sh $tech python run_uc1_measurement.py $tech 10000 5
    python3 ../tools/check_report.py $(ls -t uc1_bericht_10000n_500r_*.md | head -1) 500
  fi
done
```

**Bei jedem FAIL des Checks:** Bericht öffnen, Ursache klären (siehe README
Teil C und D), Lauf gezielt wiederholen. Nicht blind weitermachen.

---

## Teil D – UC2 bis UC4, pro Technologie am Stück (JIT-Zustand erhalten)

Kafka **nicht** zwischen UC1/UC1b/UC2/UC3/UC4 isolieren, sonst verliert die
JVM den JIT-Zustand. Deshalb pro Technologie durchlaufen, nicht pro UC.

```bash
# Kafka: EIN Aufwärmen (bereits in Teil B/C geschehen), dann UC2-UC4 am Stück
../run_isolated.sh kafka bash -c '
  cd uc2 && python run_uc2_measurement.py kafka 100000 10 1,2,4,8
  cd ../uc3 && python run_uc3_measurement.py kafka 10000 5 1,2,4,8
  cd ../uc4 && python run_uc4_measurement.py kafka 10000 5 1,2,4,8
'
# analog fuer rabbitmq, dann ibmmq (kein JIT-Effekt, aber jeweils eigener
# kurzer Aufwaermlauf laut README B0a, falls seit Teil C ein Neustart war)
```

UC2/UC3/UC4 laufen ohne Ressourcenmonitor (bewusste Entscheidung, siehe
letzte Antwort: Aufwand-Nutzen). Nach jedem Block Bericht kurz gegen Teil C
der README prüfen (Vollständigkeit, keine Reihenfolge-Verletzungen etc.).

---

## Teil E – UC5, alle drei Technologien, ohne `run_isolated.sh`

```bash
cd ../uc5
for tech in kafka rabbitmq ibmmq; do
  systemd-inhibit --what=idle:sleep --why=Messung python run_uc5_measurement.py $tech all all 1   # Aufwärmen
  systemd-inhibit --what=idle:sleep --why=Messung python run_uc5_measurement.py $tech all all 5   # Messung
done
```

Erwartung laut README Teil C (Semantik-Matrix). Jede Abweichung ist ein
Befund, Einzellauf ansehen. Offen: Erweiterung um `consumer-outage`-Szenario
(separat abgestimmt, noch zu implementieren, nicht Teil dieses Messtags).

---

## Teil F – Über Nacht (separater Block, nicht Teil des Tagesablaufs)

```bash
./pre_measurement_check.sh   # erneut, vor dem Nachtlauf
cd uc1
for tech in kafka rabbitmq ibmmq; do
  ../run_isolated.sh $tech python run_uc1_measurement.py $tech 100000 5
done
# danach, an einem separaten Abend:
for tech in kafka rabbitmq ibmmq; do
  ../run_isolated.sh $tech python run_uc1_measurement.py $tech 1000000 3
done
```

---

## Teil G – Abschluss

```bash
mkdir -p messungen/$(date +%Y-%m-%d)
mv uc*/uc*_bericht_*.md uc*/results_*.jsonl messumgebung_*.txt \
   messungen/resources messungen/$(date +%Y-%m-%d)/
git add messungen && git commit -m "Messdaten Home-PC $(date +%Y-%m-%d)"
sudo cpupower frequency-set -g schedutil     # bzw. vorherigen Governor
```

Danach: Jeden Bericht gegen Teil C der README-Tabelle prüfen, bevor die Werte
in Kapitel 6 übernommen werden.

---

## Kurzreferenz: Bekannte Fehlerbilder

Siehe README Teil D. Ergänzend aus den letzten Sitzungen:

| Symptom | Ursache | Behebung |
|---|---|---|
| `MQRC_Q_FULL` (2053) | MAXDEPTH auf 5000 zurückgefallen | `99-poc.mqsc`-Mount prüfen, `DIS QL(DEV.QUEUE.2) MAXDEPTH` |
| UC2 RabbitMQ „Tiefe nach Vorbefüllung None“ | Skriptfehler in der Tiefenabfrage | noch offen, Code-Stelle in `run_uc2_measurement.py` prüfen |
| Governor nach Neustart wieder `powersave` | Einstellung überlebt keinen Reboot | vor jeder Sitzung neu setzen, oder dauerhaft via `tuned-adm`/systemd-Unit |
