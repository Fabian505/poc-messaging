# UC2-Ressourcenauswertung (nvme0n1)

Verwendete Blockgrenzen (Sekunden ab erstem Client-Prozess, manuell per --bounds bestaetigt): [(1, 0, 271), (2, 272, 302), (4, 303, 456), (8, 457, 613)]

iostat-Block 1 (Mittelwert seit Systemstart) wurde verworfen.

| Instanzen | Sek | CPU-idle Ø | w/s Ø | w/s Max | %util Ø | %util Max | Kafka-CPU Ø | Kafka-CPU Max | Kafka-RAM Ø MB | Kafka-RAM Max MB |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 272 | 97.3% | 99.8 | 1300.0 | 1.8% | 16.4% | 25.8% | 56.7% | 984 | 1039 |
| 2 | 31 | 97.2% | 109.7 | 878.0 | 1.9% | 14.4% | 18.1% | 18.4% | 993 | 1034 |
| 4 | 154 | 96.5% | 113.5 | 1832.0 | 1.9% | 15.6% | 18.7% | 19.4% | 1005 | 1035 |
| 8 | 157 | 95.5% | 114.2 | 2036.0 | 1.9% | 15.2% | 20.2% | 21.5% | 1017 | 1032 |
