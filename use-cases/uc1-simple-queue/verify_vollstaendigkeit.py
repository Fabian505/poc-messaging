"""Verifikationslauf: Vollstaendigkeit der UC1-Zustellung (Anzahl Messwerte n).

KEIN Messlauf fuer den Datensatz: schreibt keinen uc1_bericht_*.md und fasst den
eingefrorenen Datenbestand nicht an. Nutzt dieselbe Mess-Mechanik wie die
UC1-Laeufe (run_single_measurement aus run_uc1_measurement.py, also dieselben
Producer-/Consumer-Skripte, dieselbe Queue-Leerung), gibt aber zusaetzlich aus,
was der Orchestrator verwirft: n = "Anzahl Messwerte" der Consumer-Zusammenfassung
(Nachrichten mit seq > 10, ohne Warmup). Bei vollstaendiger Zustellung ist n == Nachrichtenzahl.

Nutzung (aus use-cases/uc1-simple-queue/, je Technologie isoliert wie am Messtag):
    ../../run_isolated.sh kafka    python verify_vollstaendigkeit.py kafka    10000 3
    ../../run_isolated.sh rabbitmq python verify_vollstaendigkeit.py rabbitmq 10000 3
    ../../run_isolated.sh ibmmq    python verify_vollstaendigkeit.py ibmmq    10000 3
Rate: Standard 500/s (Referenzbedingung, bei RabbitMQ/IBM MQ mit Rueckstand, also der
kritischste Fall). Andere Rate z.B. per TARGET_RATE=130 vor dem Befehl.
"""
import sys
from datetime import datetime

import run_uc1_measurement as m


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in m.VALID_TECHNOLOGIES:
        print("Nutzung: python verify_vollstaendigkeit.py <kafka|rabbitmq|ibmmq> <nachrichten> <wiederholungen>")
        sys.exit(1)
    tech, count, reps = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])

    print(f"# Vollstaendigkeits-Verifikation {tech}, {datetime.now().isoformat(timespec='seconds')}, "
          f"{count} Nachrichten x {reps}")
    ok = 0
    for i in range(1, reps + 1):
        try:
            r = m.run_single_measurement(tech, count)
        except Exception as e:  # Timeout, EOF, MAXDEPTH, ... genau wie im Orchestrator
            print(f"[{tech}] Lauf {i}: ABGEBROCHEN ({type(e).__name__}: {e})")
            continue
        if r is None:
            print(f"[{tech}] Lauf {i}: Zusammenfassung nicht erkannt")
            continue
        status = "VOLLSTAENDIG" if r["n"] == count else f"UNVOLLSTAENDIG (fehlend: {count - r['n']})"
        ok += r["n"] == count
        print(f"[{tech}] Lauf {i}: n = {r['n']} von {count} erwartet -> {status} "
              f"(Rate {r['rate']} msg/s, Rueckstand {r['behind']})")
    print(f"# Ergebnis {tech}: {ok}/{reps} Laeufe vollstaendig")


if __name__ == "__main__":
    main()
