"""Gemeinsame Statistik-Auswertung fuer alle Latenz-Consumer.

Separat gehalten, damit die Berechnung (Min/Max/Mittelwert/Median/Perzentile)
nicht drei Mal leicht unterschiedlich implementiert wird.
"""

import statistics


def print_latency_summary(latencies_seconds, technology_name):
    if not latencies_seconds:
        print("Keine Messwerte vorhanden.")
        return

    # Drift: Mittel des letzten minus des ersten Zehntels in ANKUNFTSreihenfolge.
    # Nahe 0 = stabil. Deutlich positiv = Warteschlange baut sich im Lauf auf,
    # d.h. die Soll-Last liegt ueber der Verarbeitungskapazitaet (Saettigung).
    arrival_ms = [l * 1000 for l in latencies_seconds]
    tenth = max(1, len(arrival_ms) // 10)
    drift = statistics.mean(arrival_ms[-tenth:]) - statistics.mean(arrival_ms[:tenth])

    latencies_ms = sorted(arrival_ms)
    n = len(latencies_ms)

    def percentile(p):
        idx = min(int(round(p / 100 * (n - 1))), n - 1)
        return latencies_ms[idx]

    print(f"--- Latenz-Zusammenfassung: {technology_name} ---")
    print(f"Anzahl Messwerte: {n}")
    print(f"Min:    {min(latencies_ms):.2f} ms")
    print(f"Max:    {max(latencies_ms):.2f} ms")
    print(f"Mittel: {statistics.mean(latencies_ms):.2f} ms")
    print(f"Median: {statistics.median(latencies_ms):.2f} ms")
    print(f"P95:    {percentile(95):.2f} ms")
    print(f"P99:    {percentile(99):.2f} ms")
    if n > 1:
        print(f"Stdabw: {statistics.stdev(latencies_ms):.2f} ms")
    print(f"Drift:  {drift:.2f} ms")
