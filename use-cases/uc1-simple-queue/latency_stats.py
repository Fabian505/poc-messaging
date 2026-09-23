"""Gemeinsame Statistik-Auswertung fuer alle Latenz-Consumer.

Separat gehalten, damit die Berechnung (Min/Max/Mittelwert/Median/Perzentile)
nicht drei Mal leicht unterschiedlich implementiert wird.
"""

import statistics


def print_latency_summary(latencies_seconds, technology_name):
    if not latencies_seconds:
        print("Keine Messwerte vorhanden.")
        return

    latencies_ms = sorted(l * 1000 for l in latencies_seconds)
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
