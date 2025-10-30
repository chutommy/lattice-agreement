#!/usr/bin/env python3
"""Measure throughput and datagrams sent, and write bench/results.csv.

    bench/bench.py && Rscript bench/plot.R
"""

import csv
import re
import subprocess
from pathlib import Path

import run

OUT = Path(__file__).resolve().parent / "results.csv"
TRIALS = 3


def datagrams_sent():
    snmp = Path("/proc/net/snmp")
    if snmp.exists():
        header, values = [l.split() for l in snmp.read_text().splitlines() if l.startswith("Udp:")][:2]
        return int(values[header.index("OutDatagrams")])
    stats = subprocess.run(["netstat", "-s", "-p", "udp"], capture_output=True, text=True).stdout
    return int(re.search(r"(\d+) datagrams output", stats).group(1))


def measure(writer, experiment, program, n, m, fused=False, loss=0.0):
    for trial in range(TRIALS + 1):
        before = datagrams_sent()
        seconds = run.run(program, n, m, fused=fused, loss=loss, seed=trial)
        datagrams = datagrams_sent() - before
        if trial == 0:
            continue
        count, _ = run.units(program, n, m)
        writer.writerow([experiment, program + (" --fused" if fused else ""), n, loss, trial, count, f"{seconds:.3f}", datagrams])
        print(f"{experiment}: {program}{' --fused' if fused else ''} n={n} loss={loss} {count / seconds:,.0f}/s", flush=True)


def main():
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["experiment", "program", "processes", "loss", "trial", "delivered", "seconds", "datagrams"])
        for n in range(3, 10):
            for fused in (False, True):
                measure(w, "scaling", "broadcast", n, 600000 // n, fused=fused)
        for loss in (0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4):
            for fused in (False, True):
                measure(w, "loss", "broadcast", 5, 100000, fused=fused, loss=loss)
        for loss in (0.0, 0.2):
            measure(w, "loss", "links", 5, 200000, loss=loss)
            measure(w, "loss", "lattice", 5, 3000, loss=loss)


if __name__ == "__main__":
    main()
