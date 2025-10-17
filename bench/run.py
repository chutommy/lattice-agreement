#!/usr/bin/env python3
"""Run a group of processes on this machine, time it, and check what every
process printed against the guarantees of the algorithm.

    bench/run.py links -n 3 -m 10000

links sends messages 1..m to every other process.
"""

import argparse
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

BIN = Path(__file__).resolve().parent.parent / "bin"
PORT = 11000


def make_input(program, i, n, m):
    return "".join(f"{j} {k}\n" for k in range(1, m + 1) for j in range(1, n + 1) if j != i)


def expected(program, n, m):
    return (n - 1) * m


def check(program, n, m, outs):
    got = {j: {i: [] for i in outs} for j in outs}  # got[j][i]: messages from i printed by j
    for j, text in outs.items():
        for line in text.splitlines():
            i, k = line.split(" ")
            got[j][int(i)].append(int(k))
    for j in outs:
        for i in outs:
            ks = got[j][i]
            assert len(ks) == len(set(ks)), f"no duplication: {j} printed a message from {i} twice"
            assert all(1 <= k <= m for k in ks) and (i != j or not ks), f"no creation: {j} printed a message {i} never sent"
            if i != j:
                assert sorted(ks) == list(range(1, m + 1)), f"reliable delivery: {j} is missing messages from {i}"


def run(program, n=3, m=10000, timeout=60):
    tmp = Path(tempfile.mkdtemp(prefix=f"{program}-"))
    addresses = [f"127.0.0.1:{PORT + i}" for i in range(1, n + 1)]
    ids = range(1, n + 1)
    for i in ids:
        (tmp / f"{i}.in").write_text(make_input(program, i, n, m))

    procs = {}
    start = time.monotonic()
    for i in ids:
        with open(tmp / f"{i}.in") as stdin, open(tmp / f"{i}.out", "wb") as stdout, open(tmp / f"{i}.err", "wb") as stderr:
            procs[i] = subprocess.Popen([str(BIN / program), str(i), *addresses], stdin=stdin, stdout=stdout, stderr=stderr)
    readers = {i: open(tmp / f"{i}.out", "rb") for i in ids}
    lines = dict.fromkeys(ids, 0)

    try:
        while True:
            time.sleep(0.01)
            now = time.monotonic()
            for i in ids:
                lines[i] += readers[i].read().count(b"\n")
            if all(c == expected(program, n, m) for c in lines.values()):
                finished = now - start
                break
            if now - start > timeout:
                raise TimeoutError(f"after {timeout} s, lines printed: {lines} (outputs in {tmp})")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.terminate()
        for p in procs.values():
            p.wait()
        for r in readers.values():
            r.close()

    outs = {i: (tmp / f"{i}.out").read_text() for i in ids}
    errors = "".join((tmp / f"{i}.err").read_text() for i in ids)
    try:
        assert not errors, errors
        check(program, n, m, outs)
    except AssertionError as e:
        raise AssertionError(f"{e} (outputs in {tmp})") from None
    shutil.rmtree(tmp)
    return finished


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("program", choices=["links"])
    parser.add_argument("-n", type=int, default=3, help="processes (default 3)")
    parser.add_argument("-m", type=int, default=10000, help="messages per process (default 10000)")
    a = parser.parse_args()
    try:
        seconds = run(a.program, a.n, a.m)
    except (AssertionError, TimeoutError) as e:
        sys.exit(f"FAILED: {e}")
    count = (a.n - 1) * a.n * a.m
    print(f"OK: {count:,} messages in {seconds:.2f} s, {count / seconds:,.0f} messages/s")


if __name__ == "__main__":
    main()
