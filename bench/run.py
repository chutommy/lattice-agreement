#!/usr/bin/env python3
"""Run a group of processes on this machine, optionally with faults, time it, and
check what every process printed against the guarantees of the algorithm.

    bench/run.py links -n 3 -m 10000 --loss 0.1
    bench/run.py broadcast -n 5 -m 100000
    bench/run.py broadcast -n 5 -m 100000 --crash 2 --pause

links sends messages 1..m to every other process, broadcast broadcasts 1..m from
every process.
"""

import argparse
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

BIN = Path(__file__).resolve().parent.parent / "bin"
PORT = 11000


def make_input(program, i, n, m):
    if program == "links":
        return "".join(f"{j} {k}\n" for k in range(1, m + 1) for j in range(1, n + 1) if j != i)
    return "".join(f"{k}\n" for k in range(1, m + 1))


def expected(program, n, m):
    return {"links": (n - 1) * m, "broadcast": n * m}[program]


def check(program, n, m, outs, crashed):
    correct = [i for i in outs if i not in crashed]
    got = {j: {i: [] for i in outs} for j in outs}  # got[j][i]: messages from i printed by j
    for j, text in outs.items():
        for line in text.splitlines():
            i, k = line.split(" ")
            got[j][int(i)].append(int(k))
    for j in outs:
        for i in outs:
            ks = got[j][i]
            assert len(ks) == len(set(ks)), f"no duplication: {j} printed a message from {i} twice"
            assert all(1 <= k <= m for k in ks) and (program == "broadcast" or i != j or not ks), f"no creation: {j} printed a message {i} never sent"
            if program == "links" and i != j and i in correct and j in correct:
                assert sorted(ks) == list(range(1, m + 1)), f"reliable delivery: {j} is missing messages from {i}"
            if program == "broadcast":
                assert ks == list(range(1, len(ks) + 1)), f"FIFO order: {j} printed messages from {i} out of order"
    if program == "broadcast":
        for i in outs:
            most = max(len(got[j][i]) for j in outs)
            assert i not in correct or most == m, f"validity: messages from {i} were not all delivered"
            for j in correct:
                assert len(got[j][i]) == most, f"uniform agreement: {j} missed messages from {i} delivered elsewhere"


def run(program, n=3, m=10000, loss=0.0, pause=False, crash=0, seed=None, timeout=120):
    rng = random.Random(seed)
    tmp = Path(tempfile.mkdtemp(prefix=f"{program}-"))
    addresses = [f"127.0.0.1:{PORT + i}" for i in range(1, n + 1)]
    flags = ["--loss", str(loss)] if loss else []
    ids = range(1, n + 1)
    for i in ids:
        (tmp / f"{i}.in").write_text(make_input(program, i, n, m))

    procs = {}
    start = time.monotonic()
    for i in ids:
        with open(tmp / f"{i}.in") as stdin, open(tmp / f"{i}.out", "wb") as stdout, open(tmp / f"{i}.err", "wb") as stderr:
            procs[i] = subprocess.Popen([str(BIN / program), *flags, str(i), *addresses], stdin=stdin, stdout=stdout, stderr=stderr)
    readers = {i: open(tmp / f"{i}.out", "rb") for i in ids}
    lines = dict.fromkeys(ids, 0)
    crashed = set(rng.sample(list(ids), crash))
    crash_at = {i: start + rng.uniform(0.05, 1.0) for i in crashed}
    paused, toggle_at = None, start
    finished, checked_at, failure = None, start, None

    try:
        while True:
            time.sleep(0.01)
            now = time.monotonic()
            for i, t in crash_at.items():
                if now >= t and procs[i].poll() is None:
                    procs[i].kill()
            if pause and now >= toggle_at:
                if paused:
                    procs[paused].send_signal(signal.SIGCONT)
                    paused = None
                else:
                    paused = rng.choice([i for i in ids if i not in crashed])
                    procs[paused].send_signal(signal.SIGSTOP)
                toggle_at = now + rng.uniform(0.05, 0.3)
            for i in ids:
                lines[i] += readers[i].read().count(b"\n")
            if not crashed and all(c == expected(program, n, m) for c in lines.values()):
                finished = now - start
                break
            if crashed and now >= max(crash_at.values()) and now - checked_at > 0.5:
                # what the survivors must print depends on what the crashed printed:
                # wait until the outputs meet every guarantee
                checked_at = now
                try:
                    check(program, n, m, {i: (tmp / f"{i}.out").read_text() for i in ids}, crashed)
                    break
                except AssertionError as e:
                    failure = e
            if now - start > timeout:
                raise failure or TimeoutError(f"after {timeout} s, lines printed: {lines} (outputs in {tmp})")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGCONT)
                p.terminate()
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        for r in readers.values():
            r.close()

    outs = {i: (tmp / f"{i}.out").read_text() for i in ids}
    errors = "".join((tmp / f"{i}.err").read_text() for i in ids)
    try:
        assert not errors, errors
        check(program, n, m, outs, crashed)
    except AssertionError as e:
        raise AssertionError(f"{e} (outputs in {tmp})") from None
    shutil.rmtree(tmp)
    return finished


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("program", choices=["links", "broadcast"])
    parser.add_argument("-n", type=int, default=3, help="processes (default 3)")
    parser.add_argument("-m", type=int, default=10000, help="messages per process (default 10000)")
    parser.add_argument("--loss", type=float, default=0.0, help="fraction of datagrams dropped")
    parser.add_argument("--pause", action="store_true", help="pause and resume processes at random")
    parser.add_argument("--crash", type=int, default=0, help="processes to kill at random")
    parser.add_argument("--seed", type=int)
    a = parser.parse_args()
    if a.crash * 2 >= a.n and a.program != "links":
        parser.error(f"{a.program} needs a majority of correct processes: crash at most {(a.n - 1) // 2}")
    try:
        seconds = run(a.program, a.n, a.m, a.loss, a.pause, a.crash, a.seed)
    except (AssertionError, TimeoutError) as e:
        sys.exit(f"FAILED: {e}")
    if seconds is None:
        print("OK")
        return
    count = {"links": (a.n - 1) * a.n * a.m, "broadcast": a.n * a.m}[a.program]
    print(f"OK: {count:,} messages in {seconds:.2f} s, {count / seconds:,.0f} messages/s")


if __name__ == "__main__":
    main()
