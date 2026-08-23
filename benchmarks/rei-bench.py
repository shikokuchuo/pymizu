"""Report-only benchmark: the pyrei channel and pool, the Python mirror of
rei's dev/bench/rei-mirai.R (without the mirai/nanonext comparison rows).
Prints each number as it lands and a summary table at the end; asserts
nothing.

  1. sequential round-trip  const task, 1 worker: submit + collect loop
  2. pipelined throughput   const task, 1 worker: fire n, collect n;
                            channel and pool each carry a batched row
  3. payload round-trip     identity task on float64 vectors of 8 KB /
                            800 KB / 8 MB, 1 worker: data both ways.
                            Slots are sized to the payload where the 2^20
                            slot_size cap allows, so 8 KB and 800 KB ride
                            in-slot (RAWVEC) and 8 MB takes the spill tier
                            (a RAWSPILL region per payload)
  4. parallel fan-out       small compute tasks, 4 workers: fire all,
                            collect all (in-process loop as the anchor)
  5. streaming              one-way const messages: channel send_batch /
                            recv_batch
  6. parallel map           Pool.map of bench_sum over 2000 elements,
                            4 workers (in-process loop as the anchor)

Timings are best-of-3 after warm-up; single runs on a busy machine still
jitter.

Run from the repo root (the workers inherit it as their working directory,
which is what makes benchmarks.tasks importable to them):

  python benchmarks/rei-bench.py
"""

import os
import subprocess
import sys
import time
from functools import partial

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import numpy as np

import pyrei
from benchmarks.tasks import bench_sum, const, identity

REPS = 3
results = []


def note(scenario, framework, value, unit):
    print(f"  {framework:<20} {value:>15,.1f} {unit}", flush=True)
    results.append((scenario, framework, value, unit))


def _timed(f):
    t0 = time.perf_counter_ns()
    f()
    return (time.perf_counter_ns() - t0) / 1e6


def best_ms(f):
    return min(_timed(f) for _ in range(REPS))


def note_us(scenario, framework, ops, f, unit="us/task"):
    note(scenario, framework, best_ms(f) * 1000 / ops, unit)


def note_rate(scenario, framework, ops, f, unit="tasks/s"):
    note(scenario, framework, ops / best_ms(f) * 1000, unit)


def warmup(f, n=200):
    for _ in range(n):
        f()


def pipeline(fire, reap, n):
    ts = [fire() for _ in range(n)]
    for t in ts:
        reap(t)


def worker_launcher(token, slot):
    # cwd pins the workers' sys.path[0] to the repo root, wherever the
    # bench itself was invoked from
    return subprocess.Popen(
        [sys.executable, "-m", "pyrei.worker", token, str(slot)], cwd=ROOT
    )


def with_pool(workers, f, **args):
    p = pyrei.Pool.create(
        workers, max_submitters=2, launcher=worker_launcher, **args
    )
    try:
        f(p)
    finally:
        p.stop(timeout=15)


def with_channel(source, f, **args):
    ch = pyrei.Channel.create(source, **args)
    try:
        f(ch)
    finally:
        ch.close(timeout=10)


ECHO_PEER = """
import pyrei
while True:
    x = ch.recv()
    if x is pyrei.CLOSED or x is pyrei.PEER_GONE:
        break
    ch.send(x)
"""

BATCH_PEER = """
import pyrei
while True:
    xs = ch.recv_batch(4096)
    if xs is pyrei.CLOSED or xs is pyrei.PEER_GONE:
        break
    ch.send_batch(xs)
"""

print(f"pyrei {pyrei.__version__} | core {pyrei.__core_version__} | "
      f"Python {sys.version.split()[0]} | {sys.platform} {os.uname().machine}")

# 1. sequential round-trip -----------------------------------------------------

print("\n== 1. sequential round-trip (const task, 1 worker) ==")
n = 2000

nc = 20000   # channel rt is us-scale: run long

def seq_channel(ch):
    warmup(lambda: (ch.send(1), ch.recv(timeout=30)))
    def rep():
        for _ in range(nc):
            ch.send(1)
            ch.recv(timeout=30)
    note_us("sequential rt", "pyrei channel", nc, rep, "us/rt")

with_channel(ECHO_PEER, seq_channel, capacity=1024)

def seq_pool(p):
    warmup(lambda: p.submit(const).collect(timeout=30))
    def rep():
        for _ in range(n):
            p.submit(const).collect(timeout=30)
    note_us("sequential rt", "pyrei pool", n, rep)

with_pool(1, seq_pool)

# 2. pipelined throughput ------------------------------------------------------

print("\n== 2. pipelined throughput (const task, 1 worker) ==")
n = 10000
k = 10   # cycles per rep

def pipe_channel(ch):
    warmup(lambda: (ch.send(1), ch.recv(timeout=30)))
    def rep():
        for _ in range(k):
            for _ in range(n):
                ch.send(1)
            for _ in range(n):
                ch.recv(timeout=30)
    note_rate("pipelined", "pyrei channel", k * n, rep, "rt/s")

with_channel(ECHO_PEER, pipe_channel)

def pipe_channel_batch(ch):
    batch = [1] * 4096
    def stream(m):
        sent = 0
        while sent < m:
            want = min(4096, m - sent)
            sent += ch.send_batch(batch[:want])
        got = 0
        while got < m:
            xs = ch.recv_batch(4096, timeout=30)
            if pyrei.is_sentinel(xs):
                raise RuntimeError("pyrei channel batch: peer stopped echoing")
            got += len(xs)
    warmup(lambda: stream(100))
    note_rate("pipelined", "pyrei channel batch", k * n,
              lambda: [stream(n) for _ in range(k)], "rt/s")

with_channel(BATCH_PEER, pipe_channel_batch)

# a submitter's outstanding tasks are bounded by its result-slot share, so
# fire-n-then-collect needs result_slots / max_submitters >= n
def pipe_pool(p):
    fire = lambda: p.submit(const)
    reap = lambda t: t.collect(timeout=30)
    warmup(lambda: reap(fire()))
    note_rate("pipelined", "pyrei pool", n, lambda: pipeline(fire, reap, n))

with_pool(1, pipe_pool, result_slots=20480)

# the batch pair: one submit crossing + one collect crossing per burst
def pipe_pool_batch(p):
    fns = [const] * n
    warmup(lambda: p.collect_all(p.submit_batch(fns[:100], timeout=30),
                                 timeout=30))
    note_rate("pipelined", "pyrei pool batch", k * n, lambda: [
        p.collect_all(p.submit_batch(fns, timeout=30), timeout=30)
        for _ in range(k)
    ])

with_pool(1, pipe_pool_batch, result_slots=20480)

# 3. payload round-trip --------------------------------------------------------

print("\n== 3. payload round-trip (identity task on a float64 vector) ==")

# slots sized to the payload where the cap allows: 8 KB and 800 KB ride
# in-slot (RAWVEC, no spill); 8 MB exceeds 2^20 and spills on default
# slots — a fresh RAWSPILL region per payload each way
payloads = [
    (1000, 1000, dict(slot_size=16384)),
    (100000, 200, dict(injection_cap=16, per_worker_cap=16,
                       result_slots=4, slot_size=1048576)),
    (1000000, 30, dict()),
]

for size, n, args in payloads:
    x = np.random.random(size)
    label = f"payload {8 * size:,} B"

    def run(p, x=x, n=n, label=label):
        assert np.array_equal(p.submit(identity, x).collect(timeout=30), x)
        def rep():
            for _ in range(n):
                p.submit(identity, x).collect(timeout=30)
        note_us(label, "pyrei pool", n, rep)

    with_pool(1, run, **args)

# 4. parallel fan-out ----------------------------------------------------------

print("\n== 4. parallel fan-out (bench_sum x 2000, 4 workers) ==")
n = 2000

note_rate("fan-out", "in-process", n,
          lambda: [bench_sum(i) for i in range(n)])

def fanout_pool(p):
    fire = lambda: p.submit(bench_sum, 0)
    reap = lambda t: t.collect(timeout=30)
    pipeline(fire, reap, n)
    note_rate("fan-out", "pyrei pool", n, lambda: pipeline(fire, reap, n))

with_pool(4, fanout_pool)

# 5. streaming -----------------------------------------------------------------

print("\n== 5. streaming (one-way const messages, batched) ==")
n = 200000
k = 10   # rounds per rep

# the peer counts arrivals and sends one receipt per n, so the same channel
# serves the warm-up round and every rep
STREAM_PEER = """
import pyrei
total = 0
while True:
    xs = ch.recv_batch(4096)
    if xs is pyrei.CLOSED or xs is pyrei.PEER_GONE:
        break
    total += len(xs)
    if total >= 200000:
        ch.send(total)
        total = 0
"""

def stream_channel(ch):
    batch = [1] * 4096
    def stream_round():
        sent = 0
        while sent < n:
            want = min(4096, n - sent)
            sent += ch.send_batch(batch[:want])
        if ch.recv(timeout=60) != n:
            raise RuntimeError("stream count mismatch")
    stream_round()
    note_rate("streaming", "pyrei channel", k * n,
              lambda: [stream_round() for _ in range(k)], "msg/s")

with_channel(STREAM_PEER, stream_channel)

# 6. parallel map --------------------------------------------------------------

print("\n== 6. parallel map (Pool.map bench_sum x 2000, 4 workers) ==")
n = 2000

note_rate("map", "in-process", n, lambda: [bench_sum(i) for i in range(n)])

def map_pool(p):
    xs = list(range(n))
    p.map(bench_sum, xs)   # warm-up (bench_sum is stochastic: no assert)
    note_rate("map", "pyrei pool", n, lambda: p.map(bench_sum, xs))

with_pool(4, map_pool)

# summary ----------------------------------------------------------------------

print("\n== summary ==")
seen = []
for scenario, framework, value, unit in results:
    if scenario not in seen:
        seen.append(scenario)
for s in seen:
    for scenario, framework, value, unit in results:
        if scenario == s:
            print(f"  {scenario:<20} {framework:<20} {value:>15,.1f} {unit}")
