"""Report-only benchmark: pyrei against the stdlib concurrent.futures
executors — ProcessPoolExecutor (real parallelism, pickled payloads) and
ThreadPoolExecutor (in-process, GIL-bound for CPU work) — matched
scenario-for-scenario on one machine: the Python mirror of rei's
dev/bench/rei-mirai.R. Prints each number as it lands and a summary
table at the end; asserts nothing.

  1. sequential round-trip  const task, 1 worker: submit + collect loop
  2. pipelined throughput   const task, 1 worker: fire n, collect n
  3. payload round-trip     identity task on float64 vectors of 8 KB /
                            800 KB / 8 MB, 1 worker: data both ways.
                            pyrei slots are sized to the payload where
                            the 2^20 slot_size cap allows, so 8 KB and
                            800 KB ride in-slot and 8 MB takes the
                            zero-copy tier; cf pickles a fresh copy
                            through a pipe each way
  4. parallel fan-out       small compute tasks, 4 workers: fire all,
                            collect all (in-process loop as the anchor)
  5. parallel map           Pool.map against Executor.map, 4 workers:
                            the trivial-f overhead regime (us/elt),
                            winsum over 2000 elements as the compute
                            regime, and the skew regime (1% heavy
                            elements, ms wall). The models differ by
                            design: Executor.map chunks statically
                            (chunksize=1 here keeps it per-element, the
                            mirai_map spelling); Pool.map stages f/x
                            once and self-schedules claims off a shared
                            cursor

No streaming row: the channel has no concurrent.futures counterpart.

Timings are best-of-3 after warm-up; single runs on a busy machine still
jitter.

Run from the repo root (the workers inherit it as their working directory,
which is what makes benchmarks.tasks importable to them):

  python benchmarks/rei-stdlib-bench.py
"""

import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import numpy as np

import pyrei

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from benchmarks.tasks import (  # noqa: E402
    const,
    identity,
    plus_one,
    skewed,
    winsum,
)

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


# every cf row runs both executor flavours: processes (real parallelism,
# pickled payloads) and threads (in-process, GIL-bound for CPU work)
def with_executors(workers, f):
    for cls, label in (
        (ProcessPoolExecutor, "cf process pool"),
        (ThreadPoolExecutor, "cf thread pool"),
    ):
        with cls(max_workers=workers) as ex:
            f(ex, label)


def main():
    print(
        f"pyrei {pyrei.__version__} | core {pyrei.__core_version__} | "
        f"Python {sys.version.split()[0]} | {sys.platform} "
        f"{os.uname().machine}"
    )

    # 1. sequential round-trip ------------------------------------------------

    print("\n== 1. sequential round-trip (const task, 1 worker) ==")
    n = 2000

    def seq_pool(p):
        warmup(lambda: p.submit(const).collect(timeout=30))

        def rep():
            for _ in range(n):
                p.submit(const).collect(timeout=30)

        note_us("sequential rt", "pyrei pool", n, rep)

    with_pool(1, seq_pool)

    def seq_cf(ex, label):
        warmup(lambda: ex.submit(const).result())

        def rep():
            for _ in range(n):
                ex.submit(const).result()

        note_us("sequential rt", label, n, rep)

    with_executors(1, seq_cf)

    # 2. pipelined throughput -------------------------------------------------

    print("\n== 2. pipelined throughput (const task, 1 worker) ==")
    n = 10000

    # a submitter's outstanding tasks are bounded by its result-slot
    # share, so fire-n-then-collect needs result_slots / max_submitters
    # >= n
    def pipe_pool(p):
        def fire():
            return p.submit(const)

        def reap(t):
            return t.collect(timeout=30)

        warmup(lambda: reap(fire()))
        note_rate(
            "pipelined", "pyrei pool", n, lambda: pipeline(fire, reap, n)
        )

    with_pool(1, pipe_pool, result_slots=20480)

    def pipe_cf(ex, label):
        def fire():
            return ex.submit(const)

        def reap(t):
            return t.result()

        warmup(lambda: reap(fire()))
        note_rate("pipelined", label, n, lambda: pipeline(fire, reap, n))

    with_executors(1, pipe_cf)

    # 3. payload round-trip ---------------------------------------------------

    print("\n== 3. payload round-trip (identity task on a float64 vector) ==")

    # slots sized to the payload where the cap allows: 8 KB and 800 KB
    # ride in-slot (RAWVEC, no spill); 8 MB exceeds 2^20 and takes the
    # zero-copy tier on default slots
    payloads = [
        (1000, 1000, dict(slot_size=16384)),
        (
            100000,
            200,
            dict(
                injection_cap=16,
                per_worker_cap=16,
                result_slots=4,
                slot_size=1048576,
            ),
        ),
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

    def payload_cf(ex, label):
        ex.submit(const).result()  # warm the worker
        for size, n, _ in payloads:
            x = np.random.random(size)
            scenario = f"payload {8 * size:,} B"
            assert np.array_equal(ex.submit(identity, x).result(), x)

            def rep(x=x, n=n):
                for _ in range(n):
                    ex.submit(identity, x).result()

            note_us(scenario, label, n, rep)

    with_executors(1, payload_cf)

    # 4. parallel fan-out -----------------------------------------------------

    print("\n== 4. parallel fan-out (winsum x 2000, 4 workers) ==")
    n = 2000

    note_rate(
        "fan-out", "in-process", n, lambda: [winsum(i) for i in range(n)]
    )

    def fanout_pool(p):
        def fire():
            return p.submit(winsum, 0)

        def reap(t):
            return t.collect(timeout=30)

        pipeline(fire, reap, n)
        note_rate("fan-out", "pyrei pool", n, lambda: pipeline(fire, reap, n))

    with_pool(4, fanout_pool)

    def fanout_cf(ex, label):
        def fire():
            return ex.submit(winsum, 0)

        def reap(t):
            return t.result()

        pipeline(fire, reap, n)
        note_rate("fan-out", label, n, lambda: pipeline(fire, reap, n))

    with_executors(4, fanout_cf)

    # 5. parallel map ---------------------------------------------------------

    print("\n== 5. parallel map (f over n elements, 4 workers) ==")

    # overhead regime: trivial f, where per-element cost is the whole story
    n = 10000
    xs = np.arange(n, dtype=np.float64)
    k = 10  # map calls per rep: a trivial map is sub-ms, so loop to average

    note_us(
        "map trivial f",
        "in-process",
        k * n,
        lambda: [[plus_one(v) for v in xs] for _ in range(k)],
        "us/elt",
    )

    def map_overhead(p):
        p.map(plus_one, xs)  # warm-up
        note_us(
            "map trivial f",
            "pyrei pool",
            k * n,
            lambda: [p.map(plus_one, xs) for _ in range(k)],
            "us/elt",
        )

    with_pool(4, map_overhead)

    def map_overhead_cf(ex, label):
        list(ex.map(plus_one, xs[:200], chunksize=1))  # warm-up
        note_us(
            "map trivial f",
            label,
            n,
            lambda: list(ex.map(plus_one, xs, chunksize=1)),
            "us/elt",
        )

    with_executors(4, map_overhead_cf)

    # compute regime: scenario 4's fan-out work as a single map call
    print("\n== 5a. map compute (winsum x 2000, 4 workers) ==")
    n = 2000

    note_rate("map", "in-process", n, lambda: [winsum(i) for i in range(n)])

    def map_pool(p):
        xs = list(range(n))
        p.map(winsum, xs)  # warm-up
        note_rate("map", "pyrei pool", n, lambda: p.map(winsum, xs))

    with_pool(4, map_pool)

    def map_cf(ex, label):
        xs = list(range(n))
        list(ex.map(winsum, xs[:200], chunksize=1))  # warm-up
        note_rate(
            "map",
            label,
            n,
            lambda: list(ex.map(winsum, xs, chunksize=1)),
        )

    with_executors(4, map_cf)

    # skew regime: 1% of elements cost ~100x the rest, clustered at the
    # head — fine self-scheduled claims keep the workers level where a
    # coarse static split concentrates the heavy heads on one worker
    print("\n== 5b. map skew (1% heavy elements x 4000, 4 workers) ==")
    n = 4000

    def map_skew(p):
        xs = list(range(n))
        p.map(skewed, xs)  # warm-up
        note(
            "map skewed f",
            "pyrei pool",
            best_ms(lambda: p.map(skewed, xs)),
            "ms wall",
        )

    with_pool(4, map_skew)

    def map_skew_cf(ex, label):
        xs = list(range(n))
        list(ex.map(skewed, xs[:200], chunksize=1))  # warm-up
        note(
            "map skewed f",
            label,
            best_ms(lambda: list(ex.map(skewed, xs, chunksize=1))),
            "ms wall",
        )

    with_executors(4, map_skew_cf)

    # summary -----------------------------------------------------------------

    print("\n== summary ==")
    seen = list(dict.fromkeys(scenario for scenario, *_ in results))
    for s in seen:
        for scenario, framework, value, unit in results:
            if scenario == s:
                print(
                    f"  {scenario:<20} {framework:<20} "
                    f"{value:>15,.1f} {unit}"
                )


if __name__ == "__main__":
    # the guard is required: spawn re-imports the main module in
    # every ProcessPoolExecutor worker
    main()
