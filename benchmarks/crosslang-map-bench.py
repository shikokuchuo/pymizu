"""Report-only benchmark: the cross-language map (Phase 5) — Pool.map
with a pymizu.call spec over an R worker pool, against the same-language
map and the stdlib ProcessPoolExecutor, matched regime-for-regime with
benchmarks/mizu-stdlib-bench.py's map scenario. Prints each number as it
lands and a summary table at the end; asserts nothing. Exits with a
message when Rscript with an installed mizu is not available.

  1. overhead regime   trivial fn over 10k float64, 4 workers: the
                       in-process loop (anchor), ProcessPoolExecutor.map
                       with chunksize=1 (the task-per-element model's
                       cost), Pool.map on Python workers, and Pool.map
                       with a spec on R workers — the interop descriptor,
                       kind-2 runner tasks, and interop result values —
                       with its template, seed, and prepared variants
  2. compute regime    ~10 us of work per element as one map call
                       (n = 2000), native against spec

Timings are best-of-3 after warm-up; single runs on a busy machine still
jitter. Run from the repo root:

  python benchmarks/crosslang-map-bench.py
"""

import os
import sys
import time

import numpy as np

import pymizu

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from benchmarks.tasks import plus_one, winsum  # noqa: E402

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


def main():
    try:
        r_launcher = pymizu.r_pool_launcher()
    except pymizu.MizuError:
        print("Rscript with the mizu package not available: skipping")
        return

    # 1. overhead regime: trivial fn, per-element cost is the whole story

    print("\n== 1. cross-language map overhead (trivial fn, n = 10000, "
          "4 workers) ==")

    n = 10000
    xs = np.arange(n, dtype=np.float64)
    k = 10  # map calls per rep: a trivial map is sub-ms, so loop to average

    note_us(
        "map trivial fn",
        "in-process",
        k * n,
        lambda: [[abs(v) for v in xs] for _ in range(k)],
        "us/elt",
    )

    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(4) as ex:
        list(ex.map(plus_one, xs[:200], chunksize=1))  # warm-up
        note_us(
            "map trivial fn",
            "cf process pool",
            k * n,
            lambda: [
                list(ex.map(plus_one, xs, chunksize=1)) for _ in range(k)
            ],
            "us/elt",
        )

    pool = pymizu.Pool.create(4)
    try:
        pool.map(plus_one, xs)  # warm-up
        note_us(
            "map trivial fn",
            "pymizu pool",
            k * n,
            lambda: [pool.map(plus_one, xs) for _ in range(k)],
            "us/elt",
        )
    finally:
        pool.stop(timeout=15)

    spec = pymizu.call("base::abs")
    pool = pymizu.Pool.create(4, launcher=r_launcher)
    try:
        pool.map(spec, xs)  # warm-up
        note_us(
            "map trivial fn",
            "pymizu spec",
            k * n,
            lambda: [pool.map(spec, xs) for _ in range(k)],
            "us/elt",
        )
        # the template path: results land in the region's output area,
        # no per-element result framing at all
        tmpl = np.empty(1)
        note_us(
            "map trivial fn",
            "pymizu spec template",
            k * n,
            lambda: [pool.map(spec, xs, template=tmpl) for _ in range(k)],
            "us/elt",
        )
        # the neutral (seed, offset) pair; per-element streams derived
        # worker-side (L'Ecuyer-CMRG on the R side)
        note_us(
            "map trivial fn",
            "pymizu spec seed",
            k * n,
            lambda: [
                pool.map(pymizu.call(source="runif(1)"), xs, seed=42)
                for _ in range(k)
            ],
            "us/elt",
        )
        # prepared: stage once, run many — per-run cost is the kind-2
        # runner submit + collect, the workers' contexts cached
        pm = pool.map_prepare(spec, xs)
        try:
            pm.run()  # warm-up
            note_us(
                "map trivial fn",
                "pymizu spec prepared",
                k * n,
                lambda: [pm.run() for _ in range(k)],
                "us/elt",
            )
        finally:
            pm.close()
    finally:
        pool.stop(timeout=15)

    # 2. compute regime: ~10 us of work per element as one map call

    print("\n== 2. cross-language map compute (~10 us elements, "
          "n = 2000) ==")

    n = 2000
    gspec = pymizu.call(source="sum(runif(2000))")

    note_rate("map ~10us tasks", "in-process", n,
              lambda: [winsum(i) for i in range(n)])

    pool = pymizu.Pool.create(4)
    try:
        pool.map(winsum, range(n))  # warm-up
        note_rate("map ~10us tasks", "pymizu pool", n,
                  lambda: pool.map(winsum, range(n)))
    finally:
        pool.stop(timeout=15)

    pool = pymizu.Pool.create(4, launcher=r_launcher)
    try:
        pool.map(gspec, range(n))  # warm-up
        note_rate("map ~10us tasks", "pymizu spec", n,
                  lambda: pool.map(gspec, range(n)))
    finally:
        pool.stop(timeout=15)

    print("\n== summary ==")
    for scenario, framework, value, unit in results:
        print(f"  {scenario:<20} {framework:<22} {value:>15,.1f} {unit}")


if __name__ == "__main__":
    # the guard is required: spawn re-imports the main module in
    # every ProcessPoolExecutor worker
    main()
