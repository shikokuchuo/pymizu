# Report-only benchmark: the cross-language channel — pymizu Channel
# round trips against an R echo peer (mizu), per payload shape. One round
# trip exercises all four halves of the transport: Python stage, R read,
# R stage, Python read. The mirror of dev/bench/
# crosslang-channel-bench.R in the mizu repo (which hosts the R driver).
# Prints a table; asserts nothing. Skips with a message when Rscript with
# mizu is not available.
#
#   1. round-trip latency   None, scalars, float64 arrays from the inline
#                           RAWVEC tier (8 KB) through the zero-copy
#                           SHM_VEC tier (800 KB, 8 MB, the echo relaying
#                           the received view by reference)
#   2. typed payloads       strings, logicals with NA, timedelta64 (the
#                           difftime interchange), datetime64
#   3. send-only staging    one-way sends against an acking sink: the ack
#                           is the None row's cost, the remainder the
#                           send-side stage
#   4. pipelined throughput small arrays in flight, no per-send wait
#
# Median of 5 runs of loops sized past the timer's floor. Run:
#
#   python benchmarks/crosslang-channel-bench.py

import statistics
import time

import pymizu

try:
    launcher = pymizu.r_launcher()
except pymizu.MizuError as e:
    print(f"Rscript with mizu not available: {e}; skipping")
    raise SystemExit(0) from None

import numpy as np

R_ECHO = """
repeat {
  x <- mizu::mizu_recv(ch, timeout = 30)
  if (inherits(x, "mizu_sentinel")) break
  mizu::mizu_send(ch, x)
}
"""

# the copy peer: materializes before echoing, so both directions pay the
# layout write (the plain echo relays a received view by reference)
R_COPY_ECHO = """
repeat {
  x <- mizu::mizu_recv(ch, timeout = 30)
  if (inherits(x, "mizu_sentinel")) break
  mizu::mizu_send(ch, if (is.numeric(x)) x + 0 else x)
}
"""

# the acking sink: acks each payload with NULL, so a send + ack wait
# measures the Python-side stage alone (the sink's read of a view-tier
# payload is a cheap wrap)
R_SINK = """
repeat {
  x <- mizu::mizu_recv(ch, timeout = 30)
  if (inherits(x, "mizu_sentinel")) break
  mizu::mizu_send(ch, NULL)
}
"""

results = []


def note(scenario, peer, value, unit):
    print(f"  {peer:<10} {value:>12,.1f} {unit}")
    results.append(
        {"scenario": scenario, "peer": peer, "value": value, "unit": unit}
    )


def bench(f, loops, min_time=0.3):
    """Median of 5: seconds per `loops`-iteration run of f."""
    f()
    samples = []
    for _ in range(5):
        t0 = time.perf_counter()
        f()
        samples.append((time.perf_counter() - t0) / loops)
        if sum(samples) < min_time and loops < 10_000:
            loops *= 2
    return statistics.median(samples)


def rt_us(ch, x, loops=20):
    def f():
        for _ in range(loops):
            ch.send(x)
            ch.recv(30)

    return bench(f, loops) * 1e6


print("\n== 1. cross-language channel round-trip latency ==\n")

payloads = [
    ("None", None),
    ("scalar double", 1.5),
    ("scalar string", "hello"),
    ("8 KB float64", np.random.default_rng(0).random(1000)),
    ("800 KB float64", np.random.default_rng(1).random(100_000)),
    ("8 MB float64", np.random.default_rng(2).random(1_000_000)),
]

for name, x in payloads:
    ch = pymizu.Channel.create(R_ECHO, launcher=launcher)
    try:
        note(name, "echo", rt_us(ch, x), "us/rt")
    finally:
        ch.close()
    ch = pymizu.Channel.create(R_COPY_ECHO, launcher=launcher)
    try:
        note(name, "copy echo", rt_us(ch, x), "us/rt")
    finally:
        ch.close()

print("\n== 2. typed payloads (echo peer) ==\n")

typed = [
    ("10k strings", [f"value-{i:05d}" for i in range(10_000)]),
    ("100k bool+None", [True, False, None] * 33_333 + [True]),
    (
        "10k datetime64[D]",
        np.arange(
            np.datetime64("2026-01-01"),
            np.datetime64("2026-01-01") + np.timedelta64(10_000, "D"),
            dtype="datetime64[D]",
        ),
    ),
    (
        "100k timedelta64[us]",
        np.arange(100_000, dtype="timedelta64[us]"),
    ),
]

for name, x in typed:
    ch = pymizu.Channel.create(R_ECHO, launcher=launcher)
    try:
        note(name, "echo", rt_us(ch, x), "us/rt")
    finally:
        ch.close()

print("\n== 3. send-only staging (acking sink) ==\n")

# the ~100 KB nested tree: no buffers (below the MIZS gate), so the 'I'
# writer's arena-carrier spill is the whole cost — the one-walk stager's
# acceptance row (the malloc + second memcpy of the two-pass stager gone)
tree = [
    {"id": i, "name": f"item-{i:04d}", "vals": [i, i * 1.5, True, None]}
    for i in range(1200)
]
for name, x in [
    ("10k strings", [f"value-{i:05d}" for i in range(10_000)]),
    ("100 KB nested tree", tree),
]:
    ch = pymizu.Channel.create(R_SINK, launcher=launcher)
    try:
        note(name, "sink", rt_us(ch, x), "us/send")
    finally:
        ch.close()

print("\n== 4. pipelined throughput (10k x 8 KB float64 in flight) ==\n")

k = 10_000
x = np.random.default_rng(3).random(1000)
ch = pymizu.Channel.create(R_ECHO, launcher=launcher)
try:
    ch.send(x)
    ch.recv(30)

    def f():
        for _ in range(k):
            ch.send(x)
        for _ in range(k):
            ch.recv(30)

    secs = bench(f, 1)
    note("pipelined 8 KB", "echo", k / secs, "rt/s")
finally:
    ch.close()

print("\n== summary ==\n")
for r in results:
    v = f"{r['value']:>12,.1f}"
    print(f"{r['scenario']:<22} {r['peer']:<10} {v} {r['unit']}")
