"""Report-only benchmark: the pymizu channel and pool, the Python mirror of
mizu's dev/bench/mizu-mirai.R (without the mirai/nanonext comparison rows).
Prints each number as it lands and a summary table at the end; asserts
nothing.

  1. sequential round-trip  const task, 1 worker: submit + collect loop
  2. pipelined throughput   const task, 1 worker: fire n, collect n;
                            channel and pool each carry a batched row
  3. payload round-trip     identity task on float64 vectors of 8 KB /
                            800 KB / 8 MB, 1 worker: data both ways.
                            Slots are sized to the payload where the 2^20
                            slot_size cap allows, so 8 KB and 800 KB ride
                            in-slot (RAWVEC) and 8 MB takes the zero-copy
                            tier (an SHM_VEC view per payload)
  4. parallel fan-out       small compute tasks, 4 workers: fire all,
                            collect all (in-process loop as the anchor)
  5. streaming              one-way const messages: channel send_batch /
                            recv_batch
  6. parallel map           Pool.map, 4 workers, mirroring mizu-bench.R's
                            section 6: the trivial-f overhead regime
                            (plain / template / seed / prepared, us/elt),
                            winsum over 2000 elements as the compute
                            regime (in-process loop as the anchor), a
                            20,000-element scaling row, a template row at
                            that size, and the skew regime (1% heavy
                            elements, ms wall)

Timings are best-of-3 after warm-up; single runs on a busy machine still
jitter.

Run from the repo root (the workers inherit it as their working directory,
which is what makes benchmarks.tasks importable to them):

  python benchmarks/mizu-bench.py
"""

import os
import subprocess
import sys
import time

import numpy as np

import pymizu

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from benchmarks.tasks import (  # noqa: E402
    const,
    identity,
    plus_one,
    skewed,
    winsum,
    winsum_t,
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
        [sys.executable, "-m", "pymizu.worker", token, str(slot)], cwd=ROOT
    )


def with_pool(workers, f, **args):
    p = pymizu.Pool.create(
        workers, max_submitters=2, launcher=worker_launcher, **args
    )
    try:
        f(p)
    finally:
        p.stop(timeout=15)


def with_channel(source, f, **args):
    ch = pymizu.Channel.create(source, **args)
    try:
        f(ch)
    finally:
        ch.close(timeout=10)


ECHO_PEER = """
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(x)
"""

BATCH_PEER = """
import pymizu
while True:
    xs = ch.recv_batch(4096)
    if xs is pymizu.CLOSED or xs is pymizu.PEER_GONE:
        break
    ch.send_batch(xs)
"""

print(
    f"pymizu {pymizu.__version__} | core {pymizu.__core_version__} | "
    f"Python {sys.version.split()[0]} | {sys.platform} {os.uname().machine}"
)

# 1. sequential round-trip ----------------------------------------------------

print("\n== 1. sequential round-trip (const task, 1 worker) ==")
n = 2000

nc = 20000  # channel rt is us-scale: run long


def seq_channel(ch):
    warmup(lambda: (ch.send(1), ch.recv(timeout=30)))

    def rep():
        for _ in range(nc):
            ch.send(1)
            ch.recv(timeout=30)

    note_us("sequential rt", "pymizu channel", nc, rep, "us/rt")


with_channel(ECHO_PEER, seq_channel, capacity=1024)


def seq_pool(p):
    warmup(lambda: p.submit(const).collect(timeout=30))

    def rep():
        for _ in range(n):
            p.submit(const).collect(timeout=30)

    note_us("sequential rt", "pymizu pool", n, rep)


with_pool(1, seq_pool)

# 2. pipelined throughput -----------------------------------------------------

print("\n== 2. pipelined throughput (const task, 1 worker) ==")
n = 10000
k = 10  # cycles per rep


def pipe_channel(ch):
    warmup(lambda: (ch.send(1), ch.recv(timeout=30)))

    def rep():
        for _ in range(k):
            for _ in range(n):
                ch.send(1)
            for _ in range(n):
                ch.recv(timeout=30)

    note_rate("pipelined", "pymizu channel", k * n, rep, "rt/s")


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
            if pymizu.is_sentinel(xs):
                raise RuntimeError(
                    "pymizu channel batch: peer stopped echoing"
                )
            got += len(xs)

    warmup(lambda: stream(100))
    note_rate(
        "pipelined",
        "pymizu channel batch",
        k * n,
        lambda: [stream(n) for _ in range(k)],
        "rt/s",
    )


with_channel(BATCH_PEER, pipe_channel_batch)


# a submitter's outstanding tasks are bounded by its result-slot share, so
# fire-n-then-collect needs result_slots / max_submitters >= n
def pipe_pool(p):
    def fire():
        return p.submit(const)

    def reap(t):
        return t.collect(timeout=30)

    warmup(lambda: reap(fire()))
    note_rate("pipelined", "pymizu pool", n, lambda: pipeline(fire, reap, n))


with_pool(1, pipe_pool, result_slots=20480)


# the batch pair: one submit crossing + one collect crossing per burst
def pipe_pool_batch(p):
    fns = [const] * n
    warmup(
        lambda: p.collect_all(
            p.submit_batch(fns[:100], timeout=30), timeout=30
        )
    )
    note_rate(
        "pipelined",
        "pymizu pool batch",
        k * n,
        lambda: [
            p.collect_all(p.submit_batch(fns, timeout=30), timeout=30)
            for _ in range(k)
        ],
    )


with_pool(1, pipe_pool_batch, result_slots=20480)

# 3. payload round-trip -------------------------------------------------------

print("\n== 3. payload round-trip (identity task on a float64 vector) ==")

# slots sized to the payload where the cap allows: 8 KB and 800 KB ride
# in-slot (RAWVEC, no spill); 8 MB exceeds 2^20 and spills on default
# slots — a fresh RAWSPILL region per payload each way
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

        note_us(label, "pymizu pool", n, rep)

    with_pool(1, run, **args)

# 4. parallel fan-out ---------------------------------------------------------

print("\n== 4. parallel fan-out (winsum x 2000, 4 workers) ==")
n = 2000

note_rate("fan-out", "in-process", n, lambda: [winsum(i) for i in range(n)])


def fanout_pool(p):
    def fire():
        return p.submit(winsum, 0)

    def reap(t):
        return t.collect(timeout=30)

    pipeline(fire, reap, n)
    note_rate("fan-out", "pymizu pool", n, lambda: pipeline(fire, reap, n))


with_pool(4, fanout_pool)

# 5. streaming ----------------------------------------------------------------

print("\n== 5. streaming (one-way const messages, batched) ==")
n = 200000
k = 10  # rounds per rep

# the peer counts arrivals and sends one receipt per n, so the same channel
# serves the warm-up round and every rep
STREAM_PEER = """
import pymizu
total = 0
while True:
    xs = ch.recv_batch(4096)
    if xs is pymizu.CLOSED or xs is pymizu.PEER_GONE:
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
    note_rate(
        "streaming",
        "pymizu channel",
        k * n,
        lambda: [stream_round() for _ in range(k)],
        "msg/s",
    )


with_channel(STREAM_PEER, stream_channel)

# 6. parallel map -------------------------------------------------------------

print("\n== 6. parallel map (f over n elements, 4 workers) ==")

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
    tmpl = np.empty(1)
    p.map(plus_one, xs)  # warm-up
    note_us(
        "map trivial f",
        "pymizu pool",
        k * n,
        lambda: [p.map(plus_one, xs) for _ in range(k)],
        "us/elt",
    )
    note_us(
        "map trivial f",
        "pymizu template",
        k * n,
        lambda: [p.map(plus_one, xs, template=tmpl) for _ in range(k)],
        "us/elt",
    )
    # deterministic per-element streams: the price of reproducibility
    note_us(
        "map trivial f",
        "pymizu seed",
        k * n,
        lambda: [p.map(plus_one, xs, seed=42) for _ in range(k)],
        "us/elt",
    )
    # prepared: stage once, run many — per-run cost is submit + collect,
    # and back-to-back runs hit the workers' cached map contexts
    pm = p.map_prepare(plus_one, xs)
    try:
        p.map_run(pm)  # warm-up
        note_us(
            "map trivial f",
            "pymizu prepared",
            k * n,
            lambda: [p.map_run(pm) for _ in range(k)],
            "us/elt",
        )
    finally:
        pm.close()


with_pool(4, map_overhead)

# compute regime: scenario 4's fan-out work as a single map call — the
# per-element overhead above amortized against real tasks
print("\n== 6a. map compute (Pool.map winsum x 2000, 4 workers) ==")
n = 2000

note_rate("map", "in-process", n, lambda: [winsum(i) for i in range(n)])


def map_pool(p):
    xs = list(range(n))
    p.map(winsum, xs)  # warm-up
    note_rate("map", "pymizu pool", n, lambda: p.map(winsum, xs))


with_pool(4, map_pool)

# 6b. map scaling -------------------------------------------------------------

print("\n== 6b. map scaling (Pool.map winsum x 20000, 4 workers) ==")
n = 20000

note_rate("map 20k", "in-process", n, lambda: [winsum(i) for i in range(n)])


def map_pool_20k(p):
    xs = list(range(n))
    p.map(winsum, xs)  # warm-up
    note_rate("map 20k", "pymizu pool", n, lambda: p.map(winsum, xs))


with_pool(4, map_pool_20k)

# 6c. template map ------------------------------------------------------------

print("\n== 6c. template map (winsum x 20000 -> n x 2 float64, 4 workers) ==")


def map_template(p):
    xs = list(range(n))
    tmpl = np.empty(2)
    p.map(winsum_t, xs, template=tmpl)  # warm-up
    note_rate(
        "map template", "pymizu copy", n,
        lambda: p.map(winsum_t, xs, template=tmpl),
    )
    note_rate(
        "map template", "pymizu view", n,
        lambda: p.map(winsum_t, xs, template=tmpl, collect="view"),
    )


with_pool(4, map_template)

# 6d. map skew ----------------------------------------------------------------

# skew regime: 1% of elements cost ~100x the rest, clustered at the head —
# fine self-scheduled claims keep the workers level where a coarse static
# split concentrates the heavy heads on one worker
print("\n== 6d. map skew (1% heavy elements x 4000, 4 workers) ==")
n = 4000


def map_skew(p):
    xs = list(range(n))
    p.map(skewed, xs)  # warm-up
    note(
        "map skewed f",
        "pymizu pool",
        best_ms(lambda: p.map(skewed, xs)),
        "ms wall",
    )


with_pool(4, map_skew)

# 7. conversion staging (the numpy / Arrow conversion pass) -------------------

print("\n== 7. conversion staging (8 MB one-way sends, channel) ==")

# one-way sends against an acking sink: the measured cost is the send-side
# stage (the sink's read of a view-tier payload is a cheap wrap)
SINK_PEER = """
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(True)
"""


def arrow_masked(arr, ptype):
    """An all-valid-bitmap Arrow array with null_count "unknown" (-1): the
    masked conversion loop's fast-path shape. pyarrow computes the count
    eagerly on export, so patch each exported struct back to -1."""
    import ctypes

    import pyarrow as pa

    class ArrowArray(ctypes.Structure):
        _fields_ = [
            ("length", ctypes.c_int64), ("null_count", ctypes.c_int64),
            ("offset", ctypes.c_int64), ("n_buffers", ctypes.c_int64),
            ("n_children", ctypes.c_int64), ("buffers", ctypes.c_void_p),
            ("children", ctypes.c_void_p), ("dictionary", ctypes.c_void_p),
            ("release", ctypes.c_void_p), ("private_data", ctypes.c_void_p),
        ]

    lib = ctypes.pythonapi
    lib.PyCapsule_GetPointer.restype = ctypes.c_void_p
    lib.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    mask = pa.py_buffer(b"\xff" * ((len(arr) + 7) // 8))
    data = pa.py_buffer(arr.tobytes())
    a = pa.Array.from_buffers(ptype, len(arr), [mask, data], null_count=-1)

    class Producer:
        def __arrow_c_array__(self):
            caps = a.__arrow_c_array__()
            ap = lib.PyCapsule_GetPointer(caps[1], b"arrow_array")
            ArrowArray.from_address(ap).null_count = -1
            return caps

    return Producer()


try:
    import pyarrow as pa

    size = 1000000  # 8 MB as float64/int64
    n = 30

    def convert_channel(ch):
        base = np.arange(size, dtype=np.float64)
        cases = [
            ("stage memcpy", base.tobytes()),
            ("stage identity", base),
            ("stage int64", base.astype(np.int64)),
        ]
        for label, x in cases:

            def rep(x=x):
                for _ in range(n):
                    ch.send(x)
                    ch.recv(timeout=30)

            warmup(rep, n=3)
            note_us(label, "pymizu channel", n, rep, "us/send")

    with_channel(SINK_PEER, convert_channel)
except ImportError:
    print("  pyarrow not installed: skipped")

# the conversion rows are foreign-only (same-language channels pickle):
# an in-process foreign pair stands in for an R peer
def convert_foreign():
    from pymizu import _pymizu

    size = 1000000
    n = 30
    h = _pymizu._channel_new(1024, 1 << 16, 1 << 24, False, b"")
    p, _ = _pymizu._channel_attach(h.token, _ident=(2, 0))
    p.ready_set()
    assert h.ready_wait(10)
    try:
        base = np.arange(size, dtype=np.float64)
        cases = [
            ("stage widen", base.astype(np.uint64)),
            ("stage masked", arrow_masked(base, pa.float64())),
            ("stage masked int64",
             arrow_masked(base.astype(np.int64), pa.int64())),
            ("stage masked+scan",
             arrow_masked(base.astype(np.int32), pa.int32())),
        ]
        for label, x in cases:

            def rep(x=x):
                for _ in range(n):
                    h.send(x)
                    p.recv(timeout=30)

            warmup(rep, n=3)
            note_us(label, "pymizu channel (foreign)", n, rep, "us/send")
    finally:
        p.destroy()
        h.destroy()


try:
    import pyarrow as pa

    convert_foreign()
except ImportError:
    pass

# 7. task args by reference (F1) ----------------------------------------------

print("\n== 7. task args by reference: 8 MB float64 arg to a pool task ==")

try:
    launcher = pymizu.r_pool_launcher()
except pymizu.MizuError:
    print("  Rscript with mizu not available: skipped")
else:
    size = 1000000  # 8 MB as float64
    n = 24
    x = np.random.random(size)

    # same-language flat check: the private task frames are untouched
    def same_pool(p):
        assert p.submit(np.mean, x).collect(timeout=60) == float(x.mean())

        def rep():
            for _ in range(n):
                p.submit(np.mean, x).collect(timeout=60)

        warmup(rep, n=2)
        note_us("arg 8 MB", "Python pool (flat)", n, rep, "us/task")

    with_pool(2, same_pool)

    # the F1 row: a fresh array stages one SHM_VEC layout write where the
    # pre-F1 wire paid a full copy each way (~the payload rows' cost)
    with pymizu.Pool.create(2, launcher=launcher) as rp:
        assert np.isclose(
            rp.submit(pymizu.call("base::mean", x)).collect(timeout=60),
            float(x.mean()),
        )

        def rep_zc():
            for _ in range(n):
                rp.submit(pymizu.call("base::mean", x)).collect(timeout=60)

        warmup(rep_zc, n=2)
        note_us("arg 8 MB", "R pool (SHM_VEC)", n, rep_zc, "us/task")

        # a received view re-sent: REF — zero payload bytes for the argument
        view = rp.submit(pymizu.call("base::identity", x)).collect(timeout=60)
        assert np.isclose(
            rp.submit(pymizu.call("base::mean", view)).collect(timeout=60),
            float(x.mean()),
        )

        def rep_ref():
            for _ in range(n):
                rp.submit(pymizu.call("base::mean", view)).collect(
                    timeout=60)

        warmup(rep_ref, n=2)
        note_us("arg 8 MB", "R pool (REF)", n, rep_ref, "us/task")

    # the W2 fold: the submit plan walks once where the pre-fold loop ran
    # a size pass per candidate (k+3 walks). The by-value filler keeps the
    # whole argument tree on the walk, so the plan's walk count is the
    # measured term (k = 0 is the no-candidate baseline)
    with pymizu.Pool.create(2, launcher=launcher) as rp:
        cands = [np.random.random(5120) for _ in range(16)]  # 40 KB each
        filler = [float(i) for i in range(3000)]
        for k in (0, 1, 4, 16):
            spec_args = (filler, *cands[:k])

            def rep_k(spec_args=spec_args):
                for _ in range(n):
                    rp.submit(
                        pymizu.call(None, *spec_args, source="0L")
                    ).collect(timeout=60)

            warmup(rep_k, n=1)
            note_us(f"plan k = {k:2d}", "R pool", n, rep_k, "us/task")

# 7b. string list staging (F4) ------------------------------------------------

print("\n== 7b. string list staging: 1M list[str] to a foreign peer ==")


# one builder, two rows: the in-process foreign pair (convert_foreign's
# pattern), the acking read keeping the measured cost on the send-side stage
def strlist_foreign():
    from pymizu import _pymizu

    size = 1000000  # ~20 MB string block
    n = 10
    xs = [f"str-{i:08d}" for i in range(size)]
    for label, ident in [
        ("stage str list 0x0b", (2, 0)),   # no caps: the value-copy tree
        ("stage str list MIZS", (2, 1)),   # MIZU_CAP_MIZS: one layout write
    ]:
        h = _pymizu._channel_new(1024, 1 << 16, 1 << 24, False, b"")
        p, _ = _pymizu._channel_attach(h.token, _ident=ident)
        p.ready_set()
        assert h.ready_wait(10)
        try:

            def rep(h=h, p=p):
                for _ in range(n):
                    h.send(xs)
                    p.recv(timeout=60)

            warmup(rep, n=1)
            note_us(label, "pymizu channel (foreign)", n, rep, "us/send")
        finally:
            p.destroy()
            h.destroy()


strlist_foreign()

# 7c. f64 matrix staging (F5) -------------------------------------------------

print("\n== 7c. f64 matrix staging: 8 MB F-order matrix to a foreign peer ==")


# one builder, two rows: the in-process foreign pair (strlist_foreign's
# pattern). The pair is the calibration: both paths are one bulk memcpy +
# one region, so expect ~parity — a materially slower MIZH row means the
# tier is doing per-element work
def mizh_foreign():
    from pymizu import _pymizu

    a = np.arange(1024 * 1024, dtype=np.float64).reshape(
        (1024, 1024), order="F"
    )
    n = 10
    for label, ident in [
        ("stage f64 matrix 0x0f", (2, 0)),   # no caps: the attr-tag copy
        ("stage f64 matrix MIZH", (2, 2)),   # MIZU_CAP_ATTRS: layout + blob
    ]:
        h = _pymizu._channel_new(1024, 1 << 16, 1 << 24, False, b"")
        p, _ = _pymizu._channel_attach(h.token, _ident=ident)
        p.ready_set()
        assert h.ready_wait(10)
        try:

            def rep(h=h, p=p):
                for _ in range(n):
                    h.send(a)
                    p.recv(timeout=60)

            warmup(rep, n=1)
            note_us(label, "pymizu channel (foreign)", n, rep, "us/send")
        finally:
            p.destroy()
            h.destroy()


mizh_foreign()

# the round trip through an R echo peer — the relation of record: the
# MIZH row carries R's view wrap at receive and the REF return, the 0x0f
# row the attr-tag copy parsed and copied again at R's receive. A real R
# peer always advertises ATTRS, so the 0x0f row sends the C-order twin:
# the wire shape is the pre-F5 attr-tag copy either way, its per-element
# gather standing in for the pre-F5 bulk convert (the before row reads
# slightly slow)
R_MAT_ECHO_BENCH = r"""
repeat {
  x <- mizu::mizu_recv(ch, timeout = 120)
  if (mizu::mizu_is_sentinel(x)) break
  mizu::mizu_send(ch, x)
}
"""

try:
    mat_launcher = pymizu.r_launcher()
except pymizu.MizuError:
    print("  Rscript with mizu not available: skipped")
else:
    a = np.arange(1024 * 1024, dtype=np.float64).reshape(
        (1024, 1024), order="F"
    )
    a_c = np.ascontiguousarray(a)
    n = 10
    ch = pymizu.Channel.create(R_MAT_ECHO_BENCH, launcher=mat_launcher)
    try:

        def rep_ix():
            for _ in range(n):
                ch.send(a_c)
                ch.recv(timeout=120)

        warmup(rep_ix, n=1)
        note_us("echo f64 matrix 0x0f", "R echo", n, rep_ix, "us/rt")

        def rep_mizh():
            for _ in range(n):
                ch.send(a)
                ch.recv(timeout=120)

        warmup(rep_mizh, n=1)
        note_us("echo f64 matrix MIZH", "R echo", n, rep_mizh, "us/rt")
    finally:
        ch.close(timeout=10)

# 8. frame relay: the per-column REF (F2) -------------------------------------

print("\n== 8. frame relay: 10-col 1e6-row frame, R -> polars -> R ==")

try:
    import polars as pl
except ImportError:
    pl = None
try:
    frame_launcher = pymizu.r_launcher()
except pymizu.MizuError:
    frame_launcher = None

if pl is None or frame_launcher is None:
    print("  polars or Rscript with mizu not available: skipped")
else:
    n = 24
    nrows = 1000000
    # the return hop, ack'd per relay: unmodified is the whole-frame REF
    # (zero payload bytes, the regression guard); one computed column is
    # 1 layout leaf + 9 remote leaves where pre-F2 it was 10 columns'
    # layout write
    R_FRAME_RELAY_BENCH = r"""
df <- as.data.frame(matrix(runif(10 * 1000000), nrow = 1000000))
mizu::mizu_send(ch, df)
while (TRUE) {
  y <- mizu::mizu_recv(ch, timeout = 120)
  if (mizu::mizu_is_sentinel(y)) break
  mizu::mizu_send(ch, nrow(y))
}
"""
    ch = pymizu.Channel.create(R_FRAME_RELAY_BENCH, launcher=frame_launcher)
    try:
        f = ch.recv(120)
        df = pl.DataFrame(f)
        mod = df.with_columns((pl.col("V10") * 2).alias("V10"))

        def rep_unmod():
            for _ in range(n):
                ch.send(df)
                assert ch.recv(timeout=120) == nrows

        warmup(rep_unmod, n=2)
        note_us("frame 10col relay", "R (unmodified REF)", n, rep_unmod,
                "us/rt")

        def rep_mod():
            for _ in range(n):
                ch.send(mod)
                assert ch.recv(timeout=120) == nrows

        warmup(rep_mod, n=2)
        note_us("frame 10col relay", "R (one computed col)", n, rep_mod,
                "us/rt")

        # the same shape with one column swapped to strings (mixed <=12B
        # and >12B): polars re-views it as string_view — verified
        # read-only against the export record, the round trip REFs (F3);
        # before it the string column paid the MIZL write per relay
        R_FRAME_STR_RELAY_BENCH = r"""
df <- as.data.frame(matrix(runif(9 * 1000000), nrow = 1000000))
df$s <- ifelse(seq_len(1000000) %% 3 == 0,
               sprintf("long-string-value-%d", seq_len(1000000)),
               sprintf("s%d", seq_len(1000000)))
mizu::mizu_send(ch, df)
while (TRUE) {
  y <- mizu::mizu_recv(ch, timeout = 120)
  if (mizu::mizu_is_sentinel(y)) break
  mizu::mizu_send(ch, nrow(y))
}
"""
        ch_str = pymizu.Channel.create(R_FRAME_STR_RELAY_BENCH,
                                       launcher=frame_launcher)
        try:
            f_str = ch_str.recv(120)
            df_str = pl.DataFrame(f_str)

            def rep_str_unmod():
                for _ in range(n):
                    ch_str.send(df_str)
                    assert ch_str.recv(timeout=120) == nrows

            warmup(rep_str_unmod, n=2)
            note_us("frame strcol relay", "R (unmodified string REF)", n,
                    rep_str_unmod, "us/rt")
        finally:
            ch_str.close(timeout=10)

        # same-language flat check: the MIZL frame write carries no
        # remote leaves on same-language handles. A pickle-rebuilt frame
        # is heap-backed, so the REF fast path cannot fire (ahead of the
        # close — f's region unlinks with the channel)
        import pickle

        f2 = pickle.loads(pickle.dumps(f))
        from pymizu import _pymizu

        h = _pymizu._channel_new(1024, 1 << 16, 1 << 24, False, b"")
        p, _ = _pymizu._channel_attach(h.token)
        p.ready_set()
        assert h.ready_wait(10)
        try:
            def rep_same():
                for _ in range(n):
                    h.send(f2)
                    p.recv(timeout=30)

            warmup(rep_same, n=2)
            note_us("frame 10col relay", "Python (flat)", n, rep_same,
                    "us/send")
        finally:
            p.destroy()
            h.destroy()
    finally:
        ch.close(timeout=10)

# summary ---------------------------------------------------------------------

print("\n== summary ==")
seen = list(dict.fromkeys(scenario for scenario, *_ in results))
for s in seen:
    for scenario, framework, value, unit in results:
        if scenario == s:
            print(f"  {scenario:<20} {framework:<20} {value:>15,.1f} {unit}")
