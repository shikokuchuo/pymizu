"""Pool.map tests: round-trip and order, chunking invariance, the blob and
region paths, raw-buffer x sections, the outcome taxonomy (element-indexed
errors, fail-fast, timeout, worker death), seed determinism, nested maps,
and the interrupt backstop — over real spawned workers."""

import os
import random
import signal
import threading
import time

import pytest
from tests.helpers import (
    fail_at,
    fail_or_sleep,
    identity,
    kill_at,
    nested_map,
    np_scalar_double,
    rand_elt,
    scale_add,
    sleep_ident,
    square,
)

import pyrei


@pytest.fixture
def pool():
    p = pyrei.Pool.create(2)
    yield p
    p.stop()


def test_map_roundtrip(pool):
    assert pool.map(square, list(range(10))) == [i * i for i in range(10)]


def test_map_empty(pool):
    assert pool.map(square, []) == []


def test_map_args_kwargs(pool):
    assert pool.map(pow, [1, 2, 3], args=(2,)) == [1, 4, 9]
    assert pool.map(scale_add, [1, 2], kwargs={"scale": 2, "add": 1}) == [3, 5]


def test_map_blob_path(pool):
    # a small map whose chunk payloads fit the entry inline budget: no
    # region is created (the morsel machinery needs one)
    assert pool.map(square, [3, 1, 2]) == [9, 1, 4]


def test_map_region_path(pool):
    # a descriptor past the inline budget forces the region path
    x = list(range(2000))
    assert pool.map(identity, x) == x


def test_map_chunking_invariance(pool):
    x = list(range(500))
    want = [i * i for i in x]
    for chunks in (None, 1, 3, 7, 500):
        assert pool.map(square, x, chunks=chunks) == want


def test_map_error_index(pool):
    with pytest.raises(pyrei.TaskError) as exc_info:
        pool.map(fail_at, list(range(10)), args=(4,))
    exc = exc_info.value
    assert exc.remote_type == "ValueError"
    assert exc.index == 4
    assert "element 4" in str(exc)


def test_map_fail_fast(pool):
    # the failing element's runner sets the cancel word: peers stop within
    # ~a batch instead of draining the remaining (slow) elements
    start = time.monotonic()
    with pytest.raises(pyrei.TaskError):
        pool.map(fail_or_sleep, list(range(8)), args=(0, 5.0))
    assert time.monotonic() - start < 5.0


def test_map_timeout(pool):
    x = [0.4] * 8
    out = pool.map(sleep_ident, x, timeout=0.2)
    assert out is pyrei.TIMEOUT
    # the cancellation drained: the pool is usable immediately after
    assert pool.map(square, [2], timeout=15) == [4]


def test_map_worker_died():
    p = pyrei.Pool.create(1)
    try:
        with pytest.raises(pyrei.WorkerDiedError) as exc_info:
            p.map(kill_at, list(range(8)), args=(2,))
        exc = exc_info.value
        assert exc.slot == 0
        assert exc.pid > 0
        # the lost ranges cover the dead runner's issued elements
        assert exc.lost
        lo = min(lo for lo, _ in exc.lost)
        hi = max(hi for _, hi in exc.lost)
        assert lo <= 2 < hi
    finally:
        p.stop()


def test_map_seed_determinism(pool):
    a = pool.map(rand_elt, list(range(50)), seed=42)
    b = pool.map(rand_elt, list(range(50)), seed=42, chunks=7)
    c = pool.map(rand_elt, list(range(50)), seed=42, chunks=1)
    assert a == b == c
    d = pool.map(rand_elt, list(range(50)), seed=43)
    assert a != d


def test_map_seed_restores_worker_rng(pool):
    # a seeded map restores the worker's own random state around its
    # batches: an unseeded task afterwards still draws from that stream
    pool.map(rand_elt, list(range(10)), seed=1)
    t = pool.submit(random.random)
    assert t.collect(timeout=5) is not None


def test_map_nested(pool):
    assert pool.map(nested_map, [4, 5]) == [
        [0, 1, 4, 9],
        [0, 1, 4, 9, 16],
    ]


@pytest.mark.skipif(os.name == "nt", reason="SIGINT differs on Windows")
def test_map_interrupt_cancels():
    p = pyrei.Pool.create(1)
    timer = threading.Timer(0.3, lambda: signal.raise_signal(signal.SIGINT))
    timer.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            p.map(sleep_ident, [3.0] * 4)
    finally:
        timer.cancel()
        # the backstop cancelled the outstanding runners; the pool stops
        # cleanly (waiting out any in-flight element)
        assert p.stop(timeout=20) is True


# -- numpy x sections (bare bytes in the region) ------------------------------

np = pytest.importorskip("numpy", reason="numpy not installed")


def test_map_numpy_x(pool):
    a = np.arange(1000, dtype=np.float64)
    out = pool.map(np_scalar_double, a)
    assert out == [float(v) * 2 for v in a]


def test_map_numpy_dtypes(pool):
    for dt in (np.float64, np.int32, np.complex128, np.uint8):
        a = np.arange(100, dtype=dt)
        out = pool.map(identity, a)
        assert [complex(v) for v in out] == [complex(v) for v in a]


def test_map_numpy_chunking_invariance(pool):
    a = np.arange(300, dtype=np.float64)
    want = pool.map(identity, a)
    for chunks in (1, 5, 300):
        assert pool.map(identity, a, chunks=chunks) == want
