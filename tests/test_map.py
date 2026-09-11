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
    bad_template,
    fail_at,
    fail_or_sleep,
    identity,
    kill_at,
    nested_map,
    np_scalar_double,
    pair_up,
    rand_elt,
    scalar_double,
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


def test_map_seed_offset(pool):
    # the split-map contract: seed=(s, n) over x[n:] continues seed=s —
    # element i of the shifted map draws stream i + n of the base map
    whole = pool.map(rand_elt, list(range(70)), seed=42)
    head = pool.map(rand_elt, list(range(70))[:50], seed=42)
    rest = pool.map(rand_elt, list(range(70))[50:], seed=(42, 50))
    assert head + rest == whole
    assert rest != whole[:20]
    with pytest.raises(TypeError, match="offset"):
        pool.map(rand_elt, [1], seed=(42, -1))
    with pytest.raises(TypeError, match="pair"):
        pool.map(rand_elt, [1], seed=(1, 2, 3))


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
    for dt in (np.float64, np.int32, np.complex128, np.uint8, np.int64):
        a = np.arange(100, dtype=dt)
        out = pool.map(identity, a)
        assert [complex(v) for v in out] == [complex(v) for v in a]


def test_map_numpy_int64_x(pool):
    # values past 2^53: the 64-bit width is exercised, not just the dtype
    a = np.arange(100, dtype=np.int64) * 2**53 + 1
    out = pool.map(identity, a)
    assert [int(v) for v in out] == [int(v) for v in a]


def test_map_numpy_chunking_invariance(pool):
    a = np.arange(300, dtype=np.float64)
    want = pool.map(identity, a)
    for chunks in (1, 5, 300):
        assert pool.map(identity, a, chunks=chunks) == want


# -- template output areas ----------------------------------------------------


def test_map_template_scalar(pool):
    out = pool.map(scalar_double, list(range(100)), template=np.empty(1))
    assert isinstance(out, np.ndarray)
    assert out.shape == (100,)
    assert out.dtype == np.float64
    np.testing.assert_array_equal(out, np.arange(100) * 2.0)


def test_map_template_m2(pool):
    out = pool.map(pair_up, list(range(50)), template=np.empty(2))
    assert out.shape == (50, 2)
    np.testing.assert_array_equal(out[:, 0], np.arange(50))
    np.testing.assert_array_equal(out[:, 1], np.arange(50) * 2)


def test_map_template_buffer_result(pool):
    # fn results may be buffers directly (no scalar conversion)
    out = pool.map(pair_up, list(range(40)), template=np.empty(2), chunks=3)
    assert out.shape == (40, 2)


def test_map_template_view(pool):
    out = pool.map(
        scalar_double, list(range(100)), template=np.empty(1), collect="view"
    )
    assert isinstance(out, np.ndarray)
    assert not out.flags.writeable
    np.testing.assert_array_equal(out, np.arange(100) * 2.0)


def test_map_template_mismatch(pool):
    with pytest.raises(pyrei.TaskError) as exc_info:
        pool.map(bad_template, list(range(10)), template=np.empty(2))
    assert exc_info.value.index == 0


def test_map_template_seeded(pool):
    a = pool.map(rand_elt, list(range(50)), seed=42, template=np.empty(1))
    b = pool.map(rand_elt, list(range(50)), seed=42, template=np.empty(1))
    np.testing.assert_array_equal(a, b)


def test_map_template_empty(pool):
    out = pool.map(scalar_double, [], template=np.empty(1))
    assert out.shape == (0,)


def test_map_template_int32(pool):
    out = pool.map(square, [1, 2, 3], template=np.empty(1, dtype=np.int32))
    assert out.dtype == np.int32
    assert list(out) == [1, 4, 9]


def test_map_template_invalid(pool):
    with pytest.raises(TypeError):
        pool.map(square, [1], template="not a buffer")
    with pytest.raises(ValueError):
        pool.map(square, [1], template=np.empty(1), collect="list")
    with pytest.raises(ValueError):
        pool.map(square, [1], collect="view")


# -- prepared maps ------------------------------------------------------------


def test_map_prepared_roundtrip(pool):
    pm = pool.map_prepare(square, list(range(500)))
    try:
        want = [i * i for i in range(500)]
        for _ in range(3):
            assert pool.map_run(pm) == want
    finally:
        pm.close()


def test_map_prepared_rearm_fences_stragglers(pool):
    # repeated runs over the same region: every run returns the full set
    pm = pool.map_prepare(identity, list(range(2000)), chunks=8)
    try:
        for _ in range(5):
            assert pool.map_run(pm) == list(range(2000))
    finally:
        pm.close()


def test_map_prepared_template(pool):
    pm = pool.map_prepare(
        scalar_double, list(range(100)), template=np.empty(1)
    )
    try:
        for _ in range(3):
            out = pool.map_run(pm)
            np.testing.assert_array_equal(out, np.arange(100) * 2.0)
    finally:
        pm.close()


def test_map_prepared_view_restages(pool):
    pm = pool.map_prepare(
        scalar_double, list(range(50)), template=np.empty(1), collect="view"
    )
    try:
        v1 = pool.map_run(pm)
        np.testing.assert_array_equal(v1, np.arange(50) * 2.0)
        # the view owns the first region; the second run restages fresh
        v2 = pool.map_run(pm)
        np.testing.assert_array_equal(v2, np.arange(50) * 2.0)
        np.testing.assert_array_equal(v1, np.arange(50) * 2.0)
    finally:
        pm.close()


def test_map_prepared_error_and_reuse(pool):
    pm = pool.map_prepare(fail_at, list(range(10)), args=(4,))
    try:
        with pytest.raises(pyrei.TaskError) as exc_info:
            pool.map_run(pm)
        assert exc_info.value.index == 4
        # the cancel word fired; the next run's re-arm clears it
        pm2 = pool.map_prepare(square, list(range(10)))
        try:
            assert pool.map_run(pm2) == [i * i for i in range(10)]
        finally:
            pm2.close()
    finally:
        pm.close()


def test_map_prepared_timeout(pool):
    pm = pool.map_prepare(sleep_ident, [0.4] * 8)
    try:
        assert pool.map_run(pm, timeout=0.2) is pyrei.TIMEOUT
        assert pool.map(square, [2], timeout=15) == [4]
    finally:
        pm.close()


def test_map_prepared_swap_x(pool):
    pm = pool.map_prepare(identity, np.arange(100, dtype=np.float64))
    try:
        name = pm._name
        out = pool.map_run(pm)
        assert [float(v) for v in out] == [float(v) for v in np.arange(100)]
        # same dtype and length: an in-place swap — the region is reused
        x2 = np.arange(100, dtype=np.float64) * 10
        out = pool.map_run(pm, x=x2)
        assert [float(v) for v in out] == [float(v) for v in x2]
        assert pm._name == name
    finally:
        pm.close()


def test_map_prepared_swap_x_template(pool):
    pm = pool.map_prepare(
        scalar_double, np.arange(100, dtype=np.float64), template=np.empty(1)
    )
    try:
        np.testing.assert_array_equal(pool.map_run(pm), np.arange(100) * 2.0)
        name = pm._name
        out = pool.map_run(pm, x=np.ones(100))
        np.testing.assert_array_equal(out, np.ones(100) * 2.0)
        assert pm._name == name
    finally:
        pm.close()


def test_map_prepared_replace_x_restages(pool):
    pm = pool.map_prepare(identity, np.arange(50, dtype=np.float64))
    try:
        pool.map_run(pm)
        name = pm._name
        # a dtype change restages even at the same length
        out = pool.map_run(pm, x=np.arange(50, dtype=np.int32))
        assert [int(v) for v in out] == list(range(50))
        assert pm._name != name
        # a different length restages
        out = pool.map_run(pm, x=np.arange(10, dtype=np.float64))
        assert [float(v) for v in out] == [float(v) for v in np.arange(10)]
        # a non-buffer x restages onto the descriptor path
        assert pool.map_run(pm, x=[1, 2, 3]) == [1, 2, 3]
        # and a raw-buffer x stages again afterwards
        out = pool.map_run(pm, x=np.ones(4))
        assert [float(v) for v in out] == [1.0] * 4
        # an empty replacement short-circuits
        assert pool.map_run(pm, x=[]) == []
    finally:
        pm.close()


def test_map_prepared_closed(pool):
    pm = pool.map_prepare(square, [1, 2])
    pm.close()
    pm.close()
    assert pm.closed
    with pytest.raises(pyrei.ReiError):
        pool.map_run(pm)


def test_map_prepared_wrong_pool(pool):
    other = pyrei.Pool.create(1)
    try:
        pm = other.map_prepare(square, [1])
        try:
            with pytest.raises(ValueError):
                pool.map_run(pm)
        finally:
            pm.close()
    finally:
        other.stop()


def test_map_prepared_context_manager(pool):
    with pool.map_prepare(square, [3]) as pm:
        assert pool.map_run(pm) == [9]
    assert pm.closed
