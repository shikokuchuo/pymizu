"""Pool.map tests: round-trip and order, chunking invariance, the blob and
region paths, raw-buffer x sections, the outcome taxonomy (element-indexed
errors, fail-fast, timeout, worker death), seed determinism, nested maps,
and the interrupt backstop — over real spawned workers."""

import operator
import os
import random
import signal
import sys
import threading
import time

import pytest
from tests.helpers import (
    bad_template,
    current_rng_or_none,
    fail_at,
    fail_or_sleep,
    huge_int,
    identity,
    kill_at,
    mul_add,
    nested_map,
    np_rand_elt,
    np_rand_pair,
    np_scalar_double,
    pair_up,
    pair_up_i64,
    rand_elt,
    scalar_double,
    scale_add,
    sleep_ident,
    sleep_on,
    square,
)

import pymizu


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


def test_map_roundtrip(pool):
    assert pool.map(square, list(range(10))).collect() == [
        i * i for i in range(10)
    ]


def test_map_empty(pool):
    assert pool.map(square, []).collect() == []


def test_map_args_kwargs(pool):
    assert pool.map(pow, [1, 2, 3], args=(2,)).collect() == [1, 4, 9]
    assert pool.map(
        scale_add, [1, 2], kwargs={"scale": 2, "add": 1}
    ).collect() == [
        3,
        5,
    ]


def test_starmap(pool):
    assert pool.starmap(pow, [(2, 10), (3, 3), (4, 0)]).collect() == [
        1024,
        27,
        1,
    ]


def test_starmap_zip_idiom(pool):
    xs, ys = [1, 2, 3], [4, 5, 6]
    assert pool.starmap(operator.mul, zip(xs, ys, strict=True)).collect() == [
        4,
        10,
        18,
    ]


def test_starmap_args_kwargs(pool):
    assert pool.starmap(mul_add, [(1, 2), (3, 4)], args=(10,)).collect() == [
        12,
        22,
    ]
    assert pool.starmap(mul_add, [(1, 2)], kwargs={"c": 100}).collect() == [
        102
    ]


def test_starmap_generator_and_rows(pool):
    import numpy as np

    assert pool.starmap(pow, ((i, 2) for i in range(4))).collect() == [
        0,
        1,
        4,
        9,
    ]
    assert pool.starmap(
        operator.add, np.arange(6).reshape(3, 2)
    ).collect() == [1, 5, 9]


def test_starmap_empty(pool):
    assert pool.starmap(pow, []).collect() == []


def test_map_nan_timeout_rejected(pool):
    with pytest.raises(ValueError, match="not NaN"):
        pool.map(square, [1, 2, 3], timeout=float("nan"))


def test_starmap_spec_rejected(pool):
    with pytest.raises(TypeError, match="starmap"):
        pool.starmap(pymizu.call("math.sqrt"), [(1.0,)])


def test_starmap_fn_not_callable(pool):
    with pytest.raises(TypeError, match="callable"):
        pool.starmap(None, [(1, 2)])


def test_starmap_error_index(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.starmap(fail_at, [(0, 99), (4, 4)]).collect()
    assert exc_info.value.index == 1


def test_starmap_template(pool):
    import numpy as np

    out = pool.starmap(
        mul_add, [(1, 2), (3, 4)], template=np.empty(1)
    ).collect()
    np.testing.assert_array_equal(out, np.array([2.0, 12.0]))


def test_starmap_stream(pool):
    assert pool.starmap(
        pow, [(i, 2) for i in range(10)], stream=True
    ).collect() == [i * i for i in range(10)]


def test_map_blob_path(pool):
    # a small map whose chunk payloads fit the entry inline budget: no
    # region is created (the morsel machinery needs one)
    assert pool.map(square, [3, 1, 2]).collect() == [9, 1, 4]


def test_map_region_path(pool):
    # a descriptor past the inline budget forces the region path
    x = list(range(2000))
    assert pool.map(identity, x).collect() == x


def test_map_fn_not_callable(pool):
    with pytest.raises(TypeError, match="callable"):
        pool.map(42, [1])


def test_map_n_chunks_zero(pool):
    with pytest.raises(ValueError, match="n_chunks"):
        pool.map(square, [1], n_chunks=0)


def test_map_blob_submit_timeout(pool):
    # the deadline pre-check in the chunk-submit loop, ahead of any
    # payload build: timeout=0 expires it on the first iteration
    assert pool.map(square, [1, 2, 3], timeout=0) is pymizu.TIMEOUT


def test_map_region_submit_timeout(pool):
    # the same pre-check on the region path's runner submits
    assert pool.map(identity, list(range(2000)), timeout=0) is pymizu.TIMEOUT


def test_map_prepared_submit_timeout(pool):
    pm = pool.map_prepare(square, list(range(2000)))
    try:
        assert pm.run(timeout=0) is pymizu.TIMEOUT
    finally:
        pm.close()


def test_prepared_run_dropped_uncollected_restages(pool):
    # a dropped uncollected run cancels and drops the stage: the next run
    # restages into a fresh region
    import gc

    pm = pool.map_prepare(square, list(range(2000)))
    try:
        m = pm.run()
        del m
        gc.collect()
        assert pm.run(timeout=5).collect() == [i * i for i in range(2000)]
    finally:
        pm.close()


def test_map_slots_exhausted():
    # no free result slot: the map fails fast rather than queuing runners
    p = pymizu.Pool.create(1, max_submitters=1, result_slots=2)
    try:
        t1 = p.submit(time.sleep, 0.5)
        t2 = p.submit(time.sleep, 0.5)
        with pytest.raises(pymizu.SlotsExhaustedError, match="result slots"):
            p.map(square, [1, 2], timeout=5)
        pm = p.map_prepare(square, list(range(2000)))
        try:
            with pytest.raises(
                pymizu.SlotsExhaustedError, match="result slots"
            ):
                pm.run(timeout=5)
        finally:
            pm.close()
        t1.collect(timeout=5)
        t2.collect(timeout=5)
        assert p.map(square, [2], timeout=5).collect() == [4]
    finally:
        p.stop()


def test_map_chunking_invariance(pool):
    x = list(range(500))
    want = [i * i for i in x]
    for chunks in (None, 1, 3, 7, 500):
        assert pool.map(square, x, n_chunks=chunks).collect() == want


def test_map_error_index(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(fail_at, list(range(10)), args=(4,)).collect()
    exc = exc_info.value
    assert exc.remote_type == "ValueError"
    assert exc.index == 4
    assert "element 4" in str(exc)


def test_map_fail_fast(pool):
    # the failing element's runner sets the cancel word: peers stop within
    # ~a batch instead of draining the remaining (slow) elements
    start = time.monotonic()
    with pytest.raises(pymizu.TaskError):
        pool.map(fail_or_sleep, list(range(8)), args=(0, 5.0)).collect()
    assert time.monotonic() - start < 5.0


def test_map_timeout(pool):
    x = [0.4] * 8
    out = pool.map(sleep_ident, x).collect(timeout=0.2)
    assert out is pymizu.TIMEOUT
    # the cancellation drained: the pool is usable immediately after
    assert pool.map(square, [2], timeout=15).collect() == [4]


def test_map_double_collect_raises(pool):
    m = pool.map(square, range(4))
    assert m.collect() == [0, 1, 4, 9]
    with pytest.raises(pymizu.MizuError, match="already collected"):
        m.collect()


def test_map_region_cancel_mid_collect(pool):
    # a region map cancelled with its runners still queued behind the
    # blockers: every runner abandons, nothing splices, and the pool is
    # usable immediately after
    blockers = [pool.submit(time.sleep, 2) for _ in range(2)]
    m = pool.map(sleep_on, list(range(2000)), args=(set(range(2000)), 0.05))
    timer = threading.Timer(0.2, m.cancel)
    timer.start()
    try:
        with pytest.raises(pymizu.CancelledError, match="cancelled"):
            m.collect(timeout=15)
    finally:
        timer.cancel()
        assert [b.collect(timeout=5) for b in blockers] == [None, None]
    assert pool.map(square, [2], timeout=15).collect() == [4]


def test_map_worker_died():
    p = pymizu.Pool.create(1)
    try:
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
            p.map(kill_at, list(range(8)), args=(2,)).collect()
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


def test_map_queued_behind_a_dead_worker_fails_not_hangs():
    # the lone worker is inside the blocker, so the map's runners only
    # ever queue: the kill strands them with no claimant and no announce,
    # so no result slot ever carries the death verdict — before the
    # registry read in the collect loop this map parked to its timeout
    import numpy as np

    p = pymizu.Pool.create(1)
    try:
        pid = p.dump()["workers"][0]["pid"]
        blocker = p.submit(sleep_ident, 30.0)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if any(w["in_flight"] != -1 for w in p.dump()["workers"]):
                break
            time.sleep(0.05)
        else:
            pytest.fail("worker did not claim the blocker")
        x = np.arange(1000.0)  # a raw x section: the region path
        # the map submits its runners while the worker is pinned: they
        # queue, and the kill strands them
        out, err = [], []

        def run():
            try:
                out.append(p.map(identity, x).collect(timeout=15))
            except Exception as e:  # noqa: BLE001
                err.append(e)

        t = threading.Thread(target=run)
        t.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if any(s["queued"] for s in p.dump()["submitters"]):
                break
            time.sleep(0.05)
        else:
            pytest.fail("map runners did not queue")
        # SIGTERM on Windows: TerminateProcess, a hard kill either way
        os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        t.join()
        assert not out
        assert len(err) == 1 and isinstance(err[0], pymizu.WorkerDiedError)
        assert err[0].slot is None
        assert err[0].pid is None
        blocker.cancel()
    finally:
        p.stop()


def test_map_seed_determinism(pool):
    a = pool.map(rand_elt, list(range(50)), seed=42).collect()
    b = pool.map(rand_elt, list(range(50)), seed=42, n_chunks=7).collect()
    c = pool.map(rand_elt, list(range(50)), seed=42, n_chunks=1).collect()
    assert a == b == c
    d = pool.map(rand_elt, list(range(50)), seed=43).collect()
    assert a != d


def test_map_seed_offset(pool):
    # the split-map contract: seed=(s, n) over x[n:] continues seed=s —
    # element i of the shifted map draws stream i + n of the base map
    whole = pool.map(rand_elt, list(range(70)), seed=42).collect()
    head = pool.map(rand_elt, list(range(70))[:50], seed=42).collect()
    rest = pool.map(rand_elt, list(range(70))[50:], seed=(42, 50)).collect()
    assert head + rest == whole
    assert rest != whole[:20]
    with pytest.raises(TypeError, match="offset"):
        pool.map(rand_elt, [1], seed=(42, -1))
    with pytest.raises(TypeError, match="pair"):
        pool.map(rand_elt, [1], seed=(1, 2, 3))


def test_map_seed_bad_type(pool):
    with pytest.raises(TypeError, match="int or bytes"):
        pool.map(rand_elt, [1], seed=1.5)
    with pytest.raises(TypeError, match="int or bytes"):
        pool.map(rand_elt, [1], seed=True)


def test_map_seed_bytes(pool):
    # a bytes seed is carried as-is (an int seed crosses as its ascii form)
    a = pool.map(rand_elt, list(range(30)), seed=b"raw").collect()
    b = pool.map(rand_elt, list(range(30)), seed=b"raw", n_chunks=7).collect()
    c = pool.map(rand_elt, list(range(30)), seed=bytearray(b"raw")).collect()
    assert a == b == c


def test_map_seed_restores_worker_rng(pool):
    # a seeded map restores the worker's own random state around its
    # batches: an unseeded task afterwards still draws from that stream
    pool.map(rand_elt, list(range(10)), seed=1).collect()
    t = pool.submit(random.random)
    assert t.collect(timeout=5) is not None


def test_map_seed_stdlib_derivation_unchanged(pool):
    # golden constants pin the stdlib per-element derivation against the
    # current_rng stash edit: random.seed(SHA-256(seed_bytes +
    # i.to_bytes(8, "little"))) — random.random() is stable across
    # CPython versions
    out = pool.map(rand_elt, list(range(4)), seed=42).collect()
    assert out == [
        0.42631878050691485,
        0.5101559966523063,
        0.7226535016557073,
        0.3758578598845185,
    ]


def test_current_rng_outside_seeded_element(pool):
    # None in the submitter, in an ordinary task, and in an unseeded map
    assert pymizu.current_rng() is None
    t = pool.submit(current_rng_or_none)
    assert t.collect(timeout=5) is None
    assert pool.map(current_rng_or_none, [1, 2, 3]).collect() == [
        None,
        None,
        None,
    ]


def test_map_nested(pool):
    assert pool.map(nested_map, [4, 5]).collect() == [
        [0, 1, 4, 9],
        [0, 1, 4, 9, 16],
    ]


@pytest.mark.skipif(os.name == "nt", reason="SIGINT differs on Windows")
def test_map_interrupt_cancels():
    p = pymizu.Pool.create(1)
    timer = threading.Timer(0.3, lambda: signal.raise_signal(signal.SIGINT))
    timer.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            p.map(sleep_ident, [3.0] * 4).collect()
    finally:
        timer.cancel()
        # the backstop cancelled the outstanding runners; the pool stops
        # cleanly (waiting out any in-flight element)
        assert p.stop(timeout=20) is True


# -- numpy x sections (bare bytes in the region) ------------------------------

np = pytest.importorskip("numpy", reason="numpy not installed")


def test_map_numpy_x(pool):
    a = np.arange(1000, dtype=np.float64)
    out = pool.map(np_scalar_double, a).collect()
    assert out == [float(v) * 2 for v in a]


def test_map_numpy_dtypes(pool):
    for dt in (np.float64, np.int32, np.complex128, np.uint8, np.int64):
        a = np.arange(100, dtype=dt)
        out = pool.map(identity, a).collect()
        assert [complex(v) for v in out] == [complex(v) for v in a]


def test_map_numpy_int64_x(pool):
    # values past 2^53: the 64-bit width is exercised, not just the dtype
    a = np.arange(100, dtype=np.int64) * 2**53 + 1
    out = pool.map(identity, a).collect()
    assert [int(v) for v in out] == [int(v) for v in a]


def test_map_numpy_chunking_invariance(pool):
    a = np.arange(300, dtype=np.float64)
    want = pool.map(identity, a).collect()
    for chunks in (1, 5, 300):
        assert pool.map(identity, a, n_chunks=chunks).collect() == want


# -- seeded numpy streams (pymizu.current_rng) --------------------------------


def test_map_seed_numpy_determinism(pool):
    a = pool.map(np_rand_elt, list(range(50)), seed=42).collect()
    b = pool.map(np_rand_elt, list(range(50)), seed=42, n_chunks=7).collect()
    c = pool.map(np_rand_elt, list(range(50)), seed=42, n_chunks=1).collect()
    assert a == b == c
    d = pool.map(np_rand_elt, list(range(50)), seed=43).collect()
    assert a != d


def test_map_seed_numpy_offset(pool):
    # the split-map contract holds for numpy draws: element i of the
    # shifted map draws stream i + n of the base map
    x = list(range(70))
    whole = pool.map(np_rand_elt, x, seed=42).collect()
    head = pool.map(np_rand_elt, x[:50], seed=42).collect()
    rest = pool.map(np_rand_elt, x[50:], seed=(42, 50)).collect()
    assert head + rest == whole
    assert rest != whole[:20]


def test_map_seed_numpy_memoized_per_element(pool):
    out = pool.map(np_rand_pair, list(range(20)), seed=42).collect()
    # two calls in one element continue one stream (the memoized object)
    assert all(same and a != b for a, b, same in out)
    # distinct elements get distinct streams
    assert len({a for a, _, _ in out}) == 20


def test_map_seed_numpy_template(pool):
    a = pool.map(
        np_rand_elt, list(range(50)), seed=42, template=np.empty(1)
    ).collect()
    b = pool.map(
        np_rand_elt, list(range(50)), seed=42, template=np.empty(1)
    ).collect()
    np.testing.assert_array_equal(a, b)


def test_map_seed_numpy_prepared_rerun(pool):
    # a re-armed prepared run restarts every element's stream
    pm = pool.map_prepare(np_rand_elt, list(range(30)))
    try:
        assert pm.run(seed=42).collect() == pm.run(seed=42).collect()
    finally:
        pm.close()


def test_map_prepared_run_seed_is_per_run(pool):
    pm = pool.map_prepare(rand_elt, list(range(30)))
    try:
        first = pm.run(seed=1).collect()
        assert pm.run(seed=1).collect() == first
        assert pm.run(seed=2).collect() != first
        # None is unseeded, whatever the previous run took
        assert pm.run().collect() != first
    finally:
        pm.close()


def test_map_prepared_run_validates_seed_and_collect(pool):
    pm = pool.map_prepare(scalar_double, list(range(4)))
    try:
        with pytest.raises(TypeError, match="seed must be an int or bytes"):
            pm.run(seed="bad")
        with pytest.raises(
            ValueError, match="collect='view' requires a template"
        ):
            pm.run(collect="view")
        with pytest.raises(TypeError):  # seed/timeout/collect are keyword-only
            pm.run(None, 42)
    finally:
        pm.close()


def test_map_seed_numpy_batch_resets_memo():
    # the stash and the one-slot memo die with the batch: the same
    # element key in a later batch rebuilds its Generator from the
    # digest (run-to-run determinism of a re-armed prepared map)
    from pymizu import _map

    def draw(i):
        return pymizu.current_rng().random()

    get = [0].__getitem__
    a = _map._run_batch(draw, (), {}, get, 0, 1, (b"s", 0))
    assert _map._elt_key is None
    assert _map._run_batch(draw, (), {}, get, 0, 1, (b"s", 0)) == a


# -- template output areas ----------------------------------------------------


def test_map_template_scalar(pool):
    out = pool.map(
        scalar_double, list(range(100)), template=np.empty(1)
    ).collect()
    assert isinstance(out, np.ndarray)
    assert out.shape == (100,)
    assert out.dtype == np.float64
    np.testing.assert_array_equal(out, np.arange(100) * 2.0)


def test_map_template_m2(pool):
    out = pool.map(pair_up, list(range(50)), template=np.empty(2)).collect()
    assert out.shape == (50, 2)
    np.testing.assert_array_equal(out[:, 0], np.arange(50))
    np.testing.assert_array_equal(out[:, 1], np.arange(50) * 2)


def test_map_template_buffer_result(pool):
    # fn results may be buffers directly (no scalar conversion)
    out = pool.map(
        pair_up, list(range(40)), template=np.empty(2), n_chunks=3
    ).collect()
    assert out.shape == (40, 2)


def test_map_template_view(pool):
    out = pool.map(
        scalar_double, list(range(100)), template=np.empty(1), collect="view"
    ).collect()
    assert isinstance(out, np.ndarray)
    assert not out.flags.writeable
    np.testing.assert_array_equal(out, np.arange(100) * 2.0)


def test_map_template_mismatch(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(bad_template, list(range(10)), template=np.empty(2)).collect()
    assert exc_info.value.index == 0


def test_map_template_seeded(pool):
    a = pool.map(
        rand_elt, list(range(50)), seed=42, template=np.empty(1)
    ).collect()
    b = pool.map(
        rand_elt, list(range(50)), seed=42, template=np.empty(1)
    ).collect()
    np.testing.assert_array_equal(a, b)


def test_map_template_empty(pool):
    out = pool.map(scalar_double, [], template=np.empty(1)).collect()
    assert out.shape == (0,)


def test_map_template_int32(pool):
    out = pool.map(
        square, [1, 2, 3], template=np.empty(1, dtype=np.int32)
    ).collect()
    assert out.dtype == np.int32
    assert list(out) == [1, 4, 9]


def test_map_template_int64_scalar(pool):
    out = pool.map(
        square, [1, 2, 3], template=np.empty(1, dtype=np.int64)
    ).collect()
    assert out.dtype == np.int64
    assert list(out) == [1, 4, 9]


def test_map_template_int64_buffer_result(pool):
    out = pool.map(
        pair_up_i64, list(range(40)), template=np.empty(2, dtype=np.int64)
    ).collect()
    assert out.shape == (40, 2)
    assert out.dtype == np.int64
    np.testing.assert_array_equal(out[:, 0], np.arange(40))
    np.testing.assert_array_equal(out[:, 1], np.arange(40) * 2)


def test_map_template_int64_view(pool):
    out = pool.map(
        square,
        list(range(50)),
        template=np.empty(1, dtype=np.int64),
        collect="view",
    ).collect()
    assert isinstance(out, np.ndarray)
    assert out.dtype == np.int64
    assert not out.flags.writeable
    np.testing.assert_array_equal(out, np.arange(50) ** 2)


def test_map_template_int64_mismatch(pool):
    # an int past INT64_MAX fails the element write as a template mismatch
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(
            huge_int, [1, 2, 3], template=np.empty(1, dtype=np.int64)
        ).collect()
    assert exc_info.value.index == 0


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
            assert pm.run().collect() == want
    finally:
        pm.close()


def test_map_prepared_rearm_fences_stragglers(pool):
    # repeated runs over the same region: every run returns the full set
    pm = pool.map_prepare(identity, list(range(2000)), n_chunks=8)
    try:
        for _ in range(5):
            assert pm.run().collect() == list(range(2000))
    finally:
        pm.close()


def test_map_write_rejects_a_wrapping_range(pool):
    # lo near UINT64_MAX: the range check must not wrap past the bound
    pm = pool.map_prepare(abs, [1, 2, 3, 4], template=np.empty(1))
    try:
        with pytest.raises(pymizu.MizuError, match="out of bounds"):
            pymizu._pymizu._map_write(pm._capsule, 2**64 - 1, [1.0, 2.0])
    finally:
        pm.close()


def test_map_cancel_word_hooks(pool):
    # the fail-fast word: an erroring runner sets it ahead of its ERR
    # publish, the re-arm clears it, and a stale-generation runner then
    # claims nothing (the generation fence)
    import pickle

    from pymizu import _map

    desc = pickle.dumps((fail_at, (2,), {}, [0, 1, 2, 3]), 4)
    name, capsule = pymizu._pymizu._map_stage(desc, None, 4, 1, None, False, 1)
    try:
        assert pymizu._pymizu._map_cancel_get(capsule) is False
        with pytest.raises(pymizu.TaskError, match="element 2"):
            pool.submit(_map._runner_ix, name, 0, None).collect(timeout=10)
        assert pymizu._pymizu._map_cancel_get(capsule) is True
        assert pymizu._pymizu._map_reset(capsule) == 1
        assert pymizu._pymizu._map_cancel_get(capsule) is False
        out = pool.submit(_map._runner_ix, name, 0, None).collect(timeout=10)
        assert out == ([], [])
    finally:
        _map._ctx_cache.pop(name, None)
        pymizu._pymizu._map_close(capsule)


def test_map_lost_with_one_shot_histories(pool):
    # a generator history is materialized once — a second sizing pass
    # must not desync the allocation from the fill
    pm = pool.map_prepare(abs, [1, 2, 3, 4])
    try:
        out = pymizu._pymizu._map_lost(pm._capsule, [iter([(0, 2)]), [(2, 4)]])
        assert out == []  # nothing issued yet, so nothing lost
    finally:
        pm.close()


def test_map_prepared_template(pool):
    pm = pool.map_prepare(
        scalar_double, list(range(100)), template=np.empty(1)
    )
    try:
        for _ in range(3):
            out = pm.run().collect()
            np.testing.assert_array_equal(out, np.arange(100) * 2.0)
    finally:
        pm.close()


def test_map_prepared_empty_template(pool):
    # an empty x stages nothing: the run gathers an empty output area
    pm = pool.map_prepare(scalar_double, [], template=np.empty(2))
    try:
        out = pm.run().collect()
        assert out.shape == (0, 2)
        assert out.dtype == np.float64
    finally:
        pm.close()


def test_map_prepared_view_restages(pool):
    pm = pool.map_prepare(scalar_double, list(range(50)), template=np.empty(1))
    try:
        v1 = pm.run(collect="view").collect()
        np.testing.assert_array_equal(v1, np.arange(50) * 2.0)
        # the view owns the first region; the second run restages fresh
        v2 = pm.run(collect="view").collect()
        np.testing.assert_array_equal(v2, np.arange(50) * 2.0)
        np.testing.assert_array_equal(v1, np.arange(50) * 2.0)
    finally:
        pm.close()


def test_map_prepared_error_and_reuse(pool):
    pm = pool.map_prepare(fail_at, list(range(10)), args=(4,))
    try:
        with pytest.raises(pymizu.TaskError) as exc_info:
            pm.run().collect()
        assert exc_info.value.index == 4
        # the cancel word fired; the next run's re-arm clears it
        pm2 = pool.map_prepare(square, list(range(10)))
        try:
            assert pm2.run().collect() == [i * i for i in range(10)]
        finally:
            pm2.close()
    finally:
        pm.close()


def test_map_prepared_timeout(pool):
    pm = pool.map_prepare(sleep_ident, [0.4] * 8)
    try:
        assert pm.run().collect(timeout=0.2) is pymizu.TIMEOUT
        assert pool.map(square, [2], timeout=15).collect() == [4]
    finally:
        pm.close()


def test_map_prepared_unclean_run_restages(pool):
    # a timed-out run's straggler faces no generation fence on its
    # output-area write: the unclean run drops the stage, so the next run
    # restages into a fresh region and the straggler writes into the void
    pm = pool.map_prepare(
        sleep_on,
        np.array([100.0, 1.0, 2.0, 3.0]),
        args=({100.0}, 2.0),
        template=np.zeros(1),
    )
    try:
        assert pm.run(collect="view").collect(timeout=0.3) is pymizu.TIMEOUT
        out = pm.run(
            np.array([200.0, 1.0, 2.0, 3.0]), timeout=15, collect="view"
        ).collect()
        np.testing.assert_array_equal(np.asarray(out), [200.0, 1.0, 2.0, 3.0])
        time.sleep(2.2)  # the straggler's write lands now, into the void
        np.testing.assert_array_equal(np.asarray(out), [200.0, 1.0, 2.0, 3.0])
    finally:
        pm.close()


def test_map_prepared_swap_x(pool):
    pm = pool.map_prepare(identity, np.arange(100, dtype=np.float64))
    try:
        name = pm._name
        out = pm.run().collect()
        assert [float(v) for v in out] == [float(v) for v in np.arange(100)]
        # same dtype and length: an in-place swap — the region is reused
        x2 = np.arange(100, dtype=np.float64) * 10
        out = pm.run(x=x2).collect()
        assert [float(v) for v in out] == [float(v) for v in x2]
        assert pm._name == name
    finally:
        pm.close()


def test_map_prepared_swap_x_template(pool):
    pm = pool.map_prepare(
        scalar_double, np.arange(100, dtype=np.float64), template=np.empty(1)
    )
    try:
        np.testing.assert_array_equal(pm.run().collect(), np.arange(100) * 2.0)
        name = pm._name
        out = pm.run(x=np.ones(100)).collect()
        np.testing.assert_array_equal(out, np.ones(100) * 2.0)
        assert pm._name == name
    finally:
        pm.close()


def test_map_prepared_replace_x_restages(pool):
    pm = pool.map_prepare(identity, np.arange(50, dtype=np.float64))
    try:
        pm.run().collect()
        name = pm._name
        # a dtype change restages even at the same length
        out = pm.run(x=np.arange(50, dtype=np.int32)).collect()
        assert [int(v) for v in out] == list(range(50))
        assert pm._name != name
        # a different length restages
        out = pm.run(x=np.arange(10, dtype=np.float64)).collect()
        assert [float(v) for v in out] == [float(v) for v in np.arange(10)]
        # a non-buffer x restages onto the descriptor path
        assert pm.run(x=[1, 2, 3]).collect() == [1, 2, 3]
        # and a raw-buffer x stages again afterwards
        out = pm.run(x=np.ones(4)).collect()
        assert [float(v) for v in out] == [1.0] * 4
        # an empty replacement short-circuits
        assert pm.run(x=[]).collect() == []
    finally:
        pm.close()


def test_map_prepared_closed(pool):
    pm = pool.map_prepare(square, [1, 2])
    pm.close()
    pm.close()
    assert pm.closed
    with pytest.raises(pymizu.MizuError):
        pm.run()


def test_map_prepared_context_manager(pool):
    with pool.map_prepare(square, [3]) as pm:
        assert pm.run().collect() == [9]
    assert pm.closed


def test_map_spec_name_kind(pool):
    # a spec f always takes the region path: kind-2 runner tasks with the
    # 'I' descriptor, even on a same-language pool
    out = pool.map(pymizu.call("math.sqrt"), [1.0, 4.0, 9.0]).collect()
    assert out == [1.0, 2.0, 3.0]
    assert pool.map(pymizu.call("builtins.len"), [[1, 2], [3]]).collect() == [
        2,
        1,
    ]


def test_map_spec_source_kind(pool):
    assert pool.map(pymizu.call(source="x * 2"), [1, 2, 3]).collect() == [
        2,
        4,
        6,
    ]
    # the element binds as x, named constants as names, positional as _1
    assert pool.map(pymizu.call(source="x + k", k=10), [1, 2]).collect() == [
        11,
        12,
    ]
    assert pool.map(
        pymizu.call(None, 10, source="x * _1"), [1, 2]
    ).collect() == [
        10,
        20,
    ]
    # a statement prefix runs per element; the trailing expression's value
    src = "import math\nmath.floor(x)"
    assert pool.map(pymizu.call(source=src), [1.7, 2.3]).collect() == [1, 2]


def test_map_spec_source_no_tail(pool):
    # a source of statements only has no trailing expression: the
    # per-element result is None
    assert pool.map(pymizu.call(source="y = x + 1"), [1, 2]).collect() == [
        None,
        None,
    ]


def test_map_spec_stream_worker_died():
    # the position stamp names the dead chunk; its fixed range is the
    # lost set (the blob path's precedent)
    p = pymizu.Pool.create(1)
    try:
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
            p.map(
                pymizu.call(source="import os\nos._exit(1) if x == 0 else x"),
                [0, 1, 2],
                stream=True,
            ).collect(timeout=15)
        assert exc_info.value.lost == [(0, 1)]
    finally:
        p.stop()


def test_map_spec_stream_timeout(pool):
    out = pool.map(
        pymizu.call(source="import time\ntime.sleep(2)\nx"),
        [1, 2],
        stream=True,
    ).collect(timeout=0.3)
    assert out is pymizu.TIMEOUT
    # the backstop cancelled the outstanding chunk: the pool is usable
    assert pool.map(
        pymizu.call(source="x * 2"), [3], timeout=15
    ).collect() == [6]


def test_map_spec_stream_refill_timeout(pool):
    # the deadline pre-check in the spec window's refill, ahead of any
    # chunk stage: timeout=0 expires it on the first iteration
    out = pool.map(pymizu.call(source="x"), [1, 2], stream=True, timeout=0)
    assert out is pymizu.TIMEOUT


def test_resolve_name_malformed():
    from pymizu import _map

    with pytest.raises(pymizu.MizuError, match="not qualified"):
        _map._resolve_name("nodots")


def test_runner_ix_ordinal_out_of_range(pool):
    # a runner ordinal past the descriptor's CLAIM capacity is a malformed
    # runner stream, caught before any claim
    import pickle

    from pymizu import _map

    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = pymizu._pymizu._map_stage(desc, None, 6, 1, None, False, 3)
    try:
        claim_n = pymizu._pymizu._map_header(capsule)["claim_n"]
        with pytest.raises(pymizu.MizuError, match="out of range"):
            _map._runner_ix(name, claim_n << 32, None)
    finally:
        _map._ctx_cache.pop(name, None)
        pymizu._pymizu._map_close(capsule)


def test_collect_region_fatal_runner_error(pool):
    # a runner-level failure that is not fn's carries no element index:
    # the collect raises it as fatal rather than merging it into the
    # first-by-element-index set. An ordinary failing task stands in for
    # the runner (the _exec_fail precedent).
    import pickle

    from pymizu import _map

    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = pymizu._pymizu._map_stage(desc, None, 6, 1, None, False, 3)
    try:
        h = pool.submit(fail_at, 0, 0)
        remaining, expired = _map._deadline(10)
        with pytest.raises(pymizu.TaskError) as exc_info:
            _map._collect_region(
                pymizu, pool, capsule, [h], 6, remaining, expired
            )
        assert not hasattr(exc_info.value, "index")
    finally:
        _map._ctx_cache.pop(name, None)
        pymizu._pymizu._map_close(capsule)


def test_collect_region_deferral_timeout(pool):
    # every handle defers (nothing claimed) and the deadline expires in
    # the park-slice branch: TIMEOUT. The scripted expiry stands in for a
    # deadline landing between the two checks of one pass.
    import pickle

    from pymizu import _map

    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = pymizu._pymizu._map_stage(desc, None, 6, 1, None, False, 3)
    try:
        h = pool.submit(sleep_ident, 1)
        remaining, _ = _map._deadline(10)
        checks = iter([False, True])
        out = _map._collect_region(
            pymizu,
            pool,
            capsule,
            [h],
            6,
            remaining,
            lambda: next(checks),
        )
        assert out is pymizu.TIMEOUT
        h.cancel()
    finally:
        _map._ctx_cache.pop(name, None)
        pymizu._pymizu._map_close(capsule)


def test_map_guard_no_worker():
    # a spec fn needs the pool word already set (the descriptor's target
    # byte stages once): no worker joined yet
    h = pymizu._pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        pool0 = pymizu.Pool._wrap(h)
        with pytest.raises(pymizu.MizuError, match="no worker has joined"):
            pool0.map(pymizu.call("math.sqrt"), [1.0])
    finally:
        h.destroy()


def test_map_spec_held_in_a_variable(pool):
    fn = pymizu.call("math.floor")
    assert pool.map(fn, [1.7, 2.3]).collect() == [1, 2]


def test_map_spec_constants_only_in_spec(pool):
    with pytest.raises(TypeError, match="must be empty with a spec"):
        pool.map(pymizu.call("math.sqrt"), [1.0], args=(2,))
    with pytest.raises(TypeError, match="must be empty with a spec"):
        pool.map(pymizu.call("math.sqrt"), [1.0], kwargs={"digits": 1})
    with pytest.raises(TypeError, match="must be empty with a spec"):
        pool.map_prepare(pymizu.call("math.sqrt"), [1.0], args=(2,))


def test_map_spec_seed_gates(pool):
    with pytest.raises(TypeError, match="no i64 form"):
        pool.map(pymizu.call("math.sqrt"), [1.0], seed=b"raw")
    with pytest.raises(TypeError, match="must fit an int64"):
        pool.map(pymizu.call("math.sqrt"), [1.0], seed=2**63)


def test_map_spec_declines_non_portable(pool):
    with pytest.raises(pymizu.DeclinedError):
        pool.map(pymizu.call("builtins.print", object()), [1, 2])


def test_map_spec_raw_x_section(pool):
    import numpy as np

    x = np.arange(8, dtype=np.float64)
    # libm's log1p and numpy's can differ by 1 ulp; compare approximately
    assert pool.map(pymizu.call("math.log1p"), x).collect() == pytest.approx(
        np.log1p(x)
    )


def test_map_spec_template_and_view(pool):
    import numpy as np

    x = np.arange(1.0, 6.0)
    out = pool.map(pymizu.call("math.log"), x, template=np.empty(1)).collect()
    assert out == pytest.approx(np.log(x))
    v = pool.map(
        pymizu.call("math.log"), x, template=np.empty(1), collect="view"
    ).collect()
    assert np.asarray(v) == pytest.approx(np.log(x))


def test_map_spec_seed_determinism(pool):
    fn = pymizu.call(source="import random\nrandom.random()")
    a = pool.map(fn, list(range(50)), seed=42).collect()
    b = pool.map(fn, list(range(50)), seed=42, n_chunks=7).collect()
    assert a == b
    # the split-map contract, and parity with the native derivation
    x = list(range(70))
    whole = pool.map(fn, x, seed=42).collect()
    head = pool.map(fn, x[:50], seed=42).collect()
    rest = pool.map(fn, x[50:], seed=(42, 50)).collect()
    assert head + rest == whole
    n_whole = pool.map(rand_elt, x, seed=42).collect()
    assert whole == n_whole


def test_map_spec_prepared(pool):
    # a prepared spec map re-arms per run (a fresh generation and the
    # run's seed on the kind-2 runner fields) without restaging
    pm = pool.map_prepare(
        pymizu.call(source="import random\nrandom.random()"),
        list(range(12)),
    )
    try:
        name = pm._name
        r1 = pm.run(seed=7).collect()
        assert pm._name == name
        r2 = pm.run(seed=7).collect()
        assert pm._name == name
        # the run seed re-arms per run: identical streams
        assert r1 == r2
    finally:
        pm.close()


def test_map_spec_error_index(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(
            pymizu.call(source="if x == 5:\n    raise ValueError('boom')\nx"),
            list(range(10)),
        ).collect()
    assert exc_info.value.index == 5
    assert exc_info.value.remote_type == "ValueError"


def test_map_probe_x_convert_flag():
    # the conversion gate's probe: a 1-D non-identity buffer probes as its
    # conversion row (tag, n, converted bytes) only under the flag
    import numpy as np

    rows = {
        np.int8: (13, 4),  # -> INT
        np.int16: (13, 4),
        np.uint16: (13, 4),
        np.uint32: (14, 8),  # -> REAL
        np.uint64: (32, 8),  # -> INT64
        np.float16: (14, 8),
        np.float32: (14, 8),
        np.bool_: (10, 4),  # -> LGL
        np.complex64: (15, 16),  # -> CPLX
    }
    for dt, (tag, w_out) in rows.items():
        a = np.arange(6).astype(dt)
        assert pymizu._pymizu._map_probe_x(a, False) is None
        assert pymizu._pymizu._map_probe_x(a, True) == (tag, 6, 6 * w_out)
    # identity dtypes probe identically with and without the flag
    a = np.arange(6, dtype=np.float64)
    assert pymizu._pymizu._map_probe_x(a, True) == (14, 6, 48)
    # a 2-D non-identity buffer stays on the descriptor fallback
    m = np.arange(6, dtype=np.int8).reshape(2, 3)
    assert pymizu._pymizu._map_probe_x(m, True) is None
    # and a strided view too
    assert pymizu._pymizu._map_probe_x(a[::2], True) is None


def test_map_swap_x_convert_flag():
    # the prepared-map swap under the conversion gate: a same-dtype
    # non-identity replacement converts in place; a different dtype
    # rejects (the Python side restages)
    import pickle

    import numpy as np

    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = pymizu._pymizu._map_stage(
        desc, np.arange(6, dtype=np.float32), 6, 1, None, True, 2
    )
    try:
        hdr = pymizu._pymizu._map_header(capsule)
        assert hdr["x_tag"] == 14  # REAL after the float32 conversion
        view = pymizu._pymizu._map_x_view(capsule)
        assert list(memoryview(view).cast("d")) == [float(v) for v in range(6)]
        pymizu._pymizu._map_swap_x(
            capsule, np.full(6, 3.0, dtype=np.float32), True, 2
        )
        assert list(memoryview(view).cast("d")) == [3.0] * 6
        with pytest.raises(pymizu.MizuError, match="must match the staged"):
            pymizu._pymizu._map_swap_x(
                capsule, np.arange(6, dtype=np.int16), True, 2
            )
        # without the flag the same-dtype replacement rejects too
        with pytest.raises(pymizu.MizuError, match="must match the staged"):
            pymizu._pymizu._map_swap_x(
                capsule, np.full(6, 4.0, dtype=np.float32), False, 2
            )
    finally:
        pymizu._pymizu._map_close(capsule)


# -- low-level map hooks ----------------------------------------------------


def test_map_stage_geometry_gates():
    import pickle

    desc = pickle.dumps((identity, (), {}), 4)
    stage = pymizu._pymizu._map_stage
    for args, match in [
        ((desc, None, 0, 1, None, False, 1), "invalid map length"),
        ((desc, None, 2**48 + 1, 1, None, False, 1), "invalid map length"),
        ((desc, None, 4, 0, None, False, 1), "invalid map morsel size"),
        ((desc, None, 4, 5, None, False, 1), "invalid map morsel size"),
        ((b"", None, 4, 1, None, False, 1), "invalid map descriptor size"),
        (
            (desc, np.arange(3, dtype=np.float64), 4, 1, None, False, 1),
            "not eligible for the map raw section",
        ),
        (
            (desc, np.arange(4, dtype=np.int8), 4, 1, None, False, 1),
            "not eligible for the map raw section",
        ),
        (
            (desc, np.arange(4, dtype=np.float32), 4, 1, None, False, 1),
            "not eligible for the map raw section",
        ),
        (
            (desc, np.arange(3, dtype=np.float32), 4, 1, None, True, 2),
            "not eligible for the map raw section",
        ),
        (
            (desc, np.array(["a", "b", "c", "d"]), 4, 1, None, True, 2),
            "not eligible for the map raw section",
        ),
        (
            (desc, None, 4, 1, np.arange(2, dtype=np.int8), False, 1),
            "invalid map template",
        ),
        ((desc, None, 4, 1, np.empty(0), False, 1), "invalid map template"),
    ]:
        with pytest.raises(pymizu.MizuError, match=match):
            stage(*args)
    with pytest.raises(TypeError):
        stage(desc, 42, 4, 1, None, False, 1)
    with pytest.raises(TypeError):
        stage(desc, None, 4, 1, 42, False, 1)


def test_map_stage_conversion_warning_as_error():
    # the fused convert completes before the warning raises, so a
    # warn-as-error leaves no half-written region behind
    import pickle
    import warnings

    desc = pickle.dumps((identity, (), {}), 4)
    x = np.array([2**63, 1], dtype=np.uint64)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(RuntimeWarning, match="beyond"):
            pymizu._pymizu._map_stage(desc, x, 2, 1, None, True, 2)


def test_map_stage_region_too_large():
    # the pre-validated geometry leaves only size to the layout: the
    # output area's overflow refusal
    import pickle

    desc = pickle.dumps((identity, (), {}), 4)
    with pytest.raises(pymizu.ShmError, match="map region too large"):
        pymizu._pymizu._map_stage(
            desc, None, 2**43 + 1, 1, np.zeros(1), False, 1
        )


@pytest.mark.skipif(os.name == "nt", reason="RLIMIT_NOFILE is Unix-only")
def test_map_stage_under_fd_starvation():
    # region creation needs an fd: the stage raises, nothing half-created
    resource = pytest.importorskip("resource")
    import fcntl
    import pickle

    def open_fds():
        n = 0
        for fd in range(256):
            try:
                fcntl.fcntl(fd, fcntl.F_GETFD)
                n += 1
            except OSError:
                pass
        return n

    desc = pickle.dumps((identity, (), {}), 4)
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    armed = False
    for _ in range(8):
        resource.setrlimit(resource.RLIMIT_NOFILE, (open_fds(), hard))
        try:
            fd = os.open("/dev/null", os.O_RDONLY)
        except OSError:
            armed = True
            break
        os.close(fd)
    try:
        if not armed:
            pytest.skip("fd starvation not effective on this platform")
        with pytest.raises(pymizu.ShmError, match="cannot create map region"):
            pymizu._pymizu._map_stage(desc, None, 4, 1, None, False, 1)
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


def test_map_open_rejects_a_non_map_region():
    # a live region of the wrong kind fails the map header check
    h = pymizu._pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    try:
        prefix = "Local\\mizu_" if os.name == "nt" else "/mizu_"
        with pytest.raises(pymizu.MizuError, match="invalid map region"):
            pymizu._pymizu._map_open(prefix + h.token)
    finally:
        h.destroy()


def test_map_capsule_lifecycle():
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, caps = Q._map_stage(desc, None, 4, 1, None, False, 1)
    Q._map_close(caps)
    Q._map_close(caps)  # idempotent
    Q._map_cancel_set(caps)  # closed: a no-op (runs from unwind paths)
    for call in (
        lambda: Q._map_header(caps),
        lambda: Q._map_desc(caps),
        lambda: Q._map_x_view(caps),
        lambda: Q._map_cancel_get(caps),
        lambda: Q._map_gather(caps),
        lambda: Q._map_gather_view(caps),
        lambda: Q._map_next(caps, None, 0, 0),
        lambda: Q._map_write(caps, 0, [1.0]),
        lambda: Q._map_swap_x(caps, np.arange(4, dtype=np.float64), False, 1),
        lambda: Q._map_lost(caps, []),
        lambda: Q._map_abandon(caps, 0, 0),
    ):
        with pytest.raises(pymizu.MizuError, match="not a map context"):
            call()
    with pytest.raises(pymizu.MizuError, match="not a map context"):
        Q._map_close(42)


def test_map_foreign_capsule_tolerance():
    # a non-map capsule: close and cancel_set are no-ops, the accessors
    # refuse
    pa = pytest.importorskip("pyarrow", reason="pyarrow not installed")

    Q = pymizu._pymizu
    scap, _acap = pa.array([1.0]).__arrow_c_array__()
    Q._map_close(scap)
    Q._map_cancel_set(scap)
    with pytest.raises(pymizu.MizuError, match="not a map context"):
        Q._map_header(scap)


def test_map_open_unknown_region():
    with pytest.raises(pymizu.ShmError, match="cannot open map region"):
        pymizu._pymizu._map_open("/mizu_no_such_region_0000")


def test_map_x_view_requires_an_x_section():
    import pickle

    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = pymizu._pymizu._map_stage(desc, None, 4, 1, None, False, 1)
    try:
        with pytest.raises(pymizu.MizuError, match="no x section"):
            pymizu._pymizu._map_x_view(capsule)
    finally:
        pymizu._pymizu._map_close(capsule)


def test_map_next_gates():
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = Q._map_stage(desc, None, 4, 1, None, False, 1)
    try:
        claim_n = Q._map_header(capsule)["claim_n"]
        with pytest.raises(pymizu.MizuError, match="ordinal out of range"):
            Q._map_next(capsule, None, claim_n, 0)
        with pytest.raises(pymizu.MizuError, match="not a pool-signal"):
            Q._map_next(capsule, capsule, 0, 0)
    finally:
        Q._map_close(capsule)


def test_map_abandon_gates():
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = Q._map_stage(desc, None, 4, 1, None, False, 1)
    try:
        claim_n = Q._map_header(capsule)["claim_n"]
        assert Q._map_abandon(capsule, 0, 0) == 0  # trigger unarmed: idle
        with pytest.raises(pymizu.MizuError, match="ordinal out of range"):
            Q._map_abandon(capsule, -1, 0)
        with pytest.raises(pymizu.MizuError, match="ordinal out of range"):
            Q._map_abandon(capsule, claim_n, 0)
        with pytest.raises(TypeError):
            Q._map_abandon(capsule, "x", 0)
    finally:
        Q._map_close(capsule)


def test_map_lost_scan():
    # the lost set: issued off the cursor minus the collected histories
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = Q._map_stage(
        desc, np.arange(10, dtype=np.float64), 10, 2, None, False, 1
    )
    try:
        assert Q._map_lost(capsule, []) == []  # nothing issued yet
        assert Q._map_next(capsule, None, 0, 0) is not None
        assert Q._map_next(capsule, None, 0, 0) is not None
        issued_hi = Q._map_lost(capsule, [])[0][1]
        assert Q._map_lost(capsule, []) == [(0, issued_hi)]
        assert Q._map_lost(capsule, [[(0, 2)], [(3, 4)]]) == [
            (2, 3),
            (4, issued_hi),
        ]
        with pytest.raises(TypeError, match="list of range lists"):
            Q._map_lost(capsule, 42)
        with pytest.raises(TypeError, match="invalid map batch history"):
            Q._map_lost(capsule, [42])
        with pytest.raises(pymizu.MizuError, match="invalid map batch"):
            Q._map_lost(capsule, [[(3, 2)]])
        with pytest.raises(pymizu.MizuError, match="invalid map batch"):
            Q._map_lost(capsule, [[(0, 11)]])
        with pytest.raises(pymizu.MizuError, match="invalid map batch"):
            Q._map_lost(capsule, [[("a", 1)]])
    finally:
        Q._map_close(capsule)


def test_map_write_scalar_forms():
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    # (template, a good scalar, a scalar past the type's range, a
    # non-numeric scalar)
    forms = [
        (np.empty(1), 3.5, "s", None, "format 'd'"),
        (np.empty(1, dtype=np.int64), 5, 2**100, 1.5, "format 'q'"),
        (np.empty(1, dtype=np.uint8), 5, 300, "s", "format 'B'"),
        (np.empty(1, dtype=np.complex128), 1 + 2j, "s", None, "format 'Zd'"),
    ]
    for template, good, bad, nonnum, match in forms:
        name, capsule = Q._map_stage(desc, None, 4, 1, template, False, 1)
        try:
            Q._map_write(capsule, 0, [good, good, good, good])
            with pytest.raises(pymizu.MizuError, match=match):
                Q._map_write(capsule, 0, [bad])
            if nonnum is not None:
                with pytest.raises(pymizu.MizuError, match=match):
                    Q._map_write(capsule, 0, [nonnum])
        finally:
            Q._map_close(capsule)
    # past the int32 range: the conversion to a C long overflows outright
    # on Windows (32-bit long), elsewhere the range check takes the
    # template mismatch
    range_error = (
        OverflowError if sys.platform == "win32" else pymizu.MizuError
    )
    match = None if sys.platform == "win32" else "format 'i'"
    name, capsule = Q._map_stage(
        desc, None, 4, 1, np.empty(1, dtype=np.int32), False, 1
    )
    try:
        Q._map_write(capsule, 0, [5, 5, 5, 5])
        with pytest.raises(range_error, match=match):
            Q._map_write(capsule, 0, [2**40])
        with pytest.raises(range_error, match=match):
            Q._map_write(capsule, 0, [-(2**31) - 1])
        with pytest.raises(pymizu.MizuError, match="format 'i'"):
            Q._map_write(capsule, 0, [1.5])
    finally:
        Q._map_close(capsule)
    # an int lands in a REAL slot
    name, capsule = Q._map_stage(desc, None, 4, 1, np.empty(1), False, 1)
    try:
        Q._map_write(capsule, 0, [7])
        assert Q._map_gather(capsule)[:8] == np.float64(7.0).tobytes()
        with pytest.raises(pymizu.MizuError, match="out of bounds"):
            Q._map_write(capsule, 3, [1.0, 2.0])
        with pytest.raises(TypeError, match="must be a sequence"):
            Q._map_write(capsule, 0, 42)
    finally:
        Q._map_close(capsule)
    name, capsule = Q._map_stage(
        desc, None, 4, 1, np.empty(1, dtype=np.uint8), False, 1
    )
    try:
        with pytest.raises(pymizu.MizuError, match="format 'B'"):
            Q._map_write(capsule, 0, [-1])
    finally:
        Q._map_close(capsule)
    # PyLong conversion overflows propagate (REAL/INT/RAW), the int64 form
    # absorbs its OverflowError into the template mismatch
    name, capsule = Q._map_stage(
        desc, None, 4, 1, np.empty(1, dtype=np.int32), False, 1
    )
    try:
        with pytest.raises(OverflowError):
            Q._map_write(capsule, 0, [2**100])
    finally:
        Q._map_close(capsule)


def test_map_write_buffer_forms():
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = Q._map_stage(desc, None, 4, 1, np.empty(2), False, 1)
    try:
        Q._map_write(capsule, 0, [np.array([1.5, 2.5])])
        assert Q._map_gather(capsule)[:16] == np.array([1.5, 2.5]).tobytes()
        # a buffer of another dtype, a wrong length, and a strided view
        # all miss the template
        with pytest.raises(pymizu.MizuError, match="must match"):
            Q._map_write(capsule, 0, [np.array([1, 2], dtype=np.int32)])
        with pytest.raises(pymizu.MizuError, match="must match"):
            Q._map_write(capsule, 0, [np.array([1.0])])
        with pytest.raises(pymizu.MizuError, match="must match"):
            Q._map_write(capsule, 0, [np.arange(4, dtype=np.float64)[::2]])
    finally:
        Q._map_close(capsule)


def test_map_output_area_absent():
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = Q._map_stage(desc, None, 4, 1, None, False, 1)
    try:
        with pytest.raises(pymizu.MizuError, match="no output area"):
            Q._map_write(capsule, 0, [1.0])
        with pytest.raises(pymizu.MizuError, match="no output area"):
            Q._map_gather(capsule)
        with pytest.raises(pymizu.MizuError, match="no output area"):
            Q._map_gather_view(capsule)
    finally:
        Q._map_close(capsule)


def test_map_swap_x_gates():
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = Q._map_stage(desc, None, 4, 1, None, False, 1)
    try:
        with pytest.raises(pymizu.MizuError, match="no x section"):
            Q._map_swap_x(capsule, np.arange(4, dtype=np.float64), False, 1)
    finally:
        Q._map_close(capsule)
    name, capsule = Q._map_stage(
        desc, np.arange(4, dtype=np.float64), 4, 1, None, False, 1
    )
    try:
        with pytest.raises(TypeError):
            Q._map_swap_x(capsule, 42, False, 1)
    finally:
        Q._map_close(capsule)


def test_map_probe_x_no_conversion_row():
    # a dtype with no conversion row probes as ineligible even under the
    # flag; a dtype numpy will not export as a buffer clears and probes
    # the same
    assert pymizu._pymizu._map_probe_x(np.zeros(3, dtype="U3"), True) is None
    assert (
        pymizu._pymizu._map_probe_x(
            np.array([1, 2], dtype="datetime64[D]"), True
        )
        is None
    )


def test_map_view_buffer_formats():
    import ctypes
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    for dtype, fmt in [
        (np.float64, "d"),
        (np.int32, "i"),
        (np.int64, "q"),
        (np.uint8, "B"),
        (np.complex128, "Zd"),
    ]:
        name, capsule = Q._map_stage(
            desc, None, 3, 1, np.empty(1, dtype=dtype), False, 1
        )
        try:
            view = Q._map_gather_view(capsule)
            mv = memoryview(view)
            assert mv.format == fmt
            assert len(mv) == 3
            assert bytes(view) == bytes(3 * mv.itemsize)  # the simple form
        finally:
            Q._map_close(capsule)
    name, capsule = Q._map_stage(desc, None, 3, 1, np.empty(1), False, 1)
    try:
        view = Q._map_gather_view(capsule)
        with pytest.raises(TypeError, match="not writable"):
            ctypes.c_char.from_buffer(view)
    finally:
        Q._map_close(capsule)


def test_map_view_arrow_export_after_close():
    # the export re-validates the map context it owns: closed is an
    # informative error, never a dangling read
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = Q._map_stage(desc, None, 3, 1, np.empty(1), False, 1)
    view = Q._map_gather_view(capsule)
    Q._map_close(capsule)
    with pytest.raises(pymizu.MizuError, match="not a map context"):
        view.__arrow_c_array__()


def test_map_hook_signatures():
    # malformed calls fail at argument parsing, never in the protocol
    import pickle

    Q = pymizu._pymizu
    desc = pickle.dumps((identity, (), {}), 4)
    name, capsule = Q._map_stage(desc, None, 3, 1, np.empty(1), False, 1)
    try:
        for call in (
            lambda: Q._map_stage(desc, None, 3),
            lambda: Q._map_next(capsule, None),
            lambda: Q._map_abandon(capsule),
            lambda: Q._map_swap_x(capsule),
            lambda: Q._map_write(capsule),
            lambda: Q._map_lost(capsule),
            lambda: Q._map_probe_x(None),
            lambda: Q._map_gather_view(capsule).__arrow_c_array__(1, 2),
        ):
            with pytest.raises(TypeError):
                call()
    finally:
        Q._map_close(capsule)
