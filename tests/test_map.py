"""Pool.map tests: round-trip and order, chunking invariance, the blob and
region paths, raw-buffer x sections, the outcome taxonomy (element-indexed
errors, fail-fast, timeout, worker death), seed determinism, nested maps,
and the interrupt backstop — over real spawned workers."""

import operator
import os
import random
import signal
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
    square,
)

import pymizu


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


def test_map_roundtrip(pool):
    assert pool.map(square, list(range(10))) == [i * i for i in range(10)]


def test_map_empty(pool):
    assert pool.map(square, []) == []


def test_map_args_kwargs(pool):
    assert pool.map(pow, [1, 2, 3], args=(2,)) == [1, 4, 9]
    assert pool.map(scale_add, [1, 2], kwargs={"scale": 2, "add": 1}) == [3, 5]


def test_starmap(pool):
    assert pool.starmap(pow, [(2, 10), (3, 3), (4, 0)]) == [1024, 27, 1]


def test_starmap_zip_idiom(pool):
    xs, ys = [1, 2, 3], [4, 5, 6]
    assert pool.starmap(operator.mul, zip(xs, ys, strict=True)) == [4, 10, 18]


def test_starmap_args_kwargs(pool):
    assert pool.starmap(mul_add, [(1, 2), (3, 4)], args=(10,)) == [12, 22]
    assert pool.starmap(mul_add, [(1, 2)], kwargs={"c": 100}) == [102]


def test_starmap_generator_and_rows(pool):
    import numpy as np

    assert pool.starmap(pow, ((i, 2) for i in range(4))) == [0, 1, 4, 9]
    assert pool.starmap(operator.add, np.arange(6).reshape(3, 2)) == [1, 5, 9]


def test_starmap_empty(pool):
    assert pool.starmap(pow, []) == []


def test_starmap_spec_rejected(pool):
    with pytest.raises(TypeError, match="starmap"):
        pool.starmap(pymizu.call("math.sqrt"), [(1.0,)])


def test_starmap_error_index(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.starmap(fail_at, [(0, 99), (4, 4)])
    assert exc_info.value.index == 1


def test_starmap_template(pool):
    import numpy as np

    out = pool.starmap(mul_add, [(1, 2), (3, 4)], template=np.empty(1))
    np.testing.assert_array_equal(out, np.array([2.0, 12.0]))


def test_starmap_stream(pool):
    assert pool.starmap(pow, [(i, 2) for i in range(10)], stream=True) == [
        i * i for i in range(10)
    ]


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
        assert pool.map(square, x, n_chunks=chunks) == want


def test_map_error_index(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(fail_at, list(range(10)), args=(4,))
    exc = exc_info.value
    assert exc.remote_type == "ValueError"
    assert exc.index == 4
    assert "element 4" in str(exc)


def test_map_fail_fast(pool):
    # the failing element's runner sets the cancel word: peers stop within
    # ~a batch instead of draining the remaining (slow) elements
    start = time.monotonic()
    with pytest.raises(pymizu.TaskError):
        pool.map(fail_or_sleep, list(range(8)), args=(0, 5.0))
    assert time.monotonic() - start < 5.0


def test_map_timeout(pool):
    x = [0.4] * 8
    out = pool.map(sleep_ident, x, timeout=0.2)
    assert out is pymizu.TIMEOUT
    # the cancellation drained: the pool is usable immediately after
    assert pool.map(square, [2], timeout=15) == [4]


def test_map_worker_died():
    p = pymizu.Pool.create(1)
    try:
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
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
    b = pool.map(rand_elt, list(range(50)), seed=42, n_chunks=7)
    c = pool.map(rand_elt, list(range(50)), seed=42, n_chunks=1)
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


def test_map_seed_stdlib_derivation_unchanged(pool):
    # golden constants pin the stdlib per-element derivation against the
    # current_rng stash edit: random.seed(SHA-256(seed_bytes +
    # i.to_bytes(8, "little"))) — random.random() is stable across
    # CPython versions
    out = pool.map(rand_elt, list(range(4)), seed=42)
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
    assert pool.map(current_rng_or_none, [1, 2, 3]) == [None, None, None]


def test_map_nested(pool):
    assert pool.map(nested_map, [4, 5]) == [
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
        assert pool.map(identity, a, n_chunks=chunks) == want


# -- seeded numpy streams (pymizu.current_rng) --------------------------------


def test_map_seed_numpy_determinism(pool):
    a = pool.map(np_rand_elt, list(range(50)), seed=42)
    b = pool.map(np_rand_elt, list(range(50)), seed=42, n_chunks=7)
    c = pool.map(np_rand_elt, list(range(50)), seed=42, n_chunks=1)
    assert a == b == c
    d = pool.map(np_rand_elt, list(range(50)), seed=43)
    assert a != d


def test_map_seed_numpy_offset(pool):
    # the split-map contract holds for numpy draws: element i of the
    # shifted map draws stream i + n of the base map
    x = list(range(70))
    whole = pool.map(np_rand_elt, x, seed=42)
    head = pool.map(np_rand_elt, x[:50], seed=42)
    rest = pool.map(np_rand_elt, x[50:], seed=(42, 50))
    assert head + rest == whole
    assert rest != whole[:20]


def test_map_seed_numpy_memoized_per_element(pool):
    out = pool.map(np_rand_pair, list(range(20)), seed=42)
    # two calls in one element continue one stream (the memoized object)
    assert all(same and a != b for a, b, same in out)
    # distinct elements get distinct streams
    assert len({a for a, _, _ in out}) == 20


def test_map_seed_numpy_template(pool):
    a = pool.map(np_rand_elt, list(range(50)), seed=42, template=np.empty(1))
    b = pool.map(np_rand_elt, list(range(50)), seed=42, template=np.empty(1))
    np.testing.assert_array_equal(a, b)


def test_map_seed_numpy_prepared_rerun(pool):
    # a re-armed prepared run restarts every element's stream
    pm = pool.map_prepare(np_rand_elt, list(range(30)), seed=42)
    try:
        assert pm.run() == pm.run()
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
    out = pool.map(pair_up, list(range(40)), template=np.empty(2), n_chunks=3)
    assert out.shape == (40, 2)


def test_map_template_view(pool):
    out = pool.map(
        scalar_double, list(range(100)), template=np.empty(1), collect="view"
    )
    assert isinstance(out, np.ndarray)
    assert not out.flags.writeable
    np.testing.assert_array_equal(out, np.arange(100) * 2.0)


def test_map_template_mismatch(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
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


def test_map_template_int64_scalar(pool):
    out = pool.map(square, [1, 2, 3], template=np.empty(1, dtype=np.int64))
    assert out.dtype == np.int64
    assert list(out) == [1, 4, 9]


def test_map_template_int64_buffer_result(pool):
    out = pool.map(
        pair_up_i64, list(range(40)), template=np.empty(2, dtype=np.int64)
    )
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
    )
    assert isinstance(out, np.ndarray)
    assert out.dtype == np.int64
    assert not out.flags.writeable
    np.testing.assert_array_equal(out, np.arange(50) ** 2)


def test_map_template_int64_mismatch(pool):
    # an int past INT64_MAX fails the element write as a template mismatch
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(huge_int, [1, 2, 3], template=np.empty(1, dtype=np.int64))
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
            assert pm.run() == want
    finally:
        pm.close()


def test_map_prepared_rearm_fences_stragglers(pool):
    # repeated runs over the same region: every run returns the full set
    pm = pool.map_prepare(identity, list(range(2000)), n_chunks=8)
    try:
        for _ in range(5):
            assert pm.run() == list(range(2000))
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
            out = pm.run()
            np.testing.assert_array_equal(out, np.arange(100) * 2.0)
    finally:
        pm.close()


def test_map_prepared_view_restages(pool):
    pm = pool.map_prepare(
        scalar_double, list(range(50)), template=np.empty(1), collect="view"
    )
    try:
        v1 = pm.run()
        np.testing.assert_array_equal(v1, np.arange(50) * 2.0)
        # the view owns the first region; the second run restages fresh
        v2 = pm.run()
        np.testing.assert_array_equal(v2, np.arange(50) * 2.0)
        np.testing.assert_array_equal(v1, np.arange(50) * 2.0)
    finally:
        pm.close()


def test_map_prepared_error_and_reuse(pool):
    pm = pool.map_prepare(fail_at, list(range(10)), args=(4,))
    try:
        with pytest.raises(pymizu.TaskError) as exc_info:
            pm.run()
        assert exc_info.value.index == 4
        # the cancel word fired; the next run's re-arm clears it
        pm2 = pool.map_prepare(square, list(range(10)))
        try:
            assert pm2.run() == [i * i for i in range(10)]
        finally:
            pm2.close()
    finally:
        pm.close()


def test_map_prepared_timeout(pool):
    pm = pool.map_prepare(sleep_ident, [0.4] * 8)
    try:
        assert pm.run(timeout=0.2) is pymizu.TIMEOUT
        assert pool.map(square, [2], timeout=15) == [4]
    finally:
        pm.close()


def test_map_prepared_swap_x(pool):
    pm = pool.map_prepare(identity, np.arange(100, dtype=np.float64))
    try:
        name = pm._name
        out = pm.run()
        assert [float(v) for v in out] == [float(v) for v in np.arange(100)]
        # same dtype and length: an in-place swap — the region is reused
        x2 = np.arange(100, dtype=np.float64) * 10
        out = pm.run(x=x2)
        assert [float(v) for v in out] == [float(v) for v in x2]
        assert pm._name == name
    finally:
        pm.close()


def test_map_prepared_swap_x_template(pool):
    pm = pool.map_prepare(
        scalar_double, np.arange(100, dtype=np.float64), template=np.empty(1)
    )
    try:
        np.testing.assert_array_equal(pm.run(), np.arange(100) * 2.0)
        name = pm._name
        out = pm.run(x=np.ones(100))
        np.testing.assert_array_equal(out, np.ones(100) * 2.0)
        assert pm._name == name
    finally:
        pm.close()


def test_map_prepared_replace_x_restages(pool):
    pm = pool.map_prepare(identity, np.arange(50, dtype=np.float64))
    try:
        pm.run()
        name = pm._name
        # a dtype change restages even at the same length
        out = pm.run(x=np.arange(50, dtype=np.int32))
        assert [int(v) for v in out] == list(range(50))
        assert pm._name != name
        # a different length restages
        out = pm.run(x=np.arange(10, dtype=np.float64))
        assert [float(v) for v in out] == [float(v) for v in np.arange(10)]
        # a non-buffer x restages onto the descriptor path
        assert pm.run(x=[1, 2, 3]) == [1, 2, 3]
        # and a raw-buffer x stages again afterwards
        out = pm.run(x=np.ones(4))
        assert [float(v) for v in out] == [1.0] * 4
        # an empty replacement short-circuits
        assert pm.run(x=[]) == []
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
        assert pm.run() == [9]
    assert pm.closed


def test_map_spec_name_kind(pool):
    # a spec f always takes the region path: kind-2 runner tasks with the
    # 'I' descriptor, even on a same-language pool
    out = pool.map(pymizu.call("math.sqrt"), [1.0, 4.0, 9.0])
    assert out == [1.0, 2.0, 3.0]
    assert pool.map(pymizu.call("builtins.len"), [[1, 2], [3]]) == [2, 1]


def test_map_spec_source_kind(pool):
    assert pool.map(pymizu.call(source="x * 2"), [1, 2, 3]) == [2, 4, 6]
    # the element binds as x, named constants as names, positional as _1
    assert pool.map(pymizu.call(source="x + k", k=10), [1, 2]) == [11, 12]
    assert pool.map(pymizu.call(None, 10, source="x * _1"), [1, 2]) == [10, 20]
    # a statement prefix runs per element; the trailing expression's value
    src = "import math\nmath.floor(x)"
    assert pool.map(pymizu.call(source=src), [1.7, 2.3]) == [1, 2]


def test_map_spec_held_in_a_variable(pool):
    fn = pymizu.call("math.floor")
    assert pool.map(fn, [1.7, 2.3]) == [1, 2]


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
    assert pool.map(pymizu.call("math.log1p"), x) == pytest.approx(np.log1p(x))


def test_map_spec_template_and_view(pool):
    import numpy as np

    x = np.arange(1.0, 6.0)
    out = pool.map(pymizu.call("math.log"), x, template=np.empty(1))
    assert out == pytest.approx(np.log(x))
    v = pool.map(
        pymizu.call("math.log"), x, template=np.empty(1), collect="view"
    )
    assert np.asarray(v) == pytest.approx(np.log(x))


def test_map_spec_seed_determinism(pool):
    fn = pymizu.call(source="import random\nrandom.random()")
    a = pool.map(fn, list(range(50)), seed=42)
    b = pool.map(fn, list(range(50)), seed=42, n_chunks=7)
    assert a == b
    # the split-map contract, and parity with the native derivation
    x = list(range(70))
    whole = pool.map(fn, x, seed=42)
    head = pool.map(fn, x[:50], seed=42)
    rest = pool.map(fn, x[50:], seed=(42, 50))
    assert head + rest == whole
    n_whole = pool.map(rand_elt, x, seed=42)
    assert whole == n_whole


def test_map_spec_prepared(pool):
    # a prepared spec map re-arms per run (a fresh generation and the
    # prepared seed on the kind-2 runner fields) without restaging
    pm = pool.map_prepare(
        pymizu.call(source="import random\nrandom.random()"),
        list(range(12)),
        seed=7,
    )
    try:
        name = pm._name
        r1 = pm.run()
        assert pm._name == name
        r2 = pm.run()
        assert pm._name == name
        # the prepared seed re-arms per run: identical streams
        assert r1 == r2
    finally:
        pm.close()


def test_map_spec_error_index(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(
            pymizu.call(source="if x == 5:\n    raise ValueError('boom')\nx"),
            list(range(10)),
        )
    assert exc_info.value.index == 5
    assert exc_info.value.remote_type == "ValueError"
