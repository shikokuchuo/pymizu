"""Pool.map(stream=True) tests: the sliding submit/collect window over
fixed x-slices — order across completion order, template copy and view
collects, seed invariance, the fail-fast element index through
collect_any (the position stamp defers to the envelope's element index),
worker-death lost ranges, buffer x forms, prepared maps, and the
unclean-run restage — over real spawned workers."""

import pickle

import pytest
from tests.helpers import (
    fail_at,
    fail_on,
    identity,
    kill_at,
    map_ctx_names,
    pair_up,
    rand_elt,
    scalar_double,
    scale_add,
    sleep_ident,
    sleep_on,
    square,
)

import pymizu
from pymizu import _map as _map_mod


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


def test_stream_roundtrip(pool):
    x = list(range(100))
    assert pool.map(square, x, stream=True) == [i * i for i in x]


def test_stream_matches_non_streaming(pool):
    x = list(range(200))
    want = pool.map(square, x)
    for chunks in (None, 1, 3, 64, 200):
        assert pool.map(square, x, stream=True, n_chunks=chunks) == want


def test_stream_empty(pool):
    assert pool.map(square, [], stream=True) == []


def test_stream_args_kwargs(pool):
    assert pool.map(
        scale_add, [1, 2], kwargs={"scale": 2, "add": 1}, stream=True
    ) == [3, 5]


def test_stream_out_of_order_completion(pool):
    # chunk 0's first element sleeps, so chunks 1+ complete first — the
    # splice still assembles in input order
    x = list(range(32))
    out = pool.map(sleep_on, x, args=({0},), n_chunks=8, stream=True)
    assert out == x


def test_stream_seed_invariance(pool):
    a = pool.map(rand_elt, list(range(50)), seed=42)
    assert pool.map(rand_elt, list(range(50)), seed=42, stream=True) == a
    for chunks in (1, 3, 50):
        got = pool.map(
            rand_elt, list(range(50)), seed=42, stream=True, n_chunks=chunks
        )
        assert got == a
    # and across window sizes (a 1-worker pool halves the window)
    p1 = pymizu.Pool.create(1)
    try:
        assert p1.map(rand_elt, list(range(50)), seed=42, stream=True) == a
    finally:
        p1.stop()


def test_stream_seed_offset(pool):
    # the split-map contract, unchanged: element i of the shifted map
    # draws stream i + 5 of the base map
    whole = pool.map(rand_elt, list(range(10)), seed=7, stream=True)
    tail = pool.map(rand_elt, list(range(10))[5:], seed=(7, 5), stream=True)
    assert whole[5:] == tail


def test_stream_fail_fast_element_index(pool):
    # the regression the index guard fixes: collect_any's position stamp
    # must not clobber the envelope's element index (element 4 is in
    # chunk 2 of 4 here — the position would read 2)
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(fail_at, list(range(8)), args=(4,), stream=True, n_chunks=4)
    exc = exc_info.value
    assert exc.remote_type == "ValueError"
    assert exc.index == 4
    assert "element 4" in str(exc)


def test_stream_fail_fast_min_index():
    # sibling errors drained into the minimum element index selection:
    # chunk 1 fails at 3, chunk 2 at 5 — both terminal before the turn
    p = pymizu.Pool.create(1)
    try:
        with pytest.raises(pymizu.TaskError) as exc_info:
            p.map(
                fail_on, list(range(8)), args=({3, 5},), stream=True,
                n_chunks=4,
            )
        assert exc_info.value.index == 3
    finally:
        p.stop()


def test_stream_worker_died():
    p = pymizu.Pool.create(2)
    try:
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
            p.map(kill_at, list(range(8)), args=(5,), stream=True, n_chunks=4)
        exc = exc_info.value
        assert exc.pid > 0
        # streaming chunks are fixed ranges: the lost set is the dead
        # chunk, exactly (the blob-path shape)
        assert exc.lost == [(4, 6)]
    finally:
        p.stop()


def test_stream_timeout(pool):
    out = pool.map(sleep_ident, [0.4] * 8, timeout=0.2, stream=True)
    assert out is pymizu.TIMEOUT
    # the cancellation drained: the pool is usable immediately after
    assert pool.map(square, [2], timeout=15) == [4]


def test_stream_timeout_at_prime(pool):
    # a zero deadline expires at the first refill's pre-check
    assert pool.map(square, list(range(8)), stream=True, timeout=0) is (
        pymizu.TIMEOUT
    )


def test_stream_submit_ring_full_timeout():
    # ring-full past the deadline is the map's timeout, not an error:
    # both workers pinned, the two-slot ring never drains
    p = pymizu.Pool.create(2, injection_cap=2)
    try:
        pin1 = p.submit(sleep_ident, 3.0)
        pin2 = p.submit(sleep_ident, 3.0)
        out = p.map(square, list(range(4)), stream=True, n_chunks=4, timeout=1)
        assert out is pymizu.TIMEOUT
        # the pins really ran (the workers were genuinely busy)
        assert pin1.collect(timeout=15) == 3.0
        assert pin2.collect(timeout=15) == 3.0
        # and the cancellation drained: the pool is usable after
        assert p.map(square, [2], timeout=15) == [4]
    finally:
        p.stop()


def _stage_stream_region(fn, n):
    desc = pickle.dumps((fn, (), {}), 4)
    return pymizu._pymizu._map_stage(desc, None, n, 1, None)


def test_stream_window_expired_at_collect(pool):
    # the loop-head deadline check: refill passes, then expiry lands
    name, capsule = _stage_stream_region(identity, 2)
    handles = [None] * 2
    script = iter([False, True])  # refill pre-check, then the loop head
    try:
        out = _map_mod._stream_window(
            pool,
            pymizu,
            name,
            [(0, 1), (1, 2)],
            1,
            None,
            lambda lo, hi: [0, 1][lo:hi],
            False,
            lambda: None,
            lambda: next(script, True),
            handles,
        )
        assert out is pymizu.TIMEOUT
    finally:
        for h in handles:
            if h is not None:
                h.cancel()
        pymizu._pymizu._map_close(capsule)


def test_stream_window_expired_at_refill(pool):
    # the post-completion refill's deadline check: one chunk completes,
    # then the deadline expires before its replacement submits
    name, capsule = _stage_stream_region(identity, 2)
    handles = [None] * 2
    # prime pre-check, the loop head, then the refill pre-check
    script = iter([False, False, True])
    try:
        out = _map_mod._stream_window(
            pool,
            pymizu,
            name,
            [(0, 1), (1, 2)],
            1,
            None,
            lambda lo, hi: [0, 1][lo:hi],
            False,
            lambda: None,
            lambda: next(script, True),
            handles,
        )
        assert out is pymizu.TIMEOUT
    finally:
        for h in handles:
            if h is not None:
                h.cancel()
        pymizu._pymizu._map_close(capsule)


class _FakeHandle:
    """A drain-test stand-in for a task handle: cancel() no-ops, collect()
    replays a canned terminal outcome."""

    def __init__(self, outcome):
        self.outcome = outcome

    def cancel(self):
        pass

    def collect(self, timeout=None):
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def _task_error(index):
    e = pymizu.TaskError(f"ValueError: element {index}")
    e.index = index
    return e


def test_stream_fail_drains_sibling_errors():
    # the drain harvests already-terminal siblings for the minimum
    # element index selection, ignoring values and cancellations
    first = _task_error(3)
    handles = [
        _FakeHandle(_task_error(5)),
        None,  # an already-consumed slot reads as None
        _FakeHandle(pymizu.CancelledError("cancelled")),
        _FakeHandle([0]),
    ]
    with pytest.raises(pymizu.TaskError) as exc_info:
        _map_mod._stream_fail(
            pymizu, handles, [0, 1, 2, 3],
            [(0, 2), (2, 4), (4, 6), (6, 8)], first,
        )
    assert exc_info.value.index == 3


def test_stream_fail_death_takes_precedence():
    # a sibling death in the drain beats the observed error and reports
    # the dead chunk's range
    first = _task_error(3)
    handles = [
        _FakeHandle(_task_error(5)),
        None,
        _FakeHandle(pymizu.WorkerDiedError("worker died")),
    ]
    with pytest.raises(pymizu.WorkerDiedError) as exc_info:
        _map_mod._stream_fail(
            pymizu, handles, [0, 1, 2], [(0, 2), (2, 4), (4, 6)], first
        )
    assert exc_info.value.lost == [(4, 6)]


def test_stream_spec_fn_raises(pool):
    with pytest.raises(TypeError, match="cannot stream"):
        pool.map(pymizu.call("math.sqrt"), [1, 4], stream=True)


def test_stream_buffer_x_forms(pool):
    assert pool.map(identity, bytes(range(10)), stream=True) == list(range(10))
    assert pool.map(identity, bytearray(range(10)), stream=True) == list(
        range(10)
    )
    assert pool.map(
        identity, memoryview(bytes(range(10))), stream=True
    ) == list(range(10))


def test_stream_list_x_shallow(pool):
    x = [[i] for i in range(6)]
    out = pool.map(identity, x, stream=True)
    assert out == x
    # the submitter-side slice aliases; the elements then cross serialized
    assert all(a is not b and a == b for a, b in zip(out, x, strict=True))


def test_stream_prepared(pool):
    pm = pool.map_prepare(square, list(range(8)), stream=True)
    try:
        name = pm._name
        assert pm.run() == [i * i for i in range(8)]
        assert pm.run() == [i * i for i in range(8)]
        assert pm._name == name  # one region throughout
        # a replacement x of any shape re-slices: no restage
        assert pm.run(list(range(4))) == [0, 1, 4, 9]
        assert pm._name == name
        assert pm.run([1, 2, 3]) == [1, 4, 9]
        assert pm._name == name
    finally:
        pm.close()


def test_stream_prepared_seeded(pool):
    # a re-armed prepared run restarts every element's stream
    pm = pool.map_prepare(rand_elt, list(range(30)), seed=42, stream=True)
    try:
        assert pm.run() == pm.run()
    finally:
        pm.close()


def test_stream_prepared_timeout_restages(pool):
    pm = pool.map_prepare(sleep_ident, [0.4, 0.1, 0.1], stream=True)
    try:
        name = pm._name
        assert pm.run(timeout=0.2) is pymizu.TIMEOUT
        assert pm._capsule is None  # stale: the next run restages
        assert pm.run(timeout=60) == [0.4, 0.1, 0.1]
        assert pm._name != name
    finally:
        pm.close()


def test_stream_prepared_error_restages(pool):
    pm = pool.map_prepare(fail_at, list(range(4)), args=(2,), stream=True)
    try:
        with pytest.raises(pymizu.TaskError):
            pm.run(timeout=60)
        assert pm._capsule is None
        with pytest.raises(pymizu.TaskError) as exc_info:
            pm.run(timeout=60)
        assert exc_info.value.index == 2
    finally:
        pm.close()


def test_stream_ctx_cache_one_attach():
    # a multi-chunk map on one worker holds exactly one cached context
    p = pymizu.Pool.create(1)
    try:
        pm = p.map_prepare(square, list(range(16)), n_chunks=4, stream=True)
        try:
            assert pm.run() == [i * i for i in range(16)]
            names = p.submit(map_ctx_names, []).collect(timeout=15)
            assert names == [pm._name]
        finally:
            pm.close()
    finally:
        p.stop()


# -- numpy x sections and templates -----------------------------------------

np = pytest.importorskip("numpy", reason="numpy not installed")


def test_stream_numpy_x(pool):
    a = np.arange(1000, dtype=np.float64)
    assert pool.map(identity, a, stream=True) == list(a)


def test_stream_numpy_int64_x(pool):
    a = np.arange(10, dtype=np.int64) * (1 << 40)
    out = pool.map(identity, a, stream=True)
    assert out == list(a)
    assert all(isinstance(v, np.integer) for v in out)


def test_stream_numpy_strided_x_falls_back(pool):
    # a non-contiguous view is not raw-eligible: elements pickle per slice
    a = np.arange(20, dtype=np.float64)[::2]
    assert pool.map(identity, a, stream=True) == list(a)


def test_stream_template_copy(pool):
    out = pool.map(
        scalar_double, list(range(100)), template=np.empty(1), stream=True
    )
    assert isinstance(out, np.ndarray)
    assert out.shape == (100,)
    np.testing.assert_array_equal(out, np.arange(100) * 2.0)


def test_stream_template_view(pool):
    out = pool.map(
        scalar_double,
        list(range(100)),
        template=np.empty(1),
        collect="view",
        stream=True,
    )
    assert isinstance(out, np.ndarray)
    assert not out.flags.writeable
    np.testing.assert_array_equal(out, np.arange(100) * 2.0)


def test_stream_template_m2(pool):
    out = pool.map(
        pair_up, list(range(50)), template=np.empty(2), stream=True
    )
    assert out.shape == (50, 2)
    np.testing.assert_array_equal(out[:, 0], np.arange(50))
    np.testing.assert_array_equal(out[:, 1], np.arange(50) * 2)


def test_stream_prepared_template_length_change_restages(pool):
    pm = pool.map_prepare(
        scalar_double, [1, 2, 3], template=np.empty(1), stream=True
    )
    try:
        name = pm._name
        np.testing.assert_array_equal(
            pm.run(), np.array([2.0, 4.0, 6.0])
        )
        assert pm._name == name
        out = pm.run([1, 2, 3, 4])
        np.testing.assert_array_equal(out, np.array([2.0, 4.0, 6.0, 8.0]))
        assert pm._name != name  # the output area sizes off the staged n
    finally:
        pm.close()


def test_stream_prepared_template_view_transfers(pool):
    pm = pool.map_prepare(
        scalar_double,
        [1, 2, 3],
        template=np.empty(1),
        collect="view",
        stream=True,
    )
    try:
        out = pm.run()
        assert not out.flags.writeable
        np.testing.assert_array_equal(out, np.array([2.0, 4.0, 6.0]))
        # the view pins its region: the next run stages fresh
        assert pm._capsule is None
        np.testing.assert_array_equal(
            pm.run(), np.array([2.0, 4.0, 6.0])
        )
    finally:
        pm.close()
