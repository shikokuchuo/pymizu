"""Pool.map(stream=True) tests: the sliding submit/collect window over
fixed x-slices — order across completion order, template copy and view
collects, seed invariance, the fail-fast element index through
collect_any (the position stamp defers to the envelope's element index),
worker-death lost ranges, buffer x forms, prepared maps, and the
unclean-run restage — over real spawned workers."""

import pytest
from tests.helpers import (
    fail_at,
    fail_on,
    identity,
    kill_at,
    map_ctx_names,
    pair_up,
    rand_elt,
    scale_add,
    scalar_double,
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


def test_stream_roundtrip(pool):
    x = list(range(100))
    assert pool.map(square, x, stream=True) == [i * i for i in x]


def test_stream_matches_non_streaming(pool):
    x = list(range(200))
    want = pool.map(square, x)
    for chunks in (None, 1, 3, 64, 200):
        assert pool.map(square, x, stream=True, chunks=chunks) == want


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
    out = pool.map(sleep_on, x, args=({0},), chunks=8, stream=True)
    assert out == x


def test_stream_seed_invariance(pool):
    a = pool.map(rand_elt, list(range(50)), seed=42)
    assert pool.map(rand_elt, list(range(50)), seed=42, stream=True) == a
    for chunks in (1, 3, 50):
        got = pool.map(
            rand_elt, list(range(50)), seed=42, stream=True, chunks=chunks
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
        pool.map(fail_at, list(range(8)), args=(4,), stream=True, chunks=4)
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
                chunks=4,
            )
        assert exc_info.value.index == 3
    finally:
        p.stop()


def test_stream_worker_died():
    p = pymizu.Pool.create(2)
    try:
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
            p.map(kill_at, list(range(8)), args=(5,), stream=True, chunks=4)
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
    assert all(a is not b and a == b for a, b in zip(out, x))


def test_stream_prepared(pool):
    pm = pool.map_prepare(square, list(range(8)), stream=True)
    try:
        name = pm._name
        assert pool.map_run(pm) == [i * i for i in range(8)]
        assert pool.map_run(pm) == [i * i for i in range(8)]
        assert pm._name == name  # one region throughout
        # a replacement x of any shape re-slices: no restage
        assert pool.map_run(pm, list(range(4))) == [0, 1, 4, 9]
        assert pm._name == name
        assert pool.map_run(pm, [1, 2, 3]) == [1, 4, 9]
        assert pm._name == name
    finally:
        pm.close()


def test_stream_prepared_seeded(pool):
    # a re-armed prepared run restarts every element's stream
    pm = pool.map_prepare(rand_elt, list(range(30)), seed=42, stream=True)
    try:
        assert pool.map_run(pm) == pool.map_run(pm)
    finally:
        pm.close()


def test_stream_prepared_timeout_restages(pool):
    pm = pool.map_prepare(sleep_ident, [0.4, 0.1, 0.1], stream=True)
    try:
        name = pm._name
        assert pool.map_run(pm, timeout=0.2) is pymizu.TIMEOUT
        assert pm._capsule is None  # stale: the next run restages
        assert pool.map_run(pm, timeout=60) == [0.4, 0.1, 0.1]
        assert pm._name != name
    finally:
        pm.close()


def test_stream_prepared_error_restages(pool):
    pm = pool.map_prepare(fail_at, list(range(4)), args=(2,), stream=True)
    try:
        with pytest.raises(pymizu.TaskError):
            pool.map_run(pm, timeout=60)
        assert pm._capsule is None
        with pytest.raises(pymizu.TaskError) as exc_info:
            pool.map_run(pm, timeout=60)
        assert exc_info.value.index == 2
    finally:
        pm.close()


def test_stream_ctx_cache_one_attach():
    # a multi-chunk map on one worker holds exactly one cached context
    p = pymizu.Pool.create(1)
    try:
        pm = p.map_prepare(square, list(range(16)), chunks=4, stream=True)
        try:
            assert p.map_run(pm) == [i * i for i in range(16)]
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
            pool.map_run(pm), np.array([2.0, 4.0, 6.0])
        )
        assert pm._name == name
        out = pool.map_run(pm, [1, 2, 3, 4])
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
        out = pool.map_run(pm)
        assert not out.flags.writeable
        np.testing.assert_array_equal(out, np.array([2.0, 4.0, 6.0]))
        # the view pins its region: the next run stages fresh
        assert pm._capsule is None
        np.testing.assert_array_equal(
            pool.map_run(pm), np.array([2.0, 4.0, 6.0])
        )
    finally:
        pm.close()
