"""Pool.map(stream=True) tests: the sliding submit/collect window over
fixed x-slices — order across completion order, template copy and view
collects, seed invariance, the fail-fast element index through
collect_any (the position stamp defers to the envelope's element index),
worker-death lost ranges, buffer x forms, prepared maps, and the
unclean-run restage — over real spawned workers."""

import os
import pickle
import time

import pytest
from tests.helpers import (
    fail_at,
    fail_on,
    fail_with_map_index,
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
    assert pool.map(square, x, stream=True).collect() == [i * i for i in x]


def test_stream_matches_non_streaming(pool):
    x = list(range(200))
    want = pool.map(square, x).collect()
    for chunks in (None, 1, 3, 64, 200):
        assert (
            pool.map(square, x, stream=True, n_chunks=chunks).collect() == want
        )


def test_stream_empty(pool):
    assert pool.map(square, [], stream=True).collect() == []


def test_stream_args_kwargs(pool):
    assert pool.map(
        scale_add, [1, 2], kwargs={"scale": 2, "add": 1}, stream=True
    ).collect() == [3, 5]


def test_stream_out_of_order_completion(pool):
    # chunk 0's first element sleeps, so chunks 1+ complete first — the
    # splice still assembles in input order
    x = list(range(32))
    out = pool.map(sleep_on, x, args=({0},), n_chunks=8, stream=True).collect()
    assert out == x


def test_stream_seed_invariance(pool):
    a = pool.map(rand_elt, list(range(50)), seed=42).collect()
    assert (
        pool.map(rand_elt, list(range(50)), seed=42, stream=True).collect()
        == a
    )
    for chunks in (1, 3, 50):
        got = pool.map(
            rand_elt, list(range(50)), seed=42, stream=True, n_chunks=chunks
        ).collect()
        assert got == a
    # and across window sizes (a 1-worker pool halves the window)
    p1 = pymizu.Pool.create(1)
    try:
        assert (
            p1.map(rand_elt, list(range(50)), seed=42, stream=True).collect()
            == a
        )
    finally:
        p1.stop()


def test_stream_seed_offset(pool):
    # the split-map contract, unchanged: element i of the shifted map
    # draws stream i + 5 of the base map
    whole = pool.map(rand_elt, list(range(10)), seed=7, stream=True).collect()
    tail = pool.map(
        rand_elt, list(range(10))[5:], seed=(7, 5), stream=True
    ).collect()
    assert whole[5:] == tail


def test_stream_fail_fast_element_index(pool):
    # the regression the index guard fixes: collect_any's position stamp
    # must not clobber the envelope's element index (element 4 is in
    # chunk 2 of 4 here — the position would read 2)
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.map(
            fail_at, list(range(8)), args=(4,), stream=True, n_chunks=4
        ).collect()
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
                fail_on,
                list(range(8)),
                args=({3, 5},),
                stream=True,
                n_chunks=4,
            ).collect()
        assert exc_info.value.index == 3
    finally:
        p.stop()


def test_stream_worker_died():
    p = pymizu.Pool.create(2)
    try:
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
            p.map(
                kill_at, list(range(8)), args=(5,), stream=True, n_chunks=4
            ).collect()
        exc = exc_info.value
        assert exc.pid > 0
        # streaming chunks are fixed ranges: the lost set is the dead
        # chunk, exactly (the blob-path shape)
        assert exc.lost == [(4, 6)]
    finally:
        p.stop()


def test_stream_timeout(pool):
    out = pool.map(sleep_ident, [0.4] * 8, stream=True).collect(timeout=0.2)
    assert out is pymizu.TIMEOUT
    # the cancellation drained: the pool is usable immediately after
    assert pool.map(square, [2], timeout=15).collect() == [4]


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
        assert p.map(square, [2], timeout=15).collect() == [4]
    finally:
        p.stop()


def _stage_stream_region(fn, n):
    desc = pickle.dumps((fn, (), {}), 4)
    return pymizu._pymizu._map_stage(desc, None, n, 1, None, False, 3)


def test_stream_window_expired_at_collect(pool):
    # the loop-head deadline check: refill passes, then expiry lands
    name, capsule = _stage_stream_region(identity, 2)
    script = iter([False, True])  # refill pre-check, then the loop head
    win = _map_mod._StreamWindow(
        pool,
        pymizu,
        name,
        [(0, 1), (1, 2)],
        1,
        None,
        lambda lo, hi: [0, 1][lo:hi],
        False,
    )
    try:
        assert win.refill(lambda: None, lambda: next(script, True))
        out = win.drive(lambda: None, lambda: next(script, True))
        assert out is pymizu.TIMEOUT
    finally:
        for h in win.handles:
            if h is not None:
                h.cancel()
        pymizu._pymizu._map_close(capsule)


def test_stream_window_expired_at_refill(pool):
    # the post-completion refill's deadline check: one chunk completes,
    # then the deadline expires before its replacement submits
    name, capsule = _stage_stream_region(identity, 2)
    # prime pre-check, the loop head, then the refill pre-check
    script = iter([False, False, True])
    win = _map_mod._StreamWindow(
        pool,
        pymizu,
        name,
        [(0, 1), (1, 2)],
        1,
        None,
        lambda lo, hi: [0, 1][lo:hi],
        False,
    )
    try:
        assert win.refill(lambda: None, lambda: next(script, True))
        out = win.drive(lambda: None, lambda: next(script, True))
        assert out is pymizu.TIMEOUT
    finally:
        for h in win.handles:
            if h is not None:
                h.cancel()
        pymizu._pymizu._map_close(capsule)


class _FakeHandle:
    """A drain-test stand-in for a task handle: cancel() no-ops, state
    presets the probe, collect() replays a canned terminal outcome."""

    def __init__(self, outcome, state=None):
        self.outcome = outcome
        if state is None:
            if isinstance(outcome, pymizu.CancelledError):
                state = "cancel"
            elif isinstance(outcome, pymizu.WorkerDiedError):
                state = "died"
            elif isinstance(outcome, pymizu.TaskError):
                state = "err"
            else:
                state = "ok"
        self.state = state

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
            pymizu,
            handles,
            [0, 1, 2, 3],
            [(0, 2), (2, 4), (4, 6), (6, 8)],
            first,
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


def test_stream_fail_ignores_consumed_cancel():
    # a just-cancelled sibling whose slot the worker already consumed
    # (its publish or claim skip freed it) probes "collected": the drain
    # must skip it, not read it as a double collect
    first = _task_error(3)
    handles = [
        _FakeHandle(
            pymizu.MizuError("task handle already collected"),
            state="collected",
        ),
        _FakeHandle(pymizu.CancelledError("cancelled")),
        _FakeHandle(None, state="pending"),
    ]
    with pytest.raises(pymizu.TaskError) as exc_info:
        _map_mod._stream_fail(
            pymizu, handles, [0, 1, 2], [(0, 2), (2, 4), (4, 6)], first
        )
    assert exc_info.value.index == 3


def test_stream_fail_indexless_error_stays_fatal():
    # a drained error carrying no element index is not fn's (pool stop,
    # infrastructure): it re-raises rather than join the min selection
    first = _task_error(3)
    infra = pymizu.TaskError("attach failed")
    handles = [_FakeHandle(infra)]
    with pytest.raises(pymizu.TaskError) as exc_info:
        _map_mod._stream_fail(pymizu, handles, [0], [(0, 2)], first)
    assert exc_info.value is infra


def test_stream_fail_collect_failure_stays_fatal():
    # a genuine collect failure on a terminal sibling (a vanished payload
    # region, not the raced cancel consume) is not the drain's to swallow
    first = _task_error(3)
    handles = [
        _FakeHandle(pymizu.MizuError("payload region vanished"), state="ok")
    ]
    with pytest.raises(pymizu.MizuError, match="payload region vanished"):
        _map_mod._stream_fail(pymizu, handles, [0], [(0, 2)], first)


def test_stream_spec_fn_view_collect_raises(pool):
    # a spec f streams (per-chunk regions + kind-2 runners); only
    # collect="view" stays impossible — per-chunk output areas cannot
    # compose into one view
    with pytest.raises(TypeError, match="cannot compose a streaming spec"):
        pool.map(
            pymizu.call("math.sqrt"),
            [1.0, 4.0],
            stream=True,
            template=memoryview(b"\0" * 8),
            collect="view",
        )
    with pytest.raises(TypeError, match="cannot stream"):
        pool.map_prepare(pymizu.call("math.sqrt"), [1.0, 4.0], stream=True)


def test_stream_spec_fn_own_shape(pool):
    # a spec stream over Python workers: the runner results arrive in this
    # binding's own (ranges, values) shape
    out = pool.map(
        pymizu.call("math.sqrt"), [1.0, 4.0, 9.0], stream=True
    ).collect()
    assert out == [1.0, 2.0, 3.0]
    x = list(range(60))
    want = pool.map(pymizu.call("math.floor"), x).collect()
    for chunks in (None, 1, 7, 60):
        got = pool.map(
            pymizu.call("math.floor"), x, stream=True, n_chunks=chunks
        ).collect()
        assert got == want


def test_stream_spec_fn_seed_invariance(pool):
    fn = pymizu.call(source="import random\nrandom.random()")
    want = pool.map(fn, list(range(40)), seed=42).collect()
    for chunks in (None, 1, 7, 40):
        got = pool.map(
            fn, list(range(40)), stream=True, seed=42, n_chunks=chunks
        ).collect()
        assert got == want


def test_stream_spec_fn_error_index():
    fn = pymizu.call(source="if x in (3, 5):\n    raise ValueError('boom')\nx")
    # one worker: chunk 1 fails at 3 strictly before chunk 2 at 5 — the
    # first error's global index wins the minimum whether or not the
    # sibling's drains (its own chunk-base offset applied)
    p = pymizu.Pool.create(1)
    try:
        with pytest.raises(pymizu.TaskError) as exc_info:
            p.map(fn, list(range(8)), stream=True, n_chunks=4).collect()
        # the global element index: collect_any's `position` names the
        # chunk, the envelope's index offsets by its base
        assert exc_info.value.index == 3
        assert exc_info.value.remote_type == "ValueError"
    finally:
        p.stop()


def test_stream_buffer_x_forms(pool):
    assert pool.map(identity, bytes(range(10)), stream=True).collect() == list(
        range(10)
    )
    assert pool.map(
        identity, bytearray(range(10)), stream=True
    ).collect() == list(range(10))
    assert pool.map(
        identity, memoryview(bytes(range(10))), stream=True
    ).collect() == list(range(10))


def test_stream_list_x_shallow(pool):
    x = [[i] for i in range(6)]
    out = pool.map(identity, x, stream=True).collect()
    assert out == x
    # the submitter-side slice aliases; the elements then cross serialized
    assert all(a is not b and a == b for a, b in zip(out, x, strict=True))


def test_stream_prepared(pool):
    pm = pool.map_prepare(square, list(range(8)), stream=True)
    try:
        name = pm._name
        assert pm.run().collect() == [i * i for i in range(8)]
        assert pm.run().collect() == [i * i for i in range(8)]
        assert pm._name == name  # one region throughout
        # a replacement x of any shape re-slices: no restage
        assert pm.run(list(range(4))).collect() == [0, 1, 4, 9]
        assert pm._name == name
        assert pm.run([1, 2, 3]).collect() == [1, 4, 9]
        assert pm._name == name
    finally:
        pm.close()


def test_stream_prepared_seeded(pool):
    # a re-armed prepared run restarts every element's stream
    pm = pool.map_prepare(rand_elt, list(range(30)), stream=True)
    try:
        assert pm.run(seed=42).collect() == pm.run(seed=42).collect()
    finally:
        pm.close()


def test_stream_prepared_timeout_restages(pool):
    pm = pool.map_prepare(sleep_ident, [0.4, 0.1, 0.1], stream=True)
    try:
        name = pm._name
        assert pm.run().collect(timeout=0.2) is pymizu.TIMEOUT
        assert pm._capsule is None  # stale: the next run restages
        assert pm.run(timeout=60).collect() == [0.4, 0.1, 0.1]
        assert pm._name != name
    finally:
        pm.close()


def test_stream_prepared_error_restages(pool):
    pm = pool.map_prepare(fail_at, list(range(4)), args=(2,), stream=True)
    try:
        with pytest.raises(pymizu.TaskError):
            pm.run(timeout=60).collect()
        assert pm._capsule is None
        with pytest.raises(pymizu.TaskError) as exc_info:
            pm.run(timeout=60).collect()
        assert exc_info.value.index == 2
    finally:
        pm.close()


def test_stream_ctx_cache_swept_on_idle():
    # the worker's idle sweep drops completed maps' contexts (P-16): a
    # multi-chunk map's cache entry clears at the first idle beat
    p = pymizu.Pool.create(1)
    try:
        pm = p.map_prepare(square, list(range(16)), n_chunks=4, stream=True)
        try:
            assert pm.run().collect() == [i * i for i in range(16)]
            # poll until the completed map's context clears (pre-fix:
            # never); the sleeps let the worker park — a busy worker
            # never reaches the idle sweep
            names = None
            deadline = time.monotonic() + 15
            while names != [] and time.monotonic() < deadline:
                time.sleep(0.1)
                names = p.submit(map_ctx_names, []).collect(timeout=15)
            assert names == []
        finally:
            pm.close()
    finally:
        p.stop()


# -- numpy x sections and templates -----------------------------------------

np = pytest.importorskip("numpy", reason="numpy not installed")


def test_stream_numpy_x(pool):
    a = np.arange(1000, dtype=np.float64)
    assert pool.map(identity, a, stream=True).collect() == list(a)


def test_stream_numpy_int64_x(pool):
    a = np.arange(10, dtype=np.int64) * (1 << 40)
    out = pool.map(identity, a, stream=True).collect()
    assert out == list(a)
    assert all(isinstance(v, np.integer) for v in out)


def test_stream_numpy_strided_x_falls_back(pool):
    # a non-contiguous view is not raw-eligible: elements pickle per slice
    a = np.arange(20, dtype=np.float64)[::2]
    assert pool.map(identity, a, stream=True).collect() == list(a)


def test_stream_template_copy(pool):
    out = pool.map(
        scalar_double, list(range(100)), template=np.empty(1), stream=True
    ).collect()
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
    ).collect()
    assert isinstance(out, np.ndarray)
    assert not out.flags.writeable
    np.testing.assert_array_equal(out, np.arange(100) * 2.0)


def test_stream_template_m2(pool):
    out = pool.map(
        pair_up, list(range(50)), template=np.empty(2), stream=True
    ).collect()
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
            pm.run().collect(), np.array([2.0, 4.0, 6.0])
        )
        assert pm._name == name
        out = pm.run([1, 2, 3, 4]).collect()
        np.testing.assert_array_equal(out, np.array([2.0, 4.0, 6.0, 8.0]))
        assert pm._name != name  # the output area sizes off the staged n
    finally:
        pm.close()


def test_stream_prepared_template_view_transfers(pool):
    pm = pool.map_prepare(
        scalar_double,
        [1, 2, 3],
        template=np.empty(1),
        stream=True,
    )
    try:
        out = pm.run(collect="view").collect()
        assert not out.flags.writeable
        np.testing.assert_array_equal(out, np.array([2.0, 4.0, 6.0]))
        # the view pins its region: the next run stages fresh
        assert pm._capsule is None
        np.testing.assert_array_equal(
            pm.run(collect="view").collect(), np.array([2.0, 4.0, 6.0])
        )
    finally:
        pm.close()


# -- the spec window's fail path and deadline pre-checks (direct drives) ------


def _wait_states(*pairs):
    deadline = time.monotonic() + 10
    for h, want in pairs:
        while h.state != want:
            assert time.monotonic() < deadline
            time.sleep(0.01)


def test_stream_fail_spec_drains_siblings(pool):
    # the spec fail path's sibling drain: an already-consumed (None)
    # handle, a collected sibling, a completed sibling, and an errored
    # sibling whose element index offsets to its chunk's global range
    # before the minimum raises
    h_first = pool.submit(fail_with_map_index, 0, 2)
    h_ok = pool.submit(identity, 1)
    h_sib = pool.submit(fail_with_map_index, 0, 7)
    _wait_states((h_first, "err"), (h_ok, "ok"), (h_sib, "err"))
    first = None
    try:
        pool._h.collect_any([h_first], 5)
    except pymizu.TaskError as e:
        first = e  # position 0, element index 2 from the envelope
    assert first is not None
    handles = [None, h_first, h_ok, h_sib]
    ranges = [(0, 10), (10, 20), (20, 30), (30, 40)]
    with pytest.raises(pymizu.TaskError) as exc_info:
        _map_mod._stream_fail(
            pymizu, handles, [0, 1, 2, 3], ranges, first, [None] * 4
        )
    assert exc_info.value is first
    assert first.index == 2


def test_stream_fail_spec_sibling_without_index_raises(pool):
    # a sibling error that is not fn's (no element index) is fatal on the
    # spot, not merged into the first-by-index set
    h_first = pool.submit(fail_with_map_index, 0, 1)
    h_plain = pool.submit(fail_at, 0, 0)
    _wait_states((h_first, "err"), (h_plain, "err"))
    first = None
    try:
        pool._h.collect_any([h_first], 5)
    except pymizu.TaskError as e:
        first = e
    with pytest.raises(pymizu.TaskError) as exc_info:
        _map_mod._stream_fail(pymizu, [h_plain], [0], [(0, 4)], first, [None])
    assert not hasattr(exc_info.value, "index")
    assert exc_info.value.remote_type == "ValueError"


def test_stream_fail_spec_reraises_died():
    # a dead sibling's verdict wins over the first error: the re-raised
    # WorkerDiedError carries every dead sibling's chunk range as lost
    p = pymizu.Pool.create(2)
    try:
        h_first = p.submit(fail_with_map_index, 0, 1)
        h_die = p.submit(os._exit, 1)
        h_pend = p.submit(sleep_ident, 2)
        _wait_states((h_first, "err"), (h_die, "died"))
        first = None
        try:
            p._h.collect_any([h_first], 5)
        except pymizu.TaskError as e:
            first = e
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
            _map_mod._stream_fail(
                pymizu,
                [h_pend, h_die],
                [0, 1],
                [(0, 4), (4, 8)],
                first,
                [None, None],
            )
        assert exc_info.value.lost == [(4, 8)]
        h_pend.cancel()
    finally:
        p.stop()


def test_stream_window_spec_submit_expiry(pool):
    # the deadline expires inside a chunk's runner submit: the staged
    # chunk region is cancelled and closed, and the window reports
    # TIMEOUT. The scripted expiry stands in for a deadline landing
    # mid-stage, between the refill and submit pre-checks.
    plan = _map_mod._Plan(
        pool, pymizu.call(source="x"), (), None, None, None, True
    )
    win = _map_mod._StreamWindowSpec(
        pool,
        pymizu,
        [(0, 2), (2, 4)],
        1,
        plan,
        pymizu.call(source="x"),
        [1, 2, 3, 4],
        None,
        None,
        None,
    )
    remaining, _ = _map_mod._deadline(10)
    checks = iter([False, True])
    assert not win.refill(remaining, lambda: next(checks))
    assert win.capsules == [None, None]  # the staged chunk was closed


def test_stream_window_spec_loop_expiry(pool):
    # the refill landed a chunk but the deadline expired before the first
    # collect: TIMEOUT, with the chunk's region and runner handle handed
    # back for the caller's backstop
    plan = _map_mod._Plan(
        pool, pymizu.call(source="x"), (), None, None, None, True
    )
    win = _map_mod._StreamWindowSpec(
        pool,
        pymizu,
        [(0, 2), (2, 4)],
        1,
        plan,
        pymizu.call(source="x"),
        [1, 2, 3, 4],
        None,
        None,
        None,
    )
    remaining, _ = _map_mod._deadline(10)
    checks = iter([False, False, True])
    assert win.refill(remaining, lambda: next(checks))
    out = win.drive(remaining, lambda: next(checks))
    assert out is pymizu.TIMEOUT
    win.handles[0].cancel()
    pymizu._pymizu._map_close(win.capsules[0])


def test_stream_window_spec_refill_expiry(pool):
    # a chunk collected inside the deadline, the next refill past it:
    # TIMEOUT, every chunk region and runner consumed on the way out.
    # The scripted expiry stands in for a deadline landing between the
    # collect and the refill's pre-check.
    plan = _map_mod._Plan(
        pool, pymizu.call(source="x"), (), None, None, None, True
    )
    win = _map_mod._StreamWindowSpec(
        pool,
        pymizu,
        [(0, 2), (2, 4)],
        1,
        plan,
        pymizu.call(source="x"),
        [1, 2, 3, 4],
        None,
        None,
        None,
    )
    remaining, _ = _map_mod._deadline(10)
    checks = iter([False, False, False, True])
    assert win.refill(remaining, lambda: next(checks))
    out = win.drive(remaining, lambda: next(checks))
    assert out is pymizu.TIMEOUT
    assert win.capsules == [None, None]
    assert win.handles == [None, None]
