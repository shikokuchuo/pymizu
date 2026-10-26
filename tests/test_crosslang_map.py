"""Cross-language map tests: a Python submitter mapping specs over R
workers (the Phase 5 acceptance gate's this-direction half).

Skipped unless Rscript with an installed mizu is available — the fixture
dogfoods r_pool_launcher() so the probe logic has exactly one home.
"""

import os
import signal
import subprocess
import time

import pytest

import pymizu


@pytest.fixture(scope="module")
def r_pool():
    try:
        launcher = pymizu.r_pool_launcher(
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except pymizu.MizuError:
        pytest.skip("Rscript with the mizu package not available")
    p = pymizu.Pool.create(2, max_workers=3, launcher=launcher)
    yield p
    p.stop()


def test_name_kind(r_pool):
    assert r_pool.map(
        pymizu.call("stats::median"), [1.0, 5.0, 2.0]
    ).collect() == [
        1.0,
        5.0,
        2.0,
    ]
    # spec constants, and a numpy array crossing as an R numeric vector
    import numpy as np

    out = r_pool.map(
        pymizu.call(
            "stats::quantile", probs=np.array([0.25, 0.75]), names=False
        ),
        [np.array([1.0, 2.0, 3.0, 4.0])],
    ).collect()
    assert out[0] == pytest.approx([1.75, 3.25])


def test_source_kind(r_pool):
    # the element binds as x; named constants as names; positional as ..1
    assert r_pool.map(pymizu.call(source="x * 2"), [1, 2, 3]).collect() == [
        2,
        4,
        6,
    ]
    assert r_pool.map(pymizu.call(source="x + k", k=10), [1, 2]).collect() == [
        11,
        12,
    ]
    assert r_pool.map(
        pymizu.call(None, 10, source="x * ..1"), [1, 2]
    ).collect() == [
        10,
        20,
    ]
    # a statement prefix runs per element; the trailing expression's value
    assert r_pool.map(
        pymizu.call(source="y <- x + 1\ny * 3"), [1, 2]
    ).collect() == [
        6,
        9,
    ]


def test_template_and_view_collect(r_pool):
    import numpy as np

    x = np.arange(1.0, 6.0)
    out = r_pool.map(
        pymizu.call("base::log"), x, template=np.empty(1)
    ).collect()
    assert out == pytest.approx(np.log(x))
    v = r_pool.map(
        pymizu.call("base::log"), x, template=np.empty(1), collect="view"
    ).collect()
    assert np.asarray(v) == pytest.approx(np.log(x))
    # m > 1: the n x m matrix
    out2 = r_pool.map(
        pymizu.call("base::c", 2),
        [1.0, 2.0, 3.0],
        template=np.empty(2),
    ).collect()
    assert out2.shape == (3, 2)


def test_error_round_trip_with_element_index(r_pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        r_pool.map(
            pymizu.call(source='if (x == 3) stop("boom") else x'),
            [1, 2, 3, 4],
        ).collect()
    e = exc_info.value
    assert e.index == 2  # 0-based here, 1-based on the R side
    assert "boom" in str(e)
    assert e.remote_type is not None


def test_seed_invariance_within_r(r_pool):
    # batching- and steal-order invariance holds within a worker language
    fn = pymizu.call(source="runif(1)")
    a = r_pool.map(fn, list(range(30)), seed=42).collect()
    b = r_pool.map(fn, list(range(30)), seed=42, n_chunks=7).collect()
    assert a == b
    # the split-map contract across the neutral pair
    x = list(range(40))
    whole = r_pool.map(fn, x, seed=7).collect()
    assert (
        r_pool.map(fn, x[:20], seed=7).collect()
        + r_pool.map(fn, x[20:], seed=(7, 20)).collect()
        == whole
    )


def test_seed_outside_int32_on_r_workers_errors_locally(r_pool):
    with pytest.raises(TypeError, match="32-bit"):
        r_pool.map(pymizu.call("stats::median"), [1.0], seed=2**31)
    with pytest.raises(TypeError, match="32-bit"):
        r_pool.map(pymizu.call("stats::median"), [1.0], seed=-(2**31) - 1)


def test_seed_offset_past_exact_range_on_r_workers_errors_locally(r_pool):
    with pytest.raises(TypeError, match=r"2\^53"):
        r_pool.map(pymizu.call("stats::median"), [1.0], seed=(7, 2**53 + 1))
    # the boundary itself crosses and derives deterministically
    fn = pymizu.call(source="runif(1)")
    assert (
        r_pool.map(fn, [1, 2], seed=(7, 2**53)).collect()
        == r_pool.map(fn, [1, 2], seed=(7, 2**53)).collect()
    )


def test_prepared_spec_map_re_arms_without_restage(r_pool):
    pm = r_pool.map_prepare(
        pymizu.call(source="runif(1)"), list(range(12)), seed=7
    )
    try:
        name = pm._name
        r1 = pm.run().collect()
        assert pm._name == name
        r2 = pm.run().collect()
        assert pm._name == name
        # the prepared seed re-arms per run: identical streams
        assert r1 == r2
    finally:
        pm.close()


def test_prepared_swap_conversion_warning_escalates(r_pool):
    # a uint64 x converts to integer64 for R workers; values past
    # INT64_MAX warn after the write. Under warnings-as-errors the swap
    # raises, the staged state is dropped, and the next run restages
    # fresh.
    import warnings

    import numpy as np

    pm = r_pool.map_prepare(
        pymizu.call("base::identity"), np.arange(4, dtype=np.uint64)
    )
    try:
        assert pm.run().collect() == [0.0, 1.0, 2.0, 3.0]
        name = pm._name
        bad = np.full(4, 2**63, dtype=np.uint64)
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            with pytest.raises(RuntimeWarning, match="uint64"):
                pm.run(bad)
        assert pm._name is None  # the staged region was dropped
        assert pm.run(np.arange(4, dtype=np.uint64)).collect() == [
            0.0,
            1.0,
            2.0,
            3.0,
        ]
        assert pm._name != name
    finally:
        pm.close()


def test_a_dead_r_workers_lost_set_scan():
    # a killed worker mid-map reports the lost element ranges
    try:
        launcher = pymizu.r_pool_launcher(
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except pymizu.MizuError:
        pytest.skip("Rscript with the mizu package not available")
    p = pymizu.Pool.create(1, launcher=launcher)
    try:
        pid = p.dump()["workers"][0]["pid"]

        def kill_soon():
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if any(w["in_flight"] != -1 for w in p.dump()["workers"]):
                    os.kill(pid, signal.SIGKILL)
                    return
                time.sleep(0.02)
            raise AssertionError("the map never started")

        import threading

        killer = threading.Thread(target=kill_soon)
        killer.start()
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
            p.map(
                pymizu.call(source="Sys.sleep(0.2)\nx"),
                list(range(40)),
                n_chunks=8,
            ).collect()
        killer.join(30)
        # the lost ranges are 0-based half-open and cover the dead
        # runner's issued elements
        lost = exc_info.value.lost
        assert lost
        lo = min(lo for lo, _ in lost)
        hi = max(hi for _, hi in lost)
        assert lo == 0 and hi <= 40
    finally:
        p.stop()


def test_map_queued_behind_a_dead_r_worker_fails_not_hangs():
    # the lone R worker is inside the blocker, so the map's runners only
    # ever queue: the kill strands them with no claimant and no announce,
    # so no result slot ever carries the death verdict — before the
    # registry read in the collect loop this map parked to its timeout
    try:
        launcher = pymizu.r_pool_launcher(
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except pymizu.MizuError:
        pytest.skip("Rscript with the mizu package not available")
    p = pymizu.Pool.create(1, launcher=launcher)
    try:
        pid = p.dump()["workers"][0]["pid"]
        blocker = p.submit(pymizu.call(source="Sys.sleep(30)"))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if any(w["in_flight"] != -1 for w in p.dump()["workers"]):
                break
            time.sleep(0.05)
        else:
            pytest.fail("worker did not claim the blocker")
        # the map submits its runners while the worker is pinned: they
        # queue, and the kill strands them
        import threading

        out, err = [], []

        def run():
            try:
                out.append(
                    p.map(
                        pymizu.call("base::identity"),
                        list(range(40)),
                    ).collect(timeout=15)
                )
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
        t.join(20)
        assert not t.is_alive()
        assert not out
        assert len(err) == 1 and isinstance(err[0], pymizu.WorkerDiedError)
        assert err[0].slot is None
        assert err[0].pid is None
        blocker.cancel()
    finally:
        p.stop()


def test_map_spec_converted_dtypes(r_pool):
    # non-identity dtypes convert once at stage time into the region's raw
    # x section (the channel's conversion table), not the list fallback
    import numpy as np

    cases = {
        np.int8: [1, 2, 3],
        np.int16: [1, 2, 3],
        np.uint16: [1, 2, 3],
        np.uint32: [1, 2, 3],
        np.float16: [1, 2, 3],
        np.float32: [1, 2, 3],
        np.bool_: [True, False, True],
        np.complex64: [1 + 2j, 3 + 4j],
    }
    for dt, vals in cases.items():
        a = np.array(vals, dtype=dt)
        out = r_pool.map(pymizu.call(source="x * 2"), a).collect()
        assert [complex(v) for v in out] == [complex(v) * 2 for v in a]


def test_map_spec_uint64_past_int64_max_warns(r_pool):
    import numpy as np

    # in-range values convert exactly to integer64 both ways; past
    # INT64_MAX warns at the write and the lane lands NA_integer64_ on
    # the R side, the sentinel riding back (its read warns again)
    a = np.array([2**63, 1], dtype=np.uint64)
    with pytest.warns(RuntimeWarning, match="beyond"):
        out = r_pool.map(pymizu.call(source="x"), a).collect()
    assert out[0] == -(2**63) and out[1] == 1


def test_prepared_spec_map_converted_swap(r_pool):
    import numpy as np

    pm = r_pool.map_prepare(
        pymizu.call(source="x * 2"), np.arange(6, dtype=np.float32)
    )
    try:
        assert pm.run().collect() == [float(v) * 2 for v in range(6)]
        # a same-dtype replacement swaps in place: no restage
        name = pm._name
        assert pm.run(np.full(6, 3.0, dtype=np.float32)).collect() == [6.0] * 6
        assert pm._name == name
        # a wire-compatible float64 swaps in place too (the gate is the
        # staged wire type, not the source dtype); a different wire type
        # restages transparently
        assert pm.run(np.arange(6, dtype=np.int16)).collect() == [
            int(v) * 2 for v in range(6)
        ]
        assert pm._name != name
    finally:
        pm.close()


def test_stream_spec_map(r_pool):
    # the cross-language stream: per-chunk regions + kind-2 runners — the
    # results are the non-streaming spec map's
    fn = pymizu.call(source="x * 2")
    x = list(range(60))
    want = r_pool.map(fn, x).collect()
    for chunks in (None, 1, 7, 60):
        got = r_pool.map(fn, x, stream=True, n_chunks=chunks).collect()
        assert got == want


def test_stream_spec_map_raw_x(r_pool):
    # a numpy x slices through the per-chunk raw sections — an identity
    # dtype directly, a non-identity one after the conversion
    import numpy as np

    a = np.arange(100, dtype=np.float64)
    out = r_pool.map(
        pymizu.call(source="x * 2"), a, stream=True, n_chunks=6
    ).collect()
    assert out == [float(v) * 2 for v in a]
    b = np.arange(100, dtype=np.float32)
    out = r_pool.map(
        pymizu.call(source="x * 2"), b, stream=True, n_chunks=6
    ).collect()
    assert out == pytest.approx([float(v) * 2 for v in b])


def test_stream_spec_map_seed_invariance(r_pool):
    fn = pymizu.call(source="runif(1)")
    want = r_pool.map(fn, list(range(40)), seed=42).collect()
    for chunks in (None, 1, 7, 40):
        got = r_pool.map(
            fn, list(range(40)), stream=True, seed=42, n_chunks=chunks
        ).collect()
        assert got == want


def test_stream_spec_map_error_index(r_pool):
    fn = pymizu.call(source='if (x == 3) stop("boom") else x')
    with pytest.raises(pymizu.TaskError) as exc_info:
        r_pool.map(fn, list(range(10)), stream=True, n_chunks=5).collect()
    # the global element index (0-based here): collect_any's `position`
    # names the chunk, the envelope's index offsets by its base
    assert exc_info.value.index == 3
    assert "boom" in str(exc_info.value)


def test_stream_spec_map_error_index_min():
    # sibling errors drained into the minimum selection, offset by each
    # chunk's base — the same-language min-index discipline over R workers
    try:
        launcher = pymizu.r_pool_launcher(
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except pymizu.MizuError:
        pytest.skip("Rscript with the mizu package not available")
    p = pymizu.Pool.create(1, launcher=launcher)
    try:
        fn = pymizu.call(source='if (x %in% c(3, 5)) stop("boom") else x')
        with pytest.raises(pymizu.TaskError) as exc_info:
            p.map(fn, list(range(8)), stream=True, n_chunks=4).collect()
        assert exc_info.value.index == 3
    finally:
        p.stop()


def test_stream_spec_map_template(r_pool):
    import numpy as np

    out = r_pool.map(
        pymizu.call(source="x * 2"),
        list(range(50)),
        template=np.empty(1),
        stream=True,
        n_chunks=7,
    ).collect()
    np.testing.assert_array_equal(out, np.arange(50) * 2.0)
    out2 = r_pool.map(
        pymizu.call("base::c", 2),
        [1.0, 2.0, 3.0],
        template=np.empty(2),
        stream=True,
        n_chunks=2,
    ).collect()
    assert out2.shape == (3, 2)


def test_stream_spec_map_view_collect_raises(r_pool):
    import numpy as np

    with pytest.raises(TypeError, match="cannot compose a streaming spec"):
        r_pool.map(
            pymizu.call("base::log"),
            [1.0, 2.0],
            template=np.empty(1),
            collect="view",
            stream=True,
        )


def test_stream_spec_map_cancel(r_pool):
    # cancelling sets the fail-fast word on every staged chunk region
    fn = pymizu.call(source="Sys.sleep(0.2); x")
    m = r_pool.map(fn, list(range(20)), stream=True, n_chunks=4)
    m.cancel()
    with pytest.raises(pymizu.CancelledError, match="cancelled"):
        m.collect(timeout=15)


def test_stream_spec_map_stage_decline_aborts(r_pool):
    # a chunk the descriptor writer declines aborts the map at stage
    # time, closing the chunks already staged
    fn = pymizu.call("base::identity")
    x = list(range(40))
    x[20] = object()
    with pytest.raises(pymizu.DeclinedError):
        r_pool.map(fn, x, stream=True, n_chunks=2)
