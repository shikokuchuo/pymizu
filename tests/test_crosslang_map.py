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
    assert r_pool.map(pymizu.call("stats::median"), [1.0, 5.0, 2.0]) == [
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
    )
    assert out[0] == pytest.approx([1.75, 3.25])


def test_source_kind(r_pool):
    # the element binds as x; named constants as names; positional as ..1
    assert r_pool.map(pymizu.call(source="x * 2"), [1, 2, 3]) == [2, 4, 6]
    assert r_pool.map(pymizu.call(source="x + k", k=10), [1, 2]) == [11, 12]
    assert r_pool.map(pymizu.call(None, 10, source="x * ..1"), [1, 2]) == [
        10,
        20,
    ]
    # a statement prefix runs per element; the trailing expression's value
    assert r_pool.map(pymizu.call(source="y <- x + 1\ny * 3"), [1, 2]) == [
        6,
        9,
    ]


def test_template_and_view_collect(r_pool):
    import numpy as np

    x = np.arange(1.0, 6.0)
    out = r_pool.map(pymizu.call("base::log"), x, template=np.empty(1))
    assert out == pytest.approx(np.log(x))
    v = r_pool.map(
        pymizu.call("base::log"), x, template=np.empty(1), collect="view"
    )
    assert np.asarray(v) == pytest.approx(np.log(x))
    # m > 1: the n x m matrix
    out2 = r_pool.map(
        pymizu.call("base::c", 2),
        [1.0, 2.0, 3.0],
        template=np.empty(2),
    )
    assert out2.shape == (3, 2)


def test_error_round_trip_with_element_index(r_pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        r_pool.map(
            pymizu.call(source='if (x == 3) stop("boom") else x'),
            [1, 2, 3, 4],
        )
    e = exc_info.value
    assert e.index == 2  # 0-based here, 1-based on the R side
    assert "boom" in str(e)
    assert e.remote_type is not None


def test_seed_invariance_within_r(r_pool):
    # batching- and steal-order invariance holds within a worker language
    fn = pymizu.call(source="runif(1)")
    a = r_pool.map(fn, list(range(30)), seed=42)
    b = r_pool.map(fn, list(range(30)), seed=42, n_chunks=7)
    assert a == b
    # the split-map contract across the neutral pair
    x = list(range(40))
    whole = r_pool.map(fn, x, seed=7)
    assert r_pool.map(fn, x[:20], seed=7) + r_pool.map(
        fn, x[20:], seed=(7, 20)
    ) == whole


def test_seed_outside_int32_on_r_workers_errors_locally(r_pool):
    with pytest.raises(TypeError, match="32-bit"):
        r_pool.map(pymizu.call("stats::median"), [1.0], seed=2**31)
    with pytest.raises(TypeError, match="32-bit"):
        r_pool.map(pymizu.call("stats::median"), [1.0], seed=-(2**31) - 1)


def test_prepared_spec_map_re_arms_without_restage(r_pool):
    pm = r_pool.map_prepare(
        pymizu.call(source="runif(1)"), list(range(12)), seed=7
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
            )
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
