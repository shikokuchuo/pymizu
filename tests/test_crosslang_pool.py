"""Cross-language pool tests: a Python submitter driving R workers.

Skipped unless Rscript with an installed mizu (source-drop support) is
available — the fixture dogfoods r_pool_launcher() so the probe logic
has exactly one home.
"""

import os
import signal
import subprocess
import time

import pytest

import pymizu
from pymizu import _pymizu


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


def test_both_kinds_and_result_values(r_pool):
    import numpy as np

    # name kind, with result values per the interchange table (a numpy
    # array crosses as an R numeric vector; a Python list as an R list)
    t = r_pool.submit(
        pymizu.call(
            "stats::quantile",
            np.array([1.0, 2.0, 3.0, 4.0]),
            probs=np.array([0.25, 0.5, 0.75]),
            names=False,
        )
    )
    assert t.collect() == pytest.approx([1.75, 2.5, 3.25])
    t2 = r_pool.submit(pymizu.call("base::sqrt", 16))
    assert t2.collect() == 4
    # source kind: statement prefix + trailing expression -> the value
    src = "y <- x * 2\ny + 1"
    assert r_pool.submit(pymizu.call(source=src, x=20)).collect() == 41
    # R's eval semantics: an assignment is an expression (its value), a
    # source with no trailing value yields NULL
    assert r_pool.submit(pymizu.call(source="z <- 1")).collect() == 1
    assert r_pool.submit(pymizu.call(source="invisible()")).collect() is None
    # an R list result crosses as a Python list
    t3 = r_pool.submit(pymizu.call(source='list(1.5, "two", NULL)'))
    assert t3.collect() == [1.5, "two", None]
    # a large vector result crosses as a zero-copy view (Python's
    # capability mask admits MIZH): an ndarray over the _ShmView base
    big = r_pool.submit(pymizu.call(source="runif(10000)")).collect()
    assert type(big.base).__name__ == "_ShmView"
    assert len(big) == 10000


def test_errors_cross_with_remote_type(r_pool):
    with pytest.raises(pymizu.TaskError) as ei:
        r_pool.submit(pymizu.call("base::log", "x")).collect()
    assert ei.value.remote_type == "simpleError"
    assert "non-numeric" in str(ei.value)
    # a bare unqualified name errors at submit, never reaches a worker
    with pytest.raises(TypeError, match="qualified name"):
        r_pool.submit(pymizu.call("log", 1.5))
    # a non-portable argument declines at submit
    with pytest.raises(pymizu.DeclinedError):
        r_pool.submit(pymizu.call("base::print", {1, 2, 3}))


def test_non_portable_result_fails_the_task(r_pool):
    with pytest.raises(pymizu.TaskError) as ei:
        r_pool.submit(
            pymizu.call(source="lm(mpg ~ wt, mtcars)")
        ).collect()
    assert ei.value.remote_type == "mizu_error_not_portable"
    assert "not portable" in str(ei.value)


def test_nested_submit_inside_a_foreign_task(r_pool):
    src = (
        "t <- mizu::mizu_submit(mizu::mizu_current_pool(), x * 2, x = 21L)\n"
        "mizu::mizu_collect(t)"
    )
    assert r_pool.submit(pymizu.call(source=src)).collect() == 42


def test_collect_any_all_cross_language(r_pool):
    t1 = r_pool.submit(pymizu.call(source="1 + 1"))
    t2 = r_pool.submit(pymizu.call(source='stop("boom")'))
    idx, value = r_pool.collect_any([t1, t2])
    assert (idx, value) == (0, 2)
    t3 = r_pool.submit(pymizu.call(source="1 + 1"))
    t4 = r_pool.submit(pymizu.call(source='stop("boom")'))
    with pytest.raises(pymizu.TaskError) as ei:
        r_pool.collect_all([t3, t4])
    assert ei.value.index == 1
    assert ei.value.remote_type == "simpleError"
    assert t3.collect() == 2


def test_a_dead_r_worker_surfaces_worker_died(r_pool):
    p = pymizu.Pool.create(1, launcher=pymizu.r_pool_launcher(
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    try:
        pid = p.dump()["workers"][0]["pid"]
        t = p.submit(pymizu.call(source="Sys.sleep(30)"))
        deadline = time.monotonic() + 10
        claimed = False
        while time.monotonic() < deadline and not claimed:
            claimed = any(
                w["in_flight"] != -1 for w in p.dump()["workers"]
            )
            time.sleep(0.05)
        assert claimed
        os.kill(pid, signal.SIGKILL)
        with pytest.raises(pymizu.WorkerDiedError):
            t.collect()
    finally:
        p.stop()


def test_a_worker_of_another_language_cannot_join(r_pool):
    # the pool word is R's: an in-process Python join on the free slot
    # fails the exact-match CAS, before any task exists
    with pytest.raises(pymizu.MizuError):
        _pymizu._pool_worker_join(r_pool.token, 2)
