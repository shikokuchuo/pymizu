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

def test_large_array_arg_crosses_by_reference(r_pool):
    import numpy as np

    big = np.random.default_rng(0).random(200_000)  # 1.6 MB
    # the value crosses, and the R worker proves the zero-copy arrival
    assert r_pool.submit(pymizu.call("base::mean", big)).collect() == \
        pytest.approx(float(big.mean()))
    src = 'if (.Call(mizu:::mizu_zc_view_check, x)) "view" else "copy"'
    assert r_pool.submit(pymizu.call(source=src, x=big)).collect() == "view"


def test_received_view_resent_as_arg(r_pool):
    import numpy as np

    # receive a view on the Python side first: an ndarray over a _ShmView
    big = r_pool.submit(
        pymizu.call(source="runif(200000, 0.5, 1.5)")
    ).collect()
    assert type(big.base).__name__ == "_ShmView"
    # re-send it as a task argument: REF, REFHELD, the per-task loan balanced
    assert r_pool.submit(pymizu.call("base::mean", big)).collect() == \
        pytest.approx(float(big.mean()))
    assert big.base.flags & 1 == 1  # REFHELD
    rc0 = big.base.refcount
    r_pool.submit(pymizu.call("base::mean", big)).collect()
    assert big.base.refcount == rc0
    # result-is-the-arg: arrives intact, a view, elevated by our own add
    res = r_pool.submit(pymizu.call(source="x", x=big)).collect()
    assert type(res.base).__name__ == "_ShmView"
    assert big.base.refcount >= rc0 + 1  # plus the async producer loan
    np.testing.assert_array_equal(res, big)


def test_multiple_ref_args_and_a_nested_view(r_pool):
    import numpy as np

    a = np.random.default_rng(1).random(100_000)
    b = np.random.default_rng(2).random(100_000)
    va = r_pool.submit(pymizu.call("base::identity", a)).collect()
    vb = r_pool.submit(pymizu.call("base::identity", b)).collect()
    assert type(va.base).__name__ == "_ShmView"
    assert type(vb.base).__name__ == "_ShmView"
    big = np.random.default_rng(3).random(100_000)
    # two positional REFs, a named REF, a fresh SHM_VEC, one nested in a list
    t = r_pool.submit(
        pymizu.call(
            None,
            va,
            vb,
            source="sum(..1) + sum(..2) + sum(x[[1]]) + sum(y)",
            x=[va],
            y=vb,
        )
    )
    expected = float(a.sum() + b.sum() + a.sum() + b.sum())
    assert t.collect() == pytest.approx(expected)
    rc0a, rc0b = va.base.refcount, vb.base.refcount
    t2 = r_pool.submit(pymizu.call("base::mean", big))
    assert t2.collect() == pytest.approx(float(big.mean()))
    assert va.base.refcount == rc0a
    assert vb.base.refcount == rc0b


def test_ref_candidate_declines_without_taskref():
    import numpy as np

    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        # R workers without the ref reader (caps 7: no TASKREF bit)
        _pymizu._pool_worker_join(h.token, 0, _ident=(2, 7))
        pool = pymizu.Pool._wrap(h)
        big = np.ones(200_000)
        with pytest.raises(pymizu.DeclinedError, match="by-reference"):
            pool.submit(pymizu.call("base::mean", big))
        # an ordinary argument submits unchanged
        assert pool.submit(pymizu.call("base::sum", [1, 2, 3])) is not None
    finally:
        h.destroy()


def test_ref_to_a_vanished_region_fails_the_task(r_pool):
    ident = 3 | (15 << 32)  # this build's word as the submitter identity
    stream = _pymizu._write_task(
        "base::mean", 0, (_pymizu._IxRef("/mizu_0_0"),), {}, 2, ident
    )
    # a hand-crafted task stream rides a RAWVEC entry to the worker's
    # task-stream dispatch
    t = r_pool._h.submit(stream, None)
    with pytest.raises(pymizu.TaskError, match="not found"):
        t.collect()


def test_view_x_crosses_to_map_workers_as_a_ref(r_pool):
    import numpy as np

    big = r_pool.submit(pymizu.call(source="runif(100000)")).collect()
    assert type(big.base).__name__ == "_ShmView"
    rc0 = big.base.refcount
    res = r_pool.map(pymizu.call("base::sqrt"), big)
    assert res == np.sqrt(big).tolist()
    assert big.base.flags & 1 == 1  # REFHELD
    # the workers' map-context views release at eviction / teardown; the
    # count never drops below the pre-map value while they hold them
    assert big.base.refcount >= rc0


def _run_probe(pool, paths, args, kwargs):
    # worker-side probe: a view flag per candidate path (a selected
    # candidate arrives as a view), plus a checksum
    if not paths:
        src = "list(logical(0), 0)"
    else:
        checks = ", ".join(
            f".Call(mizu:::mizu_zc_view_check, {q})" for q in paths
        )
        sums = ", ".join(f"sum(as.numeric({q}))" for q in paths)
        src = f"list(c({checks}), sum(c({sums})))"
    flags, s = pool.submit(
        pymizu.call(None, *args, source=src, **kwargs)
    ).collect()
    if isinstance(flags, bool):
        flags = [flags]
    return [bool(f) for f in flags], s


def test_zc_selection_fold_candidate_matrix(r_pool):
    import numpy as np

    rng = np.random.default_rng(0)
    cand = rng.random(5120)  # 40960 bytes — past the floor

    def run(paths, *args, **kwargs):
        return _run_probe(r_pool, paths, args, kwargs)

    # the selection flip of a lone candidate bisects the inline budget
    lo, hi = 0, 40000
    while lo < hi:
        mid = (lo + hi + 1) // 2
        flags, _ = run(["..1"], cand, "a" * mid)
        if flags == [True]:
            lo = mid
        else:
            hi = mid - 1
    # k = 1, the boundary exact: adjusted == inline_max selects (the fit
    # is inclusive); adjusted == inline_max + 1 stays by value
    flags, s = run(["..1"], cand, "a" * lo)
    assert flags == [True]
    assert s == pytest.approx(float(cand.sum()))
    flags, _ = run(["..1"], cand, "a" * (lo + 1))
    assert flags == [False]

    # k = 1 at the middle and tail positions, and nested in a list
    assert run(["..2"], 1.5, cand, "x")[0] == [True]
    assert run(["n1"], 1.5, n1=cand)[0] == [True]
    assert run(["..1[[2]]"], [1.5, cand])[0] == [True]

    # k >= 2 never selects (each adjusted total carries the other
    # candidates by value); 17 candidates exercise the record cap
    assert run(["..1", "..2"], cand, rng.random(25000))[0] == [False, False]
    c17 = [rng.random(5120 + i) for i in range(17)]
    assert run([f"..{j}" for j in range(1, 18)], *c17)[0] == [False] * 17

    # k = 0, inline and spilled by-value totals
    assert run([], 1.5, "abc")[0] == []
    assert run([], "y" * 30000)[0] == []
