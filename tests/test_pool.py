"""Pool tests: submit/collect, the outcome taxonomy, batching, worker
death, nested submit, and collect_any/all semantics, over real spawned
workers (``python -m pymizu.worker``)."""

import os
import pickle
import re
import signal
import subprocess
import sys
import threading
import time
from functools import partial

import pytest
from tests.helpers import (
    busy,
    fanout,
    fanout_with_thread,
    identity,
    make_unpicklable,
    raise_long,
    trace_to_file,
)

import pymizu


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


def test_submit_collect(pool):
    t = pool.submit(pow, 2, 10)
    assert t.collect(timeout=5) == 1024


def test_submit_kwargs(pool):
    t = pool.submit(round, 3.14159, ndigits=2)
    assert t.collect(timeout=5) == 3.14


def test_collect_timeout(pool):
    t = pool.submit(time.sleep, 1.5)
    assert t.collect(timeout=0.05) is pymizu.TIMEOUT
    assert t.collect(timeout=5) is None


def test_task_error(pool):
    t = pool.submit(len, 5)  # TypeError in the worker
    with pytest.raises(pymizu.TaskError) as exc_info:
        t.collect(timeout=5)
    exc = exc_info.value
    assert exc.remote_type == "TypeError"
    assert "object of type 'int' has no len()" in str(exc)


def test_task_error_traceback(pool):
    # a Python-level callable unwinds Python frames: the header crosses
    t = pool.submit(raise_long, "boom")
    with pytest.raises(pymizu.TaskError) as exc_info:
        t.collect(timeout=5)
    exc = exc_info.value
    assert exc.remote_type == "ValueError"
    assert "Traceback (most recent call last)" in exc.remote_traceback
    assert "raise_long" in exc.remote_traceback


def test_task_error_envelope_is_bounded(pool):
    t = pool.submit(raise_long, "x" * 100000)
    with pytest.raises(pymizu.TaskError) as exc_info:
        t.collect(timeout=10)
    exc = exc_info.value
    assert exc.remote_type == "ValueError"
    # the envelope truncates to the result slot's inline budget (472 bytes
    # at the default 512-byte slot size)
    assert len(str(exc)) < 1000
    assert len(exc.remote_traceback) < 1000


def test_unpicklable_result(pool):
    # the result's __reduce__ raises: the publish recovers as the task's
    # ERR result — fail the task, never the worker
    t = pool.submit(make_unpicklable)
    with pytest.raises(pymizu.TaskError) as exc_info:
        t.collect(timeout=5)
    assert exc_info.value.remote_type == "TypeError"
    # the worker survived
    assert pool.submit(len, [1, 2, 3]).collect(timeout=5) == 3


def test_lambda_submit(pool):
    try:
        import cloudpickle  # noqa: F401
    except ImportError:
        # 3.11+ raises PicklingError; 3.10 raises AttributeError
        with pytest.raises((pickle.PickleError, AttributeError)):
            pool.submit(lambda: 1)
    else:
        assert pool.submit(lambda: 41).collect(timeout=5) + 1 == 42


def test_cancel(pool):
    # both workers busy: the third task stays queued in the injection ring
    slow = [pool.submit(time.sleep, 2) for _ in range(2)]
    queued = pool.submit(len, [1])
    # collect before a worker consumes the cancelled entry: the CANCEL
    # verdict (once consumed, the worker frees the slot)
    assert queued.cancel() is True
    with pytest.raises(pymizu.CancelledError):
        queued.collect(timeout=5)
    assert queued.cancel() is False  # already cancelled
    for t in slow:
        t.collect(timeout=10)


def test_cancel_too_late(pool):
    t = pool.submit(len, [1, 2])
    assert t.collect(timeout=5) == 2
    assert t.cancel() is False  # completed: every edge folds to False


def test_submit_batch(pool):
    tasks = pool.submit_batch([partial(pow, 2, i) for i in range(10)])
    assert [t.collect(timeout=5) for t in tasks] == [2**i for i in range(10)]


def test_collect_any(pool):
    slow = pool.submit(time.sleep, 1.0)
    fast = pool.submit(len, [1, 2, 3])
    idx, val = pool.collect_any([slow, fast], timeout=10)
    assert (idx, val) == (1, 3)
    assert slow.collect(timeout=10) is None


def test_collect_any_error_index(pool):
    bad = pool.submit(len, 5)  # fails fast
    slow = pool.submit(time.sleep, 1.5)
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.collect_any([slow, bad], timeout=10)
    assert exc_info.value.index == 1
    # the other handle stays collectible
    assert slow.collect(timeout=10) is None


def test_collect_all(pool):
    tasks = [pool.submit(pow, 2, i) for i in range(6)]
    assert pool.collect_all(tasks, timeout=10) == [2**i for i in range(6)]


def test_collect_all_error_index(pool):
    tasks = [pool.submit(len, [i]) for i in range(3)]
    tasks.append(pool.submit(len, 5))
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.collect_all(tasks, timeout=10)
    assert exc_info.value.index == 3


def test_collect_all_timeout_consumes_nothing(pool):
    tasks = [pool.submit(time.sleep, 0.8) for _ in range(2)]
    assert pool.collect_all(tasks, timeout=0.05) is pymizu.TIMEOUT
    assert pool.collect_all(tasks, timeout=10) == [None, None]


def test_worker_died():
    p = pymizu.Pool.create(1)
    try:
        t = p.submit(os._exit, 1)  # the worker dies mid-task
        with pytest.raises(pymizu.WorkerDiedError) as exc_info:
            t.collect(timeout=15)
        assert exc_info.value.slot == 0
        assert exc_info.value.pid > 0
    finally:
        p.stop()


def test_nested_submit(pool):
    t = pool.submit(fanout, 8)
    assert t.collect(timeout=15) == sum(i * i for i in range(8))


def test_nested_collect_releases_gil(pool):
    out, beats = pool.submit(fanout_with_thread, 6).collect(timeout=15)
    assert out == sum(i * i for i in range(6))
    assert beats > 0  # the background thread ran through the nested waits


def test_steal_under_load(pool):
    tasks = [pool.submit(busy, i) for i in range(20)]
    assert pool.collect_all(tasks, timeout=30) == [i * 2 for i in range(20)]


def test_status_and_dump(pool):
    st = pool.status()
    assert st["role"] == "controller"
    assert st["workers"] == ["live", "live"]
    assert st["max_workers"] == 2
    d = pool.dump()
    assert d["name"] == st["name"]
    assert len(d["workers"]) == 2
    assert d["workers"][0]["status"] == "live"
    assert "local" in d and "collect_parks" in d["local"]


def test_stats(pool):
    assert pool.submit(busy, 1).collect(timeout=15) == 2
    # worker counters publish at park/fairness-tick cadence, so poll
    deadline = time.monotonic() + 10
    while True:
        st = pool.stats()
        if sum(x["tasks"] for x in st["workers"]) >= 1:
            break
        assert time.monotonic() < deadline
        time.sleep(0.05)
    assert len(st["workers"]) == 2
    w = st["workers"][0]
    assert w["status"] == "live"
    for key in ("pid", "steals", "injections", "parks", "helps", "deque"):
        assert key in w
    s = [x for x in st["submitters"] if x["status"] == "live"]
    assert len(s) == 1
    assert s[0]["injected"] >= 1
    assert s[0]["queued"] == s[0]["injected"] - s[0]["claimed"]
    for key in ("pid", "claimed", "spills", "spill_reuse"):
        assert key in s[0]


def test_trace(pool):
    events = []
    pool.trace(lambda ev, tid: events.append((ev, tid)))
    t = pool.submit(busy, 1)
    assert t.collect(timeout=15) == 2
    assert len(events) == 1
    assert events[0][0] == "submit"
    assert re.fullmatch(r"\d+:\d+", events[0][1])
    pool.trace(None)
    pool.submit(busy, 2).collect(timeout=15)
    assert len(events) == 1
    with pytest.raises(TypeError, match="callable or None"):
        pool.trace(42)


def test_trace_worker_side(tmp_path):
    with pymizu.Pool.create(1) as p:
        out = tmp_path / "trace.log"
        assert p.submit(trace_to_file, str(out)).collect(timeout=15) is True
        assert p.submit(busy, 1).collect(timeout=15) == 2
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if out.exists() and len(out.read_text().splitlines()) >= 3:
                break
            time.sleep(0.05)
        events = [line.split(" ", 1) for line in out.read_text().splitlines()]
        # the install task's own "done" fires after the hook registers
        assert [ev for ev, _ in events] == ["done", "start", "done"]
        assert all(re.fullmatch(r"\d+:\d+", tid) for _, tid in events)


def test_stop_idempotent_and_stopped(pool):
    t = pool.submit(len, [1])
    assert t.collect(timeout=5) == 1
    pa = pymizu.Pool.attach(pool.token)  # a second handle on the same pool
    assert pool.stop(timeout=5) is True
    assert pool.stop(timeout=5) is True
    # the controller's own handle is dead after its stop
    with pytest.raises(pymizu.MizuError, match="closed"):
        pool.submit(len, [1])
    # an attached submitter reads the shutdown flag as StoppedError
    with pytest.raises(pymizu.StoppedError):
        pa.submit(len, [1])
    pa.destroy()


def test_retire_and_spawn():
    p = pymizu.Pool.create(1, max_workers=2)
    try:
        p.retire(0)
        deadline = time.monotonic() + 10
        while p.status()["workers"][0] != "free":
            assert time.monotonic() < deadline
            time.sleep(0.05)
        slots = p.spawn_workers(1)
        assert slots == [0]
        assert p.submit(len, [1, 2]).collect(timeout=10) == 2
    finally:
        p.stop()


def test_attach_submitter(pool):
    # a second process joins as a submitter and collects its own result
    prog = """
import sys
import pymizu
p = pymizu.Pool.attach(sys.argv[1])
t = p.submit(len, [1, 2, 3, 4])
print(t.collect(timeout=10))
p.destroy()
"""
    out = subprocess.run(
        [sys.executable, "-c", prog, pool.token],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "4"


def test_startup_error():
    def launcher(token, slot):
        return subprocess.Popen([sys.executable, "-c", "pass"])

    with pytest.raises(pymizu.StartupError):
        pymizu.Pool.create(1, startup_timeout=1.0, launcher=launcher)


def test_task_state(pool):
    t = pool.submit(time.sleep, 0.5)
    assert t.state in ("pending", "ok")  # racy by design
    assert t.collect(timeout=5) is None
    assert t.state == "collected"


def test_uncollected_handle_released(pool):
    # an uncollected handle's finalizer cancels/frees its slot
    t = pool.submit(len, [1])
    del t
    import gc

    gc.collect()
    deadline = time.monotonic() + 5
    while True:
        if sum(pool.status()["tasks"].values()) == 0:
            break
        assert time.monotonic() < deadline
        time.sleep(0.05)


@pytest.mark.skipif(os.name == "nt", reason="no fork on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fork_guard(pool):
    pid = os.fork()
    if pid == 0:
        try:
            pool.submit(len, [1])
        except Exception:
            os._exit(0)
        os._exit(1)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert pool.submit(len, [1, 2]).collect(timeout=5) == 2


@pytest.mark.skipif(os.name == "nt", reason="SIGINT differs on Windows")
def test_collect_interrupt():
    p = pymizu.Pool.create(1)
    p.submit(time.sleep, 3)
    timer = threading.Timer(0.3, lambda: signal.raise_signal(signal.SIGINT))
    timer.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            p.submit(time.sleep, 3).collect()
    finally:
        timer.cancel()
        # the interrupted collect consumed nothing; stop cancels the queued
        # task and waits out the 3-second one
        assert p.stop(timeout=15) is True


# -- numpy (results ride the raw tiers) ---------------------------------------

np = pytest.importorskip("numpy", reason="numpy not installed")


def test_numpy_result_rawvec(pool):
    t = pool.submit(np.arange, 12, dtype=np.float64)
    b = t.collect(timeout=5)
    assert isinstance(b, np.ndarray)
    assert b.dtype == np.float64
    assert np.array_equal(b, np.arange(12, dtype=np.float64))


def test_numpy_result_region(pool):
    # past the inline budget: a pool has no arena — the result rides a
    # named region (RAWSPILL pool framing), never pickle
    a = np.arange(200000, dtype=np.float64)
    b = pool.submit(identity, a).collect(timeout=15)
    assert b.dtype == np.float64
    assert np.array_equal(b, a)
    d = pool.dump()
    assert any(s["spills"] > 0 for s in d["submitters"])


# -- zero-copy views (SHM_VEC) ----------------------------------------------

np = pytest.importorskip("numpy", reason="numpy not installed")


def test_shm_vec_result(pool):
    from tests.helpers import big_array

    t = pool.submit(big_array, 100000)  # 800 KB back as a view
    b = t.collect(timeout=5)
    assert isinstance(b, np.ndarray)
    assert np.array_equal(b, np.arange(100000, dtype=np.float64))
    assert not b.flags.writeable


def test_shm_vec_argument(pool):
    from tests.helpers import array_sum

    a = np.arange(100000, dtype=np.float64)  # 800 KB there as a view
    t = pool.submit(array_sum, a)
    assert t.collect(timeout=5) == float(a.sum())
