"""Pool tests: submit/collect, the outcome taxonomy, batching, worker
death, nested submit, and collect_any/all semantics, over real spawned
workers (``python -m pymizu.worker``)."""

import gc
import os
import pickle
import re
import signal
import subprocess
import sys
import threading
import time
import types
from functools import partial

import pytest
from tests.helpers import (
    BadCallable,
    big_array,
    busy,
    fail_indexed,
    fanout,
    fanout_with_thread,
    identity,
    large_args_integrity,
    make_fail_on_unpickle,
    make_unpicklable,
    raise_deep,
    raise_keyboard_interrupt,
    raise_long,
    raise_long_named,
    raise_long_named_short_message,
    raise_system_exit,
    raise_unprintable,
    raise_with_nonint_map_index,
    raise_with_surrogate_message,
    takes_timeout,
    trace_to_file,
    warn_then,
)

import pymizu
from pymizu import _pymizu


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


def test_submit_collect(pool):
    t = pool.submit(pow, 2, 10)
    assert t.collect(timeout=5) == 1024


def test_task_result_alias(pool):
    t = pool.submit(pow, 2, 10)
    assert t.result(timeout=5) == 1024


def test_submit_argument_validation(pool):
    with pytest.raises(TypeError):
        pool._h.submit()
    with pytest.raises(TypeError):
        pool._h.stop(1, 2)
    with pytest.raises(TypeError):
        pool._h.ready_wait()
    with pytest.raises(TypeError):
        _pymizu._pool_worker_join("x")
    t = pool.submit(len, [1])
    with pytest.raises(TypeError):
        t.collect(1, 2)
    assert t.collect(timeout=5) == 1


def test_submit_on_destroyed_pool(pool):
    pool.destroy()
    with pytest.raises(ValueError, match="pool handle is closed"):
        pool.submit(len, [1])


def test_submit_kwargs(pool):
    t = pool.submit(round, 3.14159, ndigits=2)
    assert t.collect(timeout=5) == 3.14


def test_submit_container_args(pool):
    # the task frame's flat container leaves: a tuple and a dict decode
    # on the worker as themselves
    assert pool.submit(sum, (1, 2, 3)).collect(timeout=5) == 6
    t = pool.submit(sorted, {"b": 1, "a": 2})
    assert t.collect(timeout=5) == ["a", "b"]


def test_submit_large_by_value_args(pool):
    # arguments past the entry inline budget stage the task frame's
    # by-value spill: the payload crosses intact
    b = bytes(range(256)) * 4096  # 1 MiB
    s = "mizu-" * 200_000  # 1 MB
    t = pool.submit(large_args_integrity, b, s)
    want = (len(b), sum(b), b[:8], b[-8:], len(s), s[:8], s[-8:])
    assert t.collect(timeout=15) == want


def test_submit_rawspill_result(pool):
    # a bytes result between the inline budget and the zero-copy floor
    # spills a bare-bytes region (RAWSPILL): read back as a uint8 array
    np = pytest.importorskip("numpy")
    out = pool.submit(identity, b"x" * 10000).collect(timeout=10)
    assert isinstance(out, np.ndarray)
    assert out.tobytes() == b"x" * 10000


def test_collect_timeout(pool):
    t = pool.submit(time.sleep, 1.5)
    assert t.collect(timeout=0.05) is pymizu.TIMEOUT
    assert t.collect(timeout=5) is None


def test_collect_exactly_once(pool):
    t = pool.submit(len, [1, 2])
    assert t.collect(timeout=5) == 2
    with pytest.raises(pymizu.MizuError, match="already collected"):
        t.collect(timeout=0)


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
    # the envelope truncates to the result slot's inline budget (240 bytes
    # at the default 256-byte slot size)
    assert len(str(exc)) < 1000
    assert len(exc.remote_traceback) < 1000


def test_task_error_envelope_sheds_traceback_first(pool):
    # the truncation ladder: the traceback shrinks before the message is
    # touched
    t = pool.submit(raise_deep, 200, "boom")
    with pytest.raises(pymizu.TaskError) as exc_info:
        t.collect(timeout=10)
    exc = exc_info.value
    assert "boom" in str(exc)
    assert exc.remote_traceback.startswith("Traceback (most recent call last)")
    assert len(exc.remote_traceback) < 250


def test_task_error_envelope_drops_traceback_then_truncates(pool):
    # past the traceback: it empties, then the message halves
    t = pool.submit(raise_long, "x" * 5000)
    with pytest.raises(pymizu.TaskError) as exc_info:
        t.collect(timeout=10)
    exc = exc_info.value
    assert exc.remote_traceback == ""
    assert str(exc) == "ValueError: " + "x" * (len(str(exc)) - 12)
    assert len(str(exc)) < 300


def test_task_error_unprintable_exception(pool):
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.submit(raise_unprintable).collect(timeout=10)
    exc = exc_info.value
    assert exc.remote_type == "UnprintableError"
    assert "<unprintable exception>" in str(exc)


def test_task_error_untransportable_condition():
    # a tiny slot and a long type name: the fixed fallback message
    p = pymizu.Pool.create(1, slot_size=128)
    try:
        with pytest.raises(pymizu.TaskError, match="untransportable"):
            p.submit(raise_long_named, 500).collect(timeout=10)
    finally:
        p.stop()


def test_task_error_truncate_noop_at_the_half_budget(pool):
    # the message cut reached with the message already inside the half
    # budget: a no-op, and the fallback still follows (the type name
    # alone is too long)
    with pytest.raises(pymizu.TaskError, match="untransportable"):
        pool.submit(raise_long_named_short_message).collect(timeout=10)


def test_task_error_surrogate_message_blanks(pool):
    # the private envelope cannot encode a lone surrogate: the message
    # blanks (the 'I' err stream backslash-escapes instead)
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.submit(raise_with_surrogate_message).collect(timeout=10)
    exc = exc_info.value
    assert exc.remote_type == "ValueError"
    assert str(exc) == "ValueError: "


def test_task_error_nonint_map_index_dropped(pool):
    # a user exception's non-int _pymizu_map_index is dropped silently —
    # it never masquerades as a runner's element index
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.submit(raise_with_nonint_map_index).collect(timeout=10)
    exc = exc_info.value
    assert exc.remote_type == "ValueError"
    assert "boom" in str(exc)
    assert not hasattr(exc, "index")


def test_task_warning_passes_through(pool):
    assert pool.submit(warn_then, 21).collect(timeout=5) == 42


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


def test_submit_ring_full_raises():
    # ring capacity 2 with the one worker busy: polls raise, never stall
    p = pymizu.Pool.create(1, injection_cap=2)
    try:
        blocker = p.submit(time.sleep, 2)
        fills = []
        with pytest.raises(pymizu.SubmitTimeoutError):
            for _ in range(8):
                fills.append(p.submit(len, [1], timeout=0))
        blocker.collect(timeout=10)
        for t in fills:  # accepted handles stay valid and collectible
            assert t.collect(timeout=5) == 1
    finally:
        p.stop()


def test_submit_runner_ring_full_raises():
    # a runner submission polls the same ring and raises the same error
    p = pymizu.Pool.create(1, injection_cap=2)
    try:
        blocker = p.submit(time.sleep, 2)
        runner = _pymizu._runner_frame("/mizu_x", 0, None)
        with pytest.raises(pymizu.SubmitTimeoutError):
            for _ in range(8):
                p._h._submit_runner(runner, timeout=0)
        blocker.collect(timeout=10)
        p.stop()
        with pytest.raises(pymizu.MizuError, match="pool handle is closed"):
            p._h._submit_runner(runner, timeout=0)
    finally:
        p.stop()


def test_submit_batch_short_on_full_ring():
    # a full ring ends the batch short (no raise); accepted handles collect
    p = pymizu.Pool.create(1, injection_cap=4)
    try:
        blocker = p.submit(time.sleep, 2)
        fills = []
        with pytest.raises(pymizu.SubmitTimeoutError):
            for _ in range(8):
                fills.append(p.submit(len, [1], timeout=0))
        payloads = [partial(len, [i]) for i in range(4)]
        tasks = p.submit_batch(payloads, timeout=0)
        assert len(tasks) < 4
        blocker.collect(timeout=10)
        for t in fills + tasks:
            assert t.collect(timeout=5) == 1
    finally:
        p.stop()


def test_submit_blocked_until_worker_pops():
    # a full ring parks the submitter; a worker's pop wakes it
    p = pymizu.Pool.create(1, injection_cap=2)
    try:
        blocker = p.submit(time.sleep, 0.5)
        fills = []
        with pytest.raises(pymizu.SubmitTimeoutError):
            for _ in range(8):
                fills.append(p.submit(len, [1], timeout=0))
        t = p.submit(len, [1], timeout=5)  # parks until the worker drains
        blocker.collect(timeout=10)
        for u in fills + [t]:
            assert u.collect(timeout=5) == 1
    finally:
        p.stop()


def test_slots_exhausted():
    # two result slots per submitter: the third outstanding submit raises
    p = pymizu.Pool.create(1, max_submitters=1, result_slots=2)
    try:
        t1 = p.submit(len, [1])
        t2 = p.submit(len, [2])
        with pytest.raises(pymizu.SlotsExhaustedError):
            p.submit(len, [3], timeout=5)
        assert t1.collect(timeout=5) == 1
        assert t2.collect(timeout=5) == 1
        # collected slots are reused
        assert p.submit(len, [3]).collect(timeout=5) == 1
    finally:
        p.stop()


def test_submit_callable_timeout_kwarg(pool):
    # timeout= is the submission keyword: the callable's own rides partial
    t = pool.submit(partial(takes_timeout, timeout=3), timeout=5)
    assert t.collect(timeout=5) == 3


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
    # only the reported handle is consumed: the results ahead of it stay
    # collectible; the reported one is spent
    assert [t.collect(timeout=5) for t in tasks[:3]] == [1, 1, 1]
    with pytest.raises(pymizu.MizuError, match="already collected"):
        tasks[3].collect(timeout=0)


def test_collect_all_timeout_consumes_nothing(pool):
    tasks = [pool.submit(time.sleep, 0.8) for _ in range(2)]
    assert pool.collect_all(tasks, timeout=0.05) is pymizu.TIMEOUT
    assert pool.collect_all(tasks, timeout=10) == [None, None]


def test_collect_any_all_validate_tasks(pool):
    for verb in (pool.collect_any, pool.collect_all):
        with pytest.raises(ValueError, match="non-empty"):
            verb([])
        with pytest.raises(TypeError, match="not a task handle"):
            verb([1])
    pa = pymizu.Pool.attach(pool.token)  # a second handle on the same pool
    t1 = pool.submit(len, [1])
    t2 = pa.submit(len, [1, 2])
    with pytest.raises(ValueError, match="same pool handle"):
        pool.collect_any([t1, t2], timeout=0)
    assert t1.collect(timeout=5) == 1
    with pytest.raises(pymizu.MizuError, match="already collected"):
        pool.collect_all([t1], timeout=0)
    assert t2.collect(timeout=5) == 2
    pa.destroy()


def test_collect_any_cancelled_index(pool):
    slow = [pool.submit(time.sleep, 2) for _ in range(2)]  # both busy
    runs = pool.submit(len, [1])
    never = pool.submit(len, [2])
    assert never.cancel() is True
    with pytest.raises(pymizu.CancelledError) as exc_info:
        pool.collect_any([runs, never], timeout=10)
    assert exc_info.value.index == 1
    assert runs.collect(timeout=10) == 1  # the other handle stays valid
    for t in slow:
        t.collect(timeout=10)


def test_collect_all_cancelled_index(pool):
    slow = [pool.submit(time.sleep, 2) for _ in range(2)]  # both busy
    never = pool.submit(len, [1])
    also = pool.submit(len, [2])
    assert never.cancel() is True
    assert also.cancel() is True
    # both terminal at the call: no wait, the first by position raises
    with pytest.raises(pymizu.CancelledError) as exc_info:
        pool.collect_all([never, also], timeout=10)
    assert exc_info.value.index == 0
    # a cancel reads without consuming: the later handle still collects
    with pytest.raises(pymizu.CancelledError):
        also.collect(timeout=0)
    for t in slow:
        t.collect(timeout=10)


def test_collect_all_read_failure_attaches_prefix(pool):
    # a mid-batch read failure: the exception carries the failing slot's
    # index and the consumed prefix (P-04)
    t0 = pool.submit(abs, -1)
    t1 = pool.submit(make_fail_on_unpickle)
    t2 = pool.submit(abs, -2)
    with pytest.raises(ValueError, match="test unpickle failure") as ei:
        pool.collect_all([t0, t1, t2], timeout=15)
    assert ei.value.index == 1
    assert ei.value.values == [1]
    # a pool read failure never consumes: the failing slot fails again,
    # and every handle past the prefix stays collectible
    with pytest.raises(ValueError, match="test unpickle failure"):
        t1.collect(timeout=5)
    assert t2.collect(timeout=5) == 2


def test_submit_batch_mid_failure_attaches_accepted(pool):
    # a mid-batch stage failure: the exception carries the accepted
    # handles, which stay valid and collectible (P-05)
    with pytest.raises(TypeError, match="unpicklable on purpose") as ei:
        pool.submit_batch([partial(abs, -1), BadCallable()])
    accepted = ei.value.accepted
    assert len(accepted) == 1
    assert accepted[0].collect(timeout=15) == 1


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


def test_task_lost():
    # the claimant's own gone verdict (no death) collects as TaskLostError,
    # distinct from WorkerDiedError (C-03); the _exec_fail(2) arm stands in
    # for the gone read — the real trigger is a transient open failure
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        wk = _pymizu._pool_worker_join(h.token, 0)
        wk._exec_fail(2)
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        pool = pymizu.Pool._wrap(h)
        t = pool.submit(identity, 1)
        deadline = time.monotonic() + 10
        while t.state != "lost" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert t.state == "lost"
        with pytest.raises(
            pymizu.TaskLostError, match="payload region vanished"
        ):
            t.collect(timeout=10)
        # the worker lives on: a later task runs once the arm clears
        wk._exec_fail(0)
        assert pool.submit(identity, 2).collect(timeout=10) == 2
        pool.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


def test_sweep_hook_error_is_unraisable():
    # the idle sweep hook (pymizu._map._sweep_ctx_cache) reports a failure
    # as unraisable, never into the core; a fresh process so the monkeypatch
    # lands before the binding's one-time function lookup caches it
    import textwrap

    res = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import threading
                import time

                import pymizu
                import pymizu._map
                from pymizu import _pymizu


                def boom():
                    raise RuntimeError("sweep boom")


                pymizu._map._sweep_ctx_cache = boom

                h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
                try:
                    wk = _pymizu._pool_worker_join(h.token, 0)
                    loop = threading.Thread(
                        target=lambda: (wk.run(), wk.leave()), daemon=True
                    )
                    loop.start()
                    # the idle worker's empty step fires the sweep hook
                    time.sleep(2)
                    pool = pymizu.Pool._wrap(h)
                    pool.stop()
                    loop.join(timeout=10)
                    wk.destroy()
                finally:
                    h.destroy()
                """
            ),
        ],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr
    assert "sweep boom" in res.stderr


def _run_err_envelope_script(script):
    # a fresh process driving an in-process worker: the err envelope's
    # helpers run with the script's own module state, never this suite's
    import textwrap

    res = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr
    return res


_ERR_POOL_PROLOG = """
import threading

import pymizu
from pymizu import _pymizu


def fail():
    raise ValueError("boom")


h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
try:
    wk = _pymizu._pool_worker_join(h.token, 0)
    loop = threading.Thread(
        target=lambda: (wk.run(), wk.leave()), daemon=True
    )
    loop.start()
    pool = pymizu.Pool._wrap(h)


    def check():
        try:
            pool.submit(fail).collect(timeout=10)
            raise SystemExit("expected TaskError")
        except pymizu.TaskError as e:
            assert e.remote_type == "ValueError"
            assert e.remote_traceback == "", repr(e.remote_traceback)
"""

_ERR_POOL_EPILOG = """
    pool.stop()
    loop.join(timeout=10)
    wk.destroy()
finally:
    h.destroy()
"""


def test_err_envelope_traceback_import_degrades():
    # a blocked traceback import, then a missing format_exception, then a
    # raising one: the envelope's traceback text degrades to empty, the
    # worker publishes on (the one-time resolve stays unset until a
    # format_exception exists, so one process serves all three)
    _run_err_envelope_script(
        _ERR_POOL_PROLOG
        + """
    import sys
    import types

    sys.modules["traceback"] = None
    check()
    mod = types.ModuleType("traceback")
    sys.modules["traceback"] = mod
    check()


    def boom(*a):
        raise RuntimeError("no traceback")


    mod.format_exception = boom
    check()
"""
        + _ERR_POOL_EPILOG
    )


def test_err_envelope_traceback_join_degrades():
    # a format_exception whose parts are not all str: the join fails and
    # the traceback text degrades to empty, never into the worker
    _run_err_envelope_script(
        _ERR_POOL_PROLOG
        + """
    import traceback

    traceback.format_exception = lambda *a: ["boom: ", 42]
    check()
"""
        + _ERR_POOL_EPILOG
    )


@pytest.mark.skipif(os.name == "nt", reason="RLIMIT_NOFILE is Unix-only")
def test_task_lost_on_a_transient_region_open_failure():
    # the real ctx->gone arm: fd starvation fails the worker's open of a
    # spilled task region; the task fails as LOST, the worker lives on
    resource = pytest.importorskip("resource")
    np = pytest.importorskip("numpy")
    import fcntl

    def open_fds():
        n = 0
        for fd in range(256):
            try:
                fcntl.fcntl(fd, fcntl.F_GETFD)
                n += 1
            except OSError:
                pass
        return n

    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        pool = pymizu.Pool._wrap(h)
        # the worker joins (mapping its regions) and the tasks stage ahead
        # of the starvation: only the worker's region *reads* starve
        wk = _pymizu._pool_worker_join(h.token, 0)
        big = np.arange(200_000, dtype=np.float64)
        t1 = pool.submit(identity, big)  # a BUFREF leaf (inline frame)
        t2 = pool.submit(partial(identity, big))  # a spilled (SHM_RAW) entry
        # arm the starvation: the fd count can drift, so probe each try
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
            loop = threading.Thread(
                target=lambda: (wk.run(), wk.leave()), daemon=True
            )
            loop.start()
            with pytest.raises(pymizu.TaskLostError):
                t1.collect(timeout=10)
            with pytest.raises(pymizu.TaskLostError):
                t2.collect(timeout=10)
        finally:
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
        assert pool.submit(identity, 2).collect(timeout=10) == 2
        pool.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


@pytest.mark.skipif(os.name == "nt", reason="RLIMIT_NOFILE is Unix-only")
def test_create_and_stage_under_fd_starvation():
    # region creation needs an fd: the create verbs raise MizuShmError,
    # and a task frame's BUFREF leaf degrades to a pickle that cannot
    # spill either — the submit raises, the staged handle stays consistent
    resource = pytest.importorskip("resource")
    np = pytest.importorskip("numpy")
    import fcntl

    def open_fds():
        n = 0
        for fd in range(256):
            try:
                fcntl.fcntl(fd, fcntl.F_GETFD)
                n += 1
            except OSError:
                pass
        return n

    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        pool = pymizu.Pool._wrap(h)
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
            with pytest.raises(pymizu.MizuError):
                _pymizu._pool_new(1, 8, 64, 64, 64, 512)
            with pytest.raises(pymizu.MizuError):
                _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
            # the BUFREF leaf's spill checkout fails, then pickle's
            big = np.arange(200_000, dtype=np.float64)
            with pytest.raises(pymizu.MizuError):
                pool.submit(identity, big)
        finally:
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
        wk = _pymizu._pool_worker_join(h.token, 0)
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        assert pool.submit(identity, 2).collect(timeout=10) == 2
        pool.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


def test_task_decode_failure_is_the_task_error():
    # the _exec_fail(1) arm stands in for a corrupt task payload: the
    # decode failure publishes as the task's ERR, and the worker lives on
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        wk = _pymizu._pool_worker_join(h.token, 0)
        wk._exec_fail(1)
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        pool = pymizu.Pool._wrap(h)
        t = pool.submit(identity, 1)
        with pytest.raises(pymizu.TaskError, match="corrupt task payload"):
            t.collect(timeout=10)
        wk._exec_fail(0)
        assert pool.submit(identity, 2).collect(timeout=10) == 2
        pool.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


def test_task_corrupt_shape_is_the_task_error():
    # a payload that decodes but is no (fn, args, kwargs) triple fails the
    # shape check and publishes as the task's ERR; the worker lives on
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        wk = _pymizu._pool_worker_join(h.token, 0)
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        pool = pymizu.Pool._wrap(h)
        t = pool._h.submit(pickle.dumps(["not", "a", "task"]), None)
        with pytest.raises(pymizu.TaskError, match="corrupt task payload"):
            t.collect(timeout=10)
        assert pool.submit(identity, 2).collect(timeout=10) == 2
        pool.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


def _phantom_fn():
    return 42  # never runs: its reference must not resolve on the worker


def test_task_fn_reference_module_gone():
    # a by-reference task fn whose module the worker cannot import
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        wk = _pymizu._pool_worker_join(h.token, 0)
        pool = pymizu.Pool._wrap(h)
        module = _phantom_fn.__module__
        _phantom_fn.__module__ = "mizu_phantom_mod"
        try:
            t = pool.submit(_phantom_fn)
        finally:
            _phantom_fn.__module__ = module
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        with pytest.raises(pymizu.TaskError, match="mizu_phantom_mod"):
            t.collect(timeout=10)
        pool.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


def test_task_fn_reference_attr_gone():
    # the module imports on the worker but the qualname's attribute is gone
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        wk = _pymizu._pool_worker_join(h.token, 0)
        pool = pymizu.Pool._wrap(h)
        sys.modules["mizu_phantom_mod"] = types.ModuleType("mizu_phantom_mod")
        module = _phantom_fn.__module__
        _phantom_fn.__module__ = "mizu_phantom_mod"
        try:
            t = pool.submit(_phantom_fn)
        finally:
            _phantom_fn.__module__ = module
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        try:
            with pytest.raises(pymizu.TaskError, match="AttributeError"):
                t.collect(timeout=10)
        finally:
            del sys.modules["mizu_phantom_mod"]
        pool.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


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


def test_shutdown_alias(pool):
    t = pool.submit(len, [1])
    assert t.collect(timeout=5) == 1
    assert pool.shutdown(timeout=5) is True
    with pytest.raises(pymizu.MizuError, match="closed"):
        pool.submit(len, [1])


def test_stop_timeout_warns():
    # a worker mid-task cannot observe the shutdown inside the timeout:
    # stop returns False and warns (the worker exits on its own after)
    p = pymizu.Pool.create(1)
    t = p.submit(time.sleep, 1.5)  # bound: a dropped handle cancels
    deadline = time.monotonic() + 5
    while p.dump()["submitters"][0]["claimed"] != 1:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    with pytest.warns(UserWarning, match="timed out"):
        assert p.stop(timeout=0.05) is False
    assert p.stop(timeout=10) is True
    assert t.cancel() is False


def test_worker_base_exception_task_exits_1():
    # a task's SystemExit escapes the exec hook: the worker goes down
    # (status 1) and the leave handshake fails the in-flight task DIED
    procs = []

    def launcher(token, slot):
        procs.append(pymizu._default_worker_launcher()(token, slot))
        return procs[-1]

    p = pymizu.Pool.create(1, launcher=launcher)
    try:
        t = p.submit(raise_system_exit)
        with pytest.raises(pymizu.WorkerDiedError):
            t.collect(timeout=15)
        assert procs[0].wait(timeout=15) == 1
    finally:
        p.stop()


def test_worker_keyboard_interrupt_task_exits_2():
    # the KeyboardInterrupt tier of a task's BaseException escape
    procs = []

    def launcher(token, slot):
        procs.append(pymizu._default_worker_launcher()(token, slot))
        return procs[-1]

    p = pymizu.Pool.create(1, launcher=launcher)
    try:
        t = p.submit(raise_keyboard_interrupt)
        with pytest.raises(pymizu.WorkerDiedError):
            t.collect(timeout=15)
        assert procs[0].wait(timeout=15) == 2
    finally:
        p.stop()


def test_retired_worker_lingers_for_uncollected_result():
    # a retained (SHM_VEC) result keeps the retired worker mapped as its
    # lifetime anchor — plain sleeps, no unpark can reach a released slot
    # — until the collect drops the keeper
    procs = []

    def launcher(token, slot):
        procs.append(pymizu._default_worker_launcher()(token, slot))
        return procs[-1]

    p = pymizu.Pool.create(1, launcher=launcher)
    try:
        t = p.submit(big_array, 100000)
        deadline = time.monotonic() + 10
        while t.state != "ok" and time.monotonic() < deadline:
            time.sleep(0.01)
        p.retire(0)
        time.sleep(2.5)  # a linger beat
        assert procs[0].poll() is None
        assert len(t.collect(timeout=10)) == 100000
        # the keeper dropped: the next lame_duck reap ends the linger
        assert procs[0].wait(timeout=15) == 0
    finally:
        p.stop()


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


def test_raising_launcher_destroys_the_pool():
    tokens = []

    def launcher(token, slot):
        tokens.append(token)
        raise RuntimeError("boom")

    exc = None
    try:
        pymizu.Pool.create(1, launcher=launcher)
    except RuntimeError as e:
        exc = e
    assert exc is not None
    with pytest.raises(pymizu.MizuError):
        pymizu.Pool.attach(tokens[0])


def test_pool_repr(pool):
    assert re.fullmatch(
        rf"<pymizu\.Pool {re.escape(pool.token)} \(controller\)>",
        repr(pool._h),
    )
    pa = pymizu.Pool.attach(pool.token)
    assert "(submitter)" in repr(pa._h)
    pa.destroy()
    assert repr(pa._h) == "<pymizu.Pool (closed)>"


def test_task_repr_and_pool(pool):
    t = pool.submit(identity, 3)
    assert t.collect(timeout=5) == 3
    assert re.fullmatch(r"<pymizu\.Task seq=\d+ slot=\d+>", repr(t))
    assert t.pool is pool._h


def test_controller_only_verbs(pool):
    pa = pymizu.Pool.attach(pool.token)
    with pytest.raises(pymizu.MizuError, match="only the controller"):
        pa.retire(0)
    with pytest.raises(pymizu.MizuError, match="only the controller"):
        pa._h.ready_wait(1, 0)
    with pytest.raises(pymizu.MizuError, match="only the controller"):
        pa.stop()
    with pytest.raises(ValueError, match="worker slot out of range"):
        pool.retire(99)
    pa.destroy()


def test_pool_verb_argument_validation(pool):
    with pytest.raises(TypeError):
        pool._h.submit_batch()
    with pytest.raises(TypeError):
        pool._h.collect_any()
    with pytest.raises(TypeError):
        pool._h.collect_all()
    with pytest.raises(TypeError):
        pool._h.ready_wait(["x"], 1)
    with pytest.raises(ValueError, match="worker slot out of range"):
        pool._h.ready_wait([-1], 0)
    with pytest.raises(pymizu.MizuError, match="not a worker handle"):
        pool._h.run()
    with pytest.raises(pymizu.MizuError, match="not a worker handle"):
        pool._h.leave()
    with pytest.raises(TypeError):
        pool._h._submit_runner()


def test_collect_any_all_other_pool_handle(pool):
    # tasks all of one other handle: rejected before any wait
    other = pymizu.Pool.create(1)
    try:
        t = other.submit(len, [1])
        for verb in (pool.collect_any, pool.collect_all):
            with pytest.raises(ValueError, match="this pool handle"):
                verb([t], timeout=0)
        assert t.collect(timeout=5) == 1
    finally:
        other.stop()


def test_submit_batch_on_a_stopped_pool():
    # the whole batch fails before the first accept: the bare raise
    p = pymizu.Pool.create(1)
    p.stop()
    with pytest.raises(pymizu.MizuError):
        p.submit_batch([len])


def test_pool_stop_default_timeout():
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        assert h.stop() is True  # the no-argument timeout
        with pytest.raises(TypeError):
            h.stop("x")
    finally:
        h.destroy()


def test_pool_new_geometry_validation():
    from pymizu import _pymizu

    ok = (1, 8, 64, 64, 64, 512)
    cases = [
        ((0, *ok[1:]), "max_workers"),
        ((1, 0, *ok[2:]), "max_submitters"),
        ((1, 65, *ok[2:]), "max_submitters"),
        ((*ok[:2], 3, *ok[3:]), "injection_cap"),
        ((*ok[:3], 3, *ok[4:]), "per_worker_cap"),
        ((*ok[:4], 4, ok[5]), "result_slots"),
        ((*ok[:5], 100), "slot_size"),
    ]
    for args, msg in cases:
        with pytest.raises(ValueError, match=msg):
            _pymizu._pool_new(*args)
    with pytest.raises(ValueError, match="malformed join token"):
        _pymizu._pool_attach("not-a-token")
    with pytest.raises(ValueError, match="malformed join token"):
        _pymizu._pool_worker_join("not-a-token", 0)


def test_pool_attach_unknown_token():
    with pytest.raises(pymizu.MizuError):
        _pymizu._pool_attach("0_1")
    with pytest.raises(pymizu.MizuError):
        _pymizu._pool_worker_join("0_1", 0)


def test_retire_slot_out_of_range(pool):
    with pytest.raises(pymizu.MizuError, match="out of range"):
        pool.retire(3)


def test_ready_wait_bad_slot_type(pool):
    with pytest.raises(TypeError):
        pool._h.ready_wait(["x"])


def test_collect_any_on_a_closed_pool():
    p = pymizu.Pool.create(1)
    t = p.submit(identity, 1)
    assert t.collect(timeout=10) == 1
    assert p.stop() is True
    p._h.destroy()
    with pytest.raises(ValueError, match="closed"):
        p.collect_any([t])


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


@pytest.mark.skipif(os.name == "nt", reason="no fork on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fork_child_stop():
    # the child's stop() detaches its own copy as a no-op, never stopping
    # the parent's pool
    pool = pymizu.Pool.create(1)
    try:
        pid = os.fork()
        if pid == 0:
            try:
                os._exit(0 if pool.stop() is True else 1)
            except Exception:
                os._exit(1)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert pool.submit(len, [1, 2]).collect(timeout=5) == 2
    finally:
        pool.stop()


@pytest.mark.skipif(os.name == "nt", reason="no fork on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fork_child_dealloc():
    pool = pymizu.Pool.create(2)
    try:
        pid = os.fork()
        if pid == 0:
            # the child's dealloc abandons only its own copies
            del pool
            gc.collect()
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert pool.submit(len, [1, 2]).collect(timeout=5) == 2
    finally:
        pool.stop()


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


def test_numpy_result_int_sentinels_unscanned(pool):
    # the copied-read scans are gated on a foreign writer (3.2): a pool
    # result's writer is the pool's worker word, and Python workers stage
    # identity int32/int64 — a genuine -2^31/-2^63 stays a value, unwarned
    b = pool.submit(np.array, [1, -(2**31), 3], dtype=np.int32).collect(
        timeout=5
    )
    assert b.dtype == np.int32
    assert list(b) == [1, -(2**31), 3]
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        b = pool.submit(np.array, [1, -(2**63)], dtype=np.int64).collect(
            timeout=5
        )
    assert b.dtype == np.int64
    assert list(b) == [1, -(2**63)]


def test_numpy_result_region(pool):
    # past the inline budget: a pool has no arena — a fresh result rides a
    # named region (RAWSPILL pool framing), never pickle
    a = np.arange(200000, dtype=np.float64)
    b = pool.submit(np.copy, a).collect(timeout=15)
    assert b.dtype == np.float64
    assert np.array_equal(b, a)
    d = pool.dump()
    assert any(s["spills"] > 0 for s in d["submitters"])


def test_view_argument_returned_whole_is_a_ref(pool):
    # a BUFREF argument arrives as a view; returned whole it publishes as
    # REF — the region's name, no result region — after marking it REFHELD
    a = np.arange(200000, dtype=np.float64)
    b = pool.submit(identity, a).collect(timeout=15)
    assert np.array_equal(b, a)
    assert b.base.flags & 1
    d = pool.dump()
    assert all(s["spills"] == 0 for s in d["submitters"])


def test_masked_array_keeps_its_mask(pool):
    # a strict ndarray subclass keeps the lossless pickle path: the raw
    # tier would carry the base buffer only, dropping the mask
    m = np.ma.MaskedArray([1.0, 2.0, 3.0], mask=[True, False, True])
    r = pool.submit(identity, m).collect(timeout=15)
    assert isinstance(r, np.ma.MaskedArray)
    assert list(r.mask) == [True, False, True]
    assert list(r.data) == [1.0, 2.0, 3.0]


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


def test_collect_any_position_stamp(pool):
    # an error already carrying a map-style element index keeps it as
    # `index` (the documented Pool.map contract) and gains the 0-based
    # `position` alongside — a streaming spec map's window finds the
    # chunk through it
    bad = pool.submit(fail_indexed)
    slow = pool.submit(time.sleep, 1.5)
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.collect_any([slow, bad], timeout=10)
    assert exc_info.value.index == 7
    assert exc_info.value.position == 1
    assert slow.collect(timeout=10) is None


def test_collect_all_position_stamp(pool):
    bad = pool.submit(fail_indexed)
    ok = pool.submit(len, [1, 2])
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.collect_all([bad, ok], timeout=10)
    assert exc_info.value.index == 7
    assert exc_info.value.position == 0
    assert ok.collect(timeout=10) == 2
