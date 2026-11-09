"""PoolExecutor tests: the concurrent.futures.Executor adapter — submit,
map, wait/as_completed, cancellation, and shutdown semantics. Resolution
is event-driven off the pool's notify endpoint: no drain thread anywhere.
"""

import asyncio
import concurrent.futures
import operator
import threading
import time

import pytest
from tests.helpers import fail_at, sleep_ident, square

import pymizu


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


def test_submit_result(pool):
    with pymizu.PoolExecutor(pool) as ex:
        fut = ex.submit(square, 6)
        assert isinstance(fut, concurrent.futures.Future)
        assert fut.result(timeout=5) == 36


def test_submit_call_spec(pool):
    # a pymizu.call spec takes the facade submit path and resolves like
    # a callable's task
    with pymizu.PoolExecutor(pool) as ex:
        fut = ex.submit(pymizu.call("math.sqrt", 81))
        assert fut.result(timeout=5) == 9.0


def test_submit_non_callable_raises(pool):
    # the facade path's callable gate surfaces at the executor
    with pymizu.PoolExecutor(pool) as ex:
        with pytest.raises(TypeError, match="callable"):
            ex.submit(42)


def test_map_in_order(pool):
    with pymizu.PoolExecutor(pool) as ex:
        assert list(ex.map(square, range(10))) == [i * i for i in range(10)]


def test_map_multi_iterable(pool):
    with pymizu.PoolExecutor(pool) as ex:
        assert list(ex.map(operator.mul, [1, 2, 3], [4, 5, 6])) == [4, 10, 18]


def test_wait_and_as_completed(pool):
    with pymizu.PoolExecutor(pool) as ex:
        futs = [ex.submit(square, i) for i in range(6)]
        done, not_done = concurrent.futures.wait(futs, timeout=5)
        assert not not_done
        assert {f.result() for f in done} == {i * i for i in range(6)}
    with pymizu.PoolExecutor(pool) as ex:
        # the late submit overtakes: resolution rides the publish's wake,
        # so the quick square completes well ahead of the 1s sleep
        futs = [ex.submit(sleep_ident, 1.0), ex.submit(square, 3)]
        first = next(concurrent.futures.as_completed(futs, timeout=5))
        assert first.result() == 9


def test_burst_resolves_all(pool):
    # a burst larger than any internal batching: every future resolves
    n = 300
    with pymizu.PoolExecutor(pool) as ex:
        futs = [ex.submit(square, i) for i in range(n)]
        assert {f.result(timeout=10) for f in futs} == {
            i * i for i in range(n)
        }


def test_submit_from_many_threads(pool):
    # the concurrent.futures contract: submit() is thread-safe — the
    # executor's one lock serializes the pool handle's single-producer
    # injection ring (unserialized, the core's producer guard raises)
    threads, per = 4, 50
    with pymizu.PoolExecutor(pool) as ex:

        def burst(base):
            return [ex.submit(square, base + i) for i in range(per)]

        with concurrent.futures.ThreadPoolExecutor(threads) as tp:
            bases = (base * per for base in range(threads))
            futs = [f for batch in tp.map(burst, bases) for f in batch]
        assert {f.result(timeout=10) for f in futs} == {
            i * i for i in range(threads * per)
        }


def test_future_timeout(pool):
    with pymizu.PoolExecutor(pool) as ex:
        fut = ex.submit(sleep_ident, 0.5)
        with pytest.raises(concurrent.futures.TimeoutError):
            fut.result(timeout=0.05)
        assert fut.result(timeout=5) == 0.5


def test_future_exception(pool):
    with pymizu.PoolExecutor(pool) as ex:
        fut = ex.submit(fail_at, 4, 4)
        with pytest.raises(pymizu.TaskError):
            fut.result(timeout=5)


def test_cancel_queued():
    p = pymizu.Pool.create(1)
    try:
        with pymizu.PoolExecutor(p) as ex:
            blocker = ex.submit(sleep_ident, 0.5)
            fut = ex.submit(square, 3)
            assert fut.cancel()
            assert fut.cancelled()
            assert blocker.result(timeout=5) == 0.5
            with pytest.raises(concurrent.futures.CancelledError):
                fut.result()
    finally:
        p.stop()


def test_shutdown_blocks_new_submit(pool):
    ex = pymizu.PoolExecutor(pool)
    assert ex.submit(square, 2).result(timeout=5) == 4
    ex.shutdown()
    with pytest.raises(RuntimeError, match="shutdown"):
        ex.submit(square, 2)


def test_cancelled_claim_race_recovers():
    # a cancelled queued task is freed by the worker's CANCEL->FREE skip,
    # so the scan reads its slot as terminal and the drain's collect fails
    # "already collected": the drain drops the cancelled future and moves
    # on rather than failing everything outstanding (P-03)
    p = pymizu.Pool.create(2)
    try:
        ex = pymizu.PoolExecutor(p)
        f0 = ex.submit(sleep_ident, 0.6)  # both workers busy ...
        fb = ex.submit(sleep_ident, 0.6)
        deadline = time.monotonic() + 10
        while not all(w["in_flight"] != -1 for w in p.dump()["workers"]):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        f1 = ex.submit(square, 1)  # ... so f1 is still queued here
        assert f1.cancel()  # the claim skip frees it CANCEL->FREE
        deadline = time.monotonic() + 5
        while f1._task.state != "collected":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        f2 = ex.submit(square, 2)
        assert f0.result(timeout=5) == 0.6
        assert fb.result(timeout=5) == 0.6
        assert f2.result(timeout=5) == 4
        assert f1.cancelled()
        ex.shutdown(wait=False)
    finally:
        p.stop()


def test_stale_handle_recovers(pool):
    # a task collected outside the drain frees its slot: the next wake's
    # scan reads it as terminal and its collect fails "already collected"
    # — the drain fails just that future and moves on (P-03). The outside
    # collect wins the wake for its own slot, so another task's publish
    # prods the scan that picks the stale handle up.
    ex = pymizu.PoolExecutor(pool)
    try:
        f0 = ex.submit(sleep_ident, 0.3)
        f1 = ex.submit(sleep_ident, 0.6)
        # collected outside the drain (the outside collect claims its wake)
        assert f1._task.collect(timeout=5) == 0.6
        f2 = ex.submit(square, 2)
        assert f0.result(timeout=5) == 0.3
        assert f2.result(timeout=5) == 4
        with pytest.raises(pymizu.MizuError, match="already collected"):
            f1.result(timeout=5)
    finally:
        ex.shutdown()


def test_broken_executor_fails_and_blocks_submit(pool, monkeypatch):
    # the drain's scan-and-collect fails with an infrastructure error not
    # attributable to a handle: every outstanding future fails with it
    # and later submissions raise it instead of queueing onto the broken
    # executor
    err = pymizu.MizuError("infrastructure failure")

    def fail_collect(tasks):
        raise err

    with pymizu.PoolExecutor(pool) as ex:
        fut = ex.submit(sleep_ident, 0.5)
        monkeypatch.setattr(ex, "_collect_terminal", fail_collect)
        ex._drain()  # the publish's wake fires the same path
        with pytest.raises(pymizu.MizuError) as exc_info:
            fut.result(timeout=5)
        assert exc_info.value is err
        with pytest.raises(pymizu.MizuError) as exc_info:
            ex.submit(square, 2)
        assert exc_info.value is err


def test_destroy_racing_drain_fails_cleanly():
    # destroy mid-flight: the outstanding future fails with the
    # closed-handle error (the destroy hook, never a lost wake)
    p = pymizu.Pool.create(1)
    try:
        ex = pymizu.PoolExecutor(p)
        fut = ex.submit(sleep_ident, 0.5)
        p.destroy()
        with pytest.raises(ValueError, match="closed"):
            fut.result(timeout=5)
        ex.shutdown(wait=False)
    finally:
        p.destroy()


def test_shutdown_cancel_futures():
    p = pymizu.Pool.create(1)
    try:
        ex = pymizu.PoolExecutor(p)
        blocker = ex.submit(sleep_ident, 0.5)
        queued = ex.submit(square, 3)
        ex.shutdown(wait=True, cancel_futures=True)
        # the pool exposes no queued-versus-running signal: every
        # outstanding future is cancelled (a running task completes but
        # its result is discarded)
        assert blocker.cancelled()
        assert queued.cancelled()
    finally:
        p.stop()


def test_create_owns_pool():
    ex = pymizu.PoolExecutor.create(2)
    assert ex.submit(square, 4).result(timeout=5) == 16
    pool = ex._pool
    ex.shutdown()
    with pytest.raises(pymizu.MizuError, match="closed"):
        pool.submit(square, 2)


def test_wrapped_pool_outlives_executor(pool):
    ex = pymizu.PoolExecutor(pool)
    assert ex.submit(square, 2).result(timeout=5) == 4
    ex.shutdown()
    assert pool.submit(square, 5).collect(timeout=5) == 25


def test_asyncio_wrap_future(pool):
    async def main():
        return await asyncio.wrap_future(ex.submit(square, 7))

    with pymizu.PoolExecutor(pool) as ex:
        assert asyncio.run(main()) == 49


def test_submit_terminal_skips_the_drain_pass(pool, monkeypatch):
    # a task terminal at submit resolves from the re-check alone: no
    # O(outstanding) drain pass per submission (the seam counts main
    # thread passes only — the listener's wake drain is the other regime)
    from pymizu import _pymizu

    main = threading.current_thread()
    with pymizu.PoolExecutor(pool) as ex:
        drains = []
        orig_drain = ex._drain

        def counting_drain():
            if threading.current_thread() is main:
                drains.append(1)
            else:
                orig_drain()

        def terminal_submit(fn, args, kwargs):
            task, _ = ex._pool_h._exec_submit(
                _pymizu._task_frame(fn, args, kwargs), None
            )
            deadline = time.monotonic() + 5
            while not ex._scan([task])[0]:
                assert time.monotonic() < deadline
                time.sleep(0.001)
            return task, True

        monkeypatch.setattr(ex, "_drain", counting_drain)
        monkeypatch.setattr(ex, "_exec_submit", terminal_submit)
        fut = ex.submit(square, 6)
        assert fut.result(timeout=5) == 36
        assert drains == []


def test_submit_recheck_losing_to_fail_all(pool, monkeypatch):
    # a fail-all landing between the insert and the re-check: the
    # submit's resolve skips the already-failed future (the pop finds
    # nothing) rather than resolving it a second time
    from pymizu import _pymizu

    err = pymizu.MizuError("infrastructure failure")

    def pending_submit(fn, args, kwargs):
        task, _ = ex._pool_h._exec_submit(
            _pymizu._task_frame(fn, args, kwargs), None
        )
        return task, None

    def failing_scan(tasks):
        ex._fail_all(err)
        return [True] * len(tasks)

    with pymizu.PoolExecutor(pool) as ex:
        monkeypatch.setattr(ex, "_exec_submit", pending_submit)
        monkeypatch.setattr(ex, "_scan", failing_scan)
        fut = ex.submit(square, 2)
        with pytest.raises(pymizu.MizuError) as exc_info:
            fut.result(timeout=5)
        assert exc_info.value is err


def test_done_callbacks_run_on_listener_thread(pool):
    # a task completing after submit resolves off the wake: its done
    # callbacks run on the listener thread (the stdlib management-thread
    # discipline), never on the submitting one
    main = threading.current_thread()
    seen = []
    with pymizu.PoolExecutor(pool) as ex:
        fut = ex.submit(sleep_ident, 0.3)
        fut.add_done_callback(
            lambda f: seen.append(threading.current_thread())
        )
        assert fut.result(timeout=5) == 0.3
    assert seen and seen[0] is not main


def test_drain_reentry_skips(pool, monkeypatch):
    # a drain already running holds the lock: a concurrent call returns
    # at once, covered by the running drain's re-check pass
    ex = pymizu.PoolExecutor(pool)
    fut = ex.submit(sleep_ident, 0.5)
    orig_collect = ex._collect_terminal
    entered = threading.Event()
    release = threading.Event()

    def blocking_collect(tasks):
        entered.set()
        assert release.wait(5)
        return orig_collect(tasks)

    monkeypatch.setattr(ex, "_collect_terminal", blocking_collect)
    t = threading.Thread(target=ex._drain, daemon=True)
    t.start()
    assert entered.wait(5)
    ex._drain()  # skips: the running drain covers this pass
    release.set()
    t.join(timeout=5)
    assert not t.is_alive()
    assert fut.result(timeout=5) == 0.5
    ex.shutdown()


def test_drain_skips_future_cancelled_mid_scan(pool, monkeypatch):
    # a future cancelled between the drain's snapshot and its resolve is
    # dropped, not resolved
    ex = pymizu.PoolExecutor(pool)
    f0 = ex.submit(sleep_ident, 0.3)
    f1 = ex.submit(sleep_ident, 0.6)

    def cancelling_collect(tasks):
        f1.cancel()  # detaches from the outstanding set mid-drain
        return [
            (i, None, False)
            for i, task in enumerate(tasks)
            if task is f1._task
        ]

    monkeypatch.setattr(ex, "_collect_terminal", cancelling_collect)
    ex._drain()
    assert f1.cancelled()
    monkeypatch.undo()
    assert f0.result(timeout=5) == 0.3
    ex.shutdown()


def test_cancel_detaching_after_pool_destroy():
    # cancelling after the pool is gone: the announce withdrawal fails
    # into the pool's tombstone, tolerated
    p = pymizu.Pool.create(1)
    try:
        ex = pymizu.PoolExecutor(p)
        fut = ex.submit(sleep_ident, 0.5)
        p._h.destroy()  # bypass the facade hooks: the announce dies with it
        assert fut.cancel()
        assert fut.cancelled()
        ex.shutdown(wait=False)
    finally:
        p.destroy()


def test_done_callback_destroying_pool_defers():
    # a teardown landing inside the drain (done callbacks run on the
    # listener thread) hands off to a detached thread rather than
    # deadlocking the watch stop on the watched callback
    p = pymizu.Pool.create(1)
    try:
        ex = pymizu.PoolExecutor(p)
        destroyed = threading.Event()

        def on_done(f):
            p.destroy()
            destroyed.set()

        fut = ex.submit(square, 3)
        fut.add_done_callback(on_done)
        assert fut.result(timeout=5) == 9
        assert destroyed.wait(5)
        ex.shutdown(wait=False)
    finally:
        p.destroy()


def test_no_drain_thread(pool):
    # futures resolve off the notify watch — the executor spawns no
    # thread of its own
    before = {t.name for t in threading.enumerate()}
    with pymizu.PoolExecutor(pool) as ex:
        futs = [ex.submit(square, i) for i in range(8)]
        assert {f.result(timeout=10) for f in futs} == {
            i * i for i in range(8)
        }
        after = {t.name for t in threading.enumerate()}
    assert not any("executor" in n for n in after - before)


def test_stop_racing_a_listener_drain_no_deadlock(pool):
    # a direct-mode drain in flight when stop() runs must not park on
    # the stop gate: the core's watch stop waits the callback out while
    # the callback would wait out the gate — the gate answers the
    # callback the closed handle instead
    entered = threading.Event()
    release = threading.Event()

    def drain():
        entered.set()
        assert release.wait(5)
        with pytest.raises(ValueError, match="pool handle is closed"):
            pool._h._notify_rearm()

    pool._h._notify_arm(None, drain)
    pool._h._task_notify([pool.submit(sleep_ident, 0.5)], True)
    assert entered.wait(5)
    stopper = threading.Thread(target=pool.stop)
    stopper.start()
    time.sleep(0.2)  # the stop raises the gate word, then enters the core
    release.set()
    stopper.join(10)
    assert not stopper.is_alive()
