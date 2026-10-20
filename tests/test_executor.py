"""PoolExecutor tests: the concurrent.futures.Executor adapter — submit,
map, wait/as_completed, cancellation, and shutdown semantics."""

import asyncio
import concurrent.futures
import operator
import threading
import time

import pytest
from tests.helpers import fail_at, sleep_ident, square

import pymizu
from pymizu._executor import _BURST_MAX


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
        # the late submit overtakes: the drain's short collect bound
        # re-snapshots well before the 0.5s sleep completes
        futs = [ex.submit(sleep_ident, 0.5), ex.submit(square, 3)]
        first = next(concurrent.futures.as_completed(futs, timeout=5))
        assert first.result() == 9


def test_burst_rollover(pool):
    # more completions than the drain's burst bound: bursts roll over and
    # submissions merge across them, resolving every future
    n = _BURST_MAX + 44
    with pymizu.PoolExecutor(pool) as ex:
        futs = [ex.submit(square, i) for i in range(n)]
        assert {f.result(timeout=10) for f in futs} == {
            i * i for i in range(n)
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
    # a cancelled queued task is freed by the worker's skip, so the drain
    # thread's collect_any claims a stale handle and fails with a no-index
    # "already collected" error: the drain drops the collected handles and
    # re-snapshots rather than failing every outstanding future (P-03)
    p = pymizu.Pool.create(2)
    try:
        ex = pymizu.PoolExecutor(p)
        f0 = ex.submit(sleep_ident, 0.6)  # worker A busy; drain parks on f0
        time.sleep(0.1)
        f1 = ex.submit(square, 1)  # claimed by worker B at once
        assert f1.cancel()  # B's skip frees it CANCEL->FREE
        deadline = time.monotonic() + 5
        while f1._task.state != "collected" and time.monotonic() < deadline:
            time.sleep(0.01)
        # let the drain's next snapshot claim the stale handle
        time.sleep(0.2)
        f2 = ex.submit(square, 2)
        assert f0.result(timeout=5) == 0.6
        assert f2.result(timeout=5) == 4
        assert f1.cancelled()
        assert ex._drain.is_alive()
        ex.shutdown(wait=False)
    finally:
        p.stop()


def test_stale_handle_recovers(pool):
    # a task collected outside the drain frees its slot: the drain's next
    # collect_any claims a stale handle and fails with a no-index
    # "already collected" error — the drain drops the stale futures and
    # re-snapshots rather than failing everything outstanding (P-03). The
    # done callback parks the drain outside its collect so the outside
    # collect wins the slot deterministically.
    ex = pymizu.PoolExecutor(pool)
    entered = threading.Event()

    def park_drain(_):
        entered.set()
        time.sleep(1)

    f0 = ex.submit(sleep_ident, 0.3)
    f0.add_done_callback(park_drain)
    f1 = ex.submit(sleep_ident, 0.6)
    assert entered.wait(timeout=5)
    # collected outside the drain while it is parked in the callback
    assert f1._task.collect(timeout=5) == 0.6
    assert f0.result(timeout=5) == 0.3
    with pytest.raises(pymizu.MizuError, match="already collected"):
        f1.result(timeout=5)
    ex.shutdown()


def test_broken_executor_fails_and_blocks_submit(pool, monkeypatch):
    # the drain's collect fails with an infrastructure error not
    # attributable to a handle: every outstanding future fails with it
    # and later submissions raise it instead of queueing onto the dead
    # drain thread
    err = pymizu.MizuError("infrastructure failure")

    def fail_collect(tasks, timeout=None):
        raise err

    with pymizu.PoolExecutor(pool) as ex:
        fut = ex.submit(sleep_ident, 0.5)
        monkeypatch.setattr(ex._pool, "collect_any", fail_collect)
        with pytest.raises(pymizu.MizuError) as exc_info:
            fut.result(timeout=5)
        assert exc_info.value is err
        with pytest.raises(pymizu.MizuError) as exc_info:
            ex.submit(square, 2)
        assert exc_info.value is err


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
