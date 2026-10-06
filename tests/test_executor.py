"""PoolExecutor tests: the concurrent.futures.Executor adapter — submit,
map, wait/as_completed, cancellation, and shutdown semantics."""

import asyncio
import concurrent.futures
import operator

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
        futs = [ex.submit(sleep_ident, 0.5), ex.submit(square, 3)]
        first = next(concurrent.futures.as_completed(futs, timeout=5))
        assert first.result() == 9


def test_future_timeout(pool):
    with pymizu.PoolExecutor(pool) as ex:
        fut = ex.submit(sleep_ident, 0.5)
        with pytest.raises(TimeoutError):
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
