"""concurrent.futures.Executor adapter: stdlib drop-in interop over a Pool.

PoolExecutor maps submit/shutdown onto a pymizu Pool and resolves real
concurrent.futures.Future objects, so wait / as_completed / wrap_future
and the stdlib base Executor.map (over submit) work unchanged. A single
daemon drain thread retires completed tasks through collect_any and
resolves their futures; cancellation is advisory, mirroring Task.cancel.
"""

from __future__ import annotations

import threading as _threading
from collections.abc import Callable as _Callable
from concurrent.futures import Executor as _Executor
from concurrent.futures import Future as _Future
from typing import TYPE_CHECKING
from typing import Any as _Any

from pymizu import _pymizu

if TYPE_CHECKING:
    import pymizu

_MISSING = object()

# Drain collect bound: a submission made mid-collect waits at most this
# long for the next snapshot — and, the verbs lock serializing the core
# handle, for the drain's collect to release the lock. 50ms keeps late
# submissions prompt; an idle re-park costs one syscall.
_COLLECT_BOUND = 0.05

# Burst bound: completions resolved back-to-back before the drain loops
# back to merge _pending into the snapshot. The verbs lock is per-collect,
# so a submit's lock wait is one scan at most regardless; the bound caps
# how long a submission made mid-flood waits to join the collect set.
_BURST_MAX = 256


def _swap_remove(futures: list, tasks: list, index: int) -> None:
    """O(1) removal from the drain's parallel snapshot lists (order is
    insignificant to collect_any)."""
    last = len(tasks) - 1
    futures[index] = futures[last]
    tasks[index] = tasks[last]
    futures.pop()
    tasks.pop()


class _TaskFuture(_Future):
    """A Future carrying its pymizu Task: cancel() cancels the task too."""

    def __init__(self, task: _pymizu._Task) -> None:
        super().__init__()
        self._task = task

    def cancel(self) -> bool:
        if not super().cancel():
            return False
        self._task.cancel()
        return True


class PoolExecutor(_Executor):
    """A `concurrent.futures.Executor` over a pymizu pool.

    `submit()` returns a real `concurrent.futures.Future`: `wait` /
    `as_completed` / `result(timeout)` / `asyncio.wrap_future` and the
    stdlib base `Executor.map` all work unchanged, so code written
    against `ProcessPoolExecutor` drops in. `create()` spawns an owned
    pool instead.

    Parameters
    ----------
    pool
        The pool to wrap; its lifetime stays with its owner unless
        `stop_pool` is set — then `shutdown()` stops it.
    stop_pool
        When True, `shutdown()` stops the wrapped pool.

    Notes
    -----
    Deltas from stdlib semantics: a task's failure surfaces as
    `pymizu.TaskError` (the remote error envelope);
    `Future.running()` is always False (futures resolve at completion);
    cancellation is advisory, mirroring [`Task.cancel()`](`pymizu.Task`)
    — and `shutdown(cancel_futures=True)` cancels every outstanding
    future, since the pool exposes no queued-versus-running signal (a
    running task completes, but its result is discarded). An
    infrastructure failure (e.g. the wrapped pool is stopped from under
    the executor) fails every outstanding future; later submissions
    raise instead of queueing onto a dead drain.
    `map` is the stdlib base implementation over `submit` — for bulk
    maps, `Pool.map` is the faster path.

    Examples
    --------
    Use as a drop-in `concurrent.futures.Executor`:

    ```{python}
    import pymizu

    with pymizu.PoolExecutor.create(2) as ex:
        futures = [ex.submit(pow, 2, i) for i in range(4)]
        results = [f.result() for f in futures]
    results
    ```
    """

    def __init__(self, pool: pymizu.Pool, *, stop_pool: bool = False) -> None:
        super().__init__()
        self._pool = pool
        self._stop_pool = stop_pool
        self._cond = _threading.Condition()
        # one lock serializing the core verbs: a pool handle is
        # single-threaded per side, so the caller thread's submit and the
        # drain thread's collect_any must never run concurrently
        self._verbs = _threading.Lock()
        self._outstanding: dict[_TaskFuture, _pymizu._Task] = {}
        self._pending: list[tuple[_TaskFuture, _pymizu._Task]] = []
        self._drain: _threading.Thread | None = None
        self._shutdown = False
        self._broken: Exception | None = None

    @classmethod
    def create(cls, workers: int = 1, **pool_kwargs: _Any) -> PoolExecutor:
        """Spawn an owned pool —
        [`Pool.create()`](`pymizu.Pool.create`) with `workers` and
        `pool_kwargs` — and wrap it; `shutdown()` stops it.

        Parameters
        ----------
        workers
            Number of worker processes for the pool.
        pool_kwargs
            Forwarded to [`Pool.create()`](`pymizu.Pool.create`).

        Returns
        -------
            A PoolExecutor wrapping the spawned pool.
        """
        import pymizu

        return cls(pymizu.Pool.create(workers, **pool_kwargs), stop_pool=True)

    def submit(
        self, fn: _Callable[..., _Any], /, *args: _Any, **kwargs: _Any
    ) -> _Future:
        """Schedule `fn(*args, **kwargs)` on the pool.

        Parameters
        ----------
        fn
            The callable to run.
        args
            Positional arguments for `fn`.
        kwargs
            Keyword arguments for `fn`.

        Returns
        -------
            A `concurrent.futures.Future` resolving with the result.

        Raises
        ------
        RuntimeError
            After shutdown.
        pymizu.MizuError
            The infrastructure failure that broke the executor (its
            drain thread failed every outstanding future on it).
        """
        with self._cond:
            if self._shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after shutdown"
                )
            if self._broken is not None:
                raise self._broken
            with self._verbs:
                task = self._pool.submit(fn, *args, **kwargs)
            future = _TaskFuture(task)
            self._outstanding[future] = task
            self._pending.append((future, task))
            if self._drain is None:
                self._drain = _threading.Thread(
                    target=self._drain_loop,
                    name="pymizu-executor-drain",
                    daemon=True,
                )
                self._drain.start()
            else:
                self._cond.notify()
            return future

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        """Stdlib shutdown.

        Parameters
        ----------
        wait
            Wait for the outstanding futures.
        cancel_futures
            Cancel the not-yet-started futures. Stops the pool only
            when `stop_pool` was set.
        """
        with self._cond:
            self._shutdown = True
            if cancel_futures:
                for future in list(self._outstanding):
                    future.cancel()
            self._cond.notify_all()
        if wait and self._drain is not None:
            self._drain.join()
        if self._stop_pool:
            self._pool.stop()

    def _drain_loop(self) -> None:
        """The one drain thread: collect_any over the outstanding tasks,
        resolving futures in completion order.

        The task list rides in a persistent snapshot — submissions append
        to _pending and merge in bulk, resolves swap-remove in O(1) — so
        a burst of n completions costs O(n) collects, never O(n^2) list
        churn. The verbs lock is taken per collect, so a submit's lock
        wait is one scan at most; _BURST_MAX instead bounds how many
        completions resolve before _pending merges, keeping a
        submission's wait to join the collect set short under a flood.
        The first collect of a burst waits up to _COLLECT_BOUND, so a
        submission made mid-collect joins the snapshot promptly and an
        idle re-park costs one syscall."""
        snap_futures: list[_TaskFuture] = []
        snap_tasks: list[_pymizu._Task] = []
        while True:
            with self._cond:
                while not self._outstanding and not self._shutdown:
                    self._cond.wait()
                if not self._outstanding:
                    return
                if self._pending:
                    snap_futures.extend(f for f, _ in self._pending)
                    snap_tasks.extend(t for _, t in self._pending)
                    self._pending.clear()
            timeout = _COLLECT_BOUND
            for _ in range(_BURST_MAX):
                if not snap_tasks:
                    break
                try:
                    with self._verbs:
                        got = self._pool.collect_any(
                            snap_tasks, timeout=timeout
                        )
                except Exception as e:
                    index = getattr(e, "index", None)
                    if index is not None:
                        future = snap_futures[index]
                        _swap_remove(snap_futures, snap_tasks, index)
                        self._resolve(future, exception=e)
                        timeout = 0.0
                        continue
                    # a cancelled queued task is freed by the worker's
                    # CANCEL->FREE skip, so a stale handle in the snapshot
                    # fails collect_any's claim with a no-index "already
                    # collected" error: drop the handles now reading
                    # "collected" (the failed claim consumed nothing)
                    # rather than failing every outstanding future
                    stale = [
                        i
                        for i, t in enumerate(snap_tasks)
                        if t.state == "collected"
                    ]
                    if stale:
                        for i in reversed(stale):
                            future = snap_futures[i]
                            _swap_remove(snap_futures, snap_tasks, i)
                            self._resolve(future, exception=e)
                        timeout = 0.0
                        continue
                    # infrastructure failure: fail everything outstanding
                    self._fail_all(e)
                    return
                if isinstance(got, _pymizu.Sentinel):  # TIMEOUT: burst over
                    break
                index, value = got
                future = snap_futures[index]
                _swap_remove(snap_futures, snap_tasks, index)
                self._resolve(future, result=value)
                timeout = 0.0

    def _resolve(
        self,
        future: _TaskFuture,
        *,
        result: _Any = _MISSING,
        exception: _Any = _MISSING,
    ) -> None:
        """Retire one task: pop it, then resolve its Future — outside the
        condition, since set_result runs done callbacks that may
        re-enter. A cancelled future's outcome is discarded."""
        with self._cond:
            if self._outstanding.pop(future, None) is None:
                return
        if not future.set_running_or_notify_cancel():
            return
        if exception is _MISSING:
            future.set_result(result)
        else:
            future.set_exception(exception)

    def _fail_all(self, exc: Exception) -> None:
        """Fail every outstanding future with the same infrastructure
        error and break the executor: the drain thread exits on this
        failure, so a later submission raises the error instead of
        queueing onto a dead drain."""
        with self._cond:
            self._broken = exc
            futures = list(self._outstanding)
            self._outstanding.clear()
            self._pending.clear()
        for future in futures:
            if future.set_running_or_notify_cancel():
                future.set_exception(exc)
