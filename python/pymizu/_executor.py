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
# long for the next snapshot. 50ms keeps late submissions prompt; an idle
# re-park costs one syscall.
_COLLECT_BOUND = 0.05


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
    """A :class:`concurrent.futures.Executor` over a pymizu pool.

    ``pool`` is the pool to wrap; its lifetime stays with its owner
    unless ``stop_pool`` is set — then :meth:`shutdown` stops it.
    :meth:`create` spawns an owned pool instead. :meth:`submit` returns a
    real :class:`concurrent.futures.Future`: ``wait`` / ``as_completed``
    / ``result(timeout)`` / ``asyncio.wrap_future`` and the stdlib base
    ``Executor.map`` all work unchanged, so code written against
    ``ProcessPoolExecutor`` drops in.

    Deltas from stdlib semantics: a task's failure surfaces as
    :class:`pymizu.TaskError` (the remote error envelope);
    ``Future.running()`` is always False (futures resolve at completion);
    cancellation is advisory, mirroring :meth:`pymizu.Task.cancel` — and
    ``shutdown(cancel_futures=True)`` cancels every outstanding future,
    since the pool exposes no queued-versus-running signal (a running
    task completes, but its result is discarded). ``map`` is the stdlib
    base implementation over ``submit`` — for bulk maps,
    :meth:`pymizu.Pool.map` is the faster path.
    """

    def __init__(self, pool: pymizu.Pool, *, stop_pool: bool = False) -> None:
        super().__init__()
        self._pool = pool
        self._stop_pool = stop_pool
        self._cond = _threading.Condition()
        self._outstanding: dict[_TaskFuture, _pymizu._Task] = {}
        self._drain: _threading.Thread | None = None
        self._shutdown = False

    @classmethod
    def create(cls, workers: int = 1, **pool_kwargs: _Any) -> PoolExecutor:
        """Spawn an owned pool — ``Pool.create(workers, **pool_kwargs)``
        — and wrap it; :meth:`shutdown` stops it."""
        import pymizu

        return cls(pymizu.Pool.create(workers, **pool_kwargs), stop_pool=True)

    def submit(
        self, fn: _Callable[..., _Any], /, *args: _Any, **kwargs: _Any
    ) -> _Future:
        """Schedule ``fn(*args, **kwargs)``; return a Future.
        RuntimeError after shutdown."""
        with self._cond:
            if self._shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after shutdown"
                )
            task = self._pool.submit(fn, *args, **kwargs)
            future = _TaskFuture(task)
            self._outstanding[future] = task
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
        """Stdlib shutdown: ``wait`` waits for the outstanding futures;
        ``cancel_futures`` cancels the not-yet-started ones. Stops the
        pool only when ``stop_pool`` was set."""
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
        resolving futures in completion order. The bounded collect bounds
        how long a submission made mid-collect waits for its snapshot;
        _COLLECT_BOUND keeps that wait short."""
        while True:
            with self._cond:
                while not self._outstanding and not self._shutdown:
                    self._cond.wait()
                if not self._outstanding:
                    return
                pairs = list(self._outstanding.items())
            tasks = [task for _, task in pairs]
            try:
                got = self._pool.collect_any(tasks, timeout=_COLLECT_BOUND)
            except Exception as e:
                index = getattr(e, "index", None)
                if index is None:
                    # infrastructure failure: fail everything outstanding
                    self._fail_all(e)
                    return
                self._resolve(pairs[index][0], exception=e)
                continue
            if isinstance(got, _pymizu.Sentinel):  # TIMEOUT: re-snapshot
                continue
            index, value = got
            self._resolve(pairs[index][0], result=value)

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
        error (e.g. the pool stopped from under the executor)."""
        with self._cond:
            futures = list(self._outstanding)
            self._outstanding.clear()
        for future in futures:
            if future.set_running_or_notify_cancel():
                future.set_exception(exc)
