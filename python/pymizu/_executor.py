"""concurrent.futures.Executor adapter: stdlib drop-in interop over a Pool.

PoolExecutor maps submit/shutdown onto a pymizu Pool and resolves real
concurrent.futures.Future objects, so wait / as_completed / wrap_future
and the stdlib base Executor.map (over submit) work unchanged. There is
no drain thread: the pool's notify endpoint wakes a scan (the core's
edge-triggered signal rides the death-listener machinery), and
concurrent.futures.Future.set_result — thread-safe by design, it is how
executor workers resolve — resolves futures directly in the listener
callback. Cancellation is advisory, mirroring Task.cancel.
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


class _TaskFuture(_Future):
    """A Future carrying its pymizu Task: cancel() cancels the task too,
    and detaches it from the executor's outstanding set at once (its
    slot drains when the worker's skip frees it — no publish, no wake)."""

    def __init__(self, task: _pymizu._Task, on_cancel: _Any = None) -> None:
        super().__init__()
        self._task = task
        self._on_cancel = on_cancel

    def cancel(self) -> bool:
        if not super().cancel():
            return False
        self._task.cancel()
        if self._on_cancel is not None:
            self._on_cancel(self)
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
    `submit()` (and the stdlib `map` over it) is thread-safe, as the
    `concurrent.futures` contract requires: an internal lock
    serializes the single-producer pool handle. This is the adapter
    to reach for when several threads must submit to one pool.
    Deltas from stdlib semantics: a task's failure surfaces as
    `pymizu.TaskError` (the remote error envelope);
    `Future.running()` is always False (futures resolve at completion);
    cancellation is advisory, mirroring [`Task.cancel()`](`pymizu.Task`)
    — and `shutdown(cancel_futures=True)` cancels every outstanding
    future, since the pool exposes no queued-versus-running signal (a
    running task completes, but its result is discarded). An
    infrastructure failure (e.g. the wrapped pool is stopped from under
    the executor) fails every outstanding future; later submissions
    raise instead of queueing onto a broken executor.
    Resolution — including any `add_done_callback` callables — runs on
    the core's listener thread, the same discipline as the stdlib
    executors' management thread: keep callbacks short, and never block
    one on the pool itself.
    `map` is the stdlib base implementation over `submit` — for bulk
    maps, [`Pool.map`](`pymizu.Pool.map`) is the faster path.

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
        # one lock serializing the core submits: a pool handle's
        # injection ring is single-producer, so concurrent submitters
        # serialize (the drain never touches it — the scan's atomic
        # loads and the terminal collects need no lock)
        self._submits = _threading.Lock()
        # serializes overlapping drains (the listener callback and the
        # submit-time re-check): the skipped one is covered by the
        # running one's re-check pass
        self._draining = _threading.Lock()
        self._outstanding: dict[_TaskFuture, _pymizu._Task] = {}
        self._armed = False
        self._shutdown = False
        self._broken: Exception | None = None
        # a pool destroy publishes nothing: the facade's hook fails the
        # outstanding futures (its own teardown never wakes the drain)
        hooks = getattr(pool, "_destroy_hooks", None)
        if hooks is None:
            hooks = pool._destroy_hooks = []
        hooks.append(self._on_pool_destroy)

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
            drain failed every outstanding future on it).
        """
        with self._cond:
            if self._shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after shutdown"
                )
            if self._broken is not None:
                raise self._broken
            with self._submits:
                task = self._pool.submit(fn, *args, **kwargs)
            future = _TaskFuture(task, self._detach)
            self._outstanding[future] = task
            if not self._armed:
                # a NULL loop: the drain runs directly on the listener
                # thread (set_result is thread-safe by design)
                self._pool._h._notify_arm(None, self._drain)
                self._armed = True
            # the per-slot NOTIFY announce, then the re-check: a slot
            # already terminal at this point resolves from the scan
            # (announce before arming is the parker transplant)
            self._pool._h._task_notify([task], True)
        # the re-check covers a publish that beat the announce: scan the
        # new task only (a full-drain per submit costs O(outstanding) per
        # submission); a publish past the announce rides the armed word's
        # wake instead
        if self._scan([task])[0]:
            self._drain()
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
            futures = list(self._outstanding) if cancel_futures else []
        # outside the condition: cancel() detaches through it
        for future in futures:
            future.cancel()
        if wait:
            with self._cond:
                while self._outstanding:
                    self._cond.wait()
        self._disarm()
        hooks = getattr(self._pool, "_destroy_hooks", None)
        if hooks is not None and self._on_pool_destroy in hooks:
            hooks.remove(self._on_pool_destroy)
        if self._stop_pool:
            self._pool.stop()

    def _on_pool_destroy(self) -> None:
        self._fail_all(ValueError("pymizu: pool handle is closed"))

    def _detach(self, future: _TaskFuture) -> None:
        with self._cond:
            task = self._outstanding.pop(future, None)
            self._cond.notify_all()
        if task is not None:
            try:
                self._pool._h._task_notify([task], False)
            except Exception:
                pass  # a destroyed pool: the announce dies with it

    def _disarm(self) -> None:
        if self._armed:
            self._armed = False
            self._pool._h._notify_disarm()

    def _scan(self, tasks: list) -> list:
        """The drain's terminal scan (one method for tests to seam)."""
        return self._pool._h._tasks_terminal(tasks)

    def _drain(self) -> None:
        """One drain pass (the listener callback and the submit-time
        re-check): re-arm the submitter word, scan the outstanding set,
        resolve what turned terminal — looping until a pass finds
        nothing new, so a publish racing the pass rides the re-armed
        word's next signal (the announce/recheck transplant)."""
        if not self._draining.acquire(blocking=False):
            return  # the running drain's re-check covers this call
        try:
            while True:
                try:
                    self._pool._h._notify_rearm()
                except Exception:
                    # the pool was destroyed mid-drain (a done callback
                    # tearing it down): the facade's hook already failed
                    # every outstanding future — nothing left to scan
                    return
                with self._cond:
                    items = list(self._outstanding.items())
                if not items:
                    self._disarm()
                    return
                try:
                    terminal = self._scan([t for _, t in items])
                except Exception as e:
                    # infrastructure failure: fail everything outstanding
                    self._fail_all(e)
                    return
                progressed = False
                for (future, task), term in zip(items, terminal, strict=False):
                    if not term:
                        continue
                    with self._cond:
                        if self._outstanding.pop(future, None) is None:
                            continue
                    try:
                        value = task.collect(0)
                    except Exception as e:
                        self._resolve(future, exception=e)
                    else:
                        self._resolve(future, result=value)
                    progressed = True
                if progressed:
                    # one shutdown-waiter wake per pass, not per resolve
                    with self._cond:
                        self._cond.notify_all()
                else:
                    return
        finally:
            self._draining.release()

    def _resolve(
        self,
        future: _TaskFuture,
        *,
        result: _Any = _MISSING,
        exception: _Any = _MISSING,
    ) -> None:
        """Retire one task's future — set_result/set_exception outside
        the condition, since they run done callbacks that may re-enter.
        A cancelled future's outcome is discarded. The shutdown-waiter
        wake is the drain's, batched per pass."""
        if not future.set_running_or_notify_cancel():
            return
        if exception is _MISSING:
            future.set_result(result)
        else:
            future.set_exception(exception)

    def _fail_all(self, exc: Exception) -> None:
        """Fail every outstanding future with the same infrastructure
        error and break the executor: a later submission raises the
        error instead of queueing onto a broken executor."""
        with self._cond:
            self._broken = exc
            futures = list(self._outstanding)
            self._outstanding.clear()
            self._cond.notify_all()
        self._disarm()
        for future in futures:
            if future.set_running_or_notify_cancel():
                future.set_exception(exc)
