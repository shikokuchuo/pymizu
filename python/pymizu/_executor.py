"""concurrent.futures.Executor adapter: stdlib drop-in interop over a Pool.

PoolExecutor maps submit/shutdown onto a pymizu Pool and resolves real
concurrent.futures.Future objects, so wait / as_completed / wrap_future
and a ProcessPoolExecutor-style chunked map work unchanged. There is
no drain thread: the pool's notify endpoint wakes a scan (the core's
edge-triggered signal rides the death-listener machinery), and
concurrent.futures.Future.set_result — thread-safe by design, it is how
executor workers resolve — resolves futures directly in the listener
callback. Cancellation is advisory, mirroring Task.cancel.
"""

from __future__ import annotations

import operator as _operator
import threading as _threading
import time as _time
import weakref as _weakref
from collections import deque as _deque
from collections.abc import Callable as _Callable
from concurrent.futures import Executor as _Executor
from concurrent.futures import Future as _Future
from functools import partial as _partial
from itertools import islice as _islice
from typing import TYPE_CHECKING
from typing import Any as _Any
from typing import Self as _Self

import pymizu as _pkg
from pymizu import _pymizu

if TYPE_CHECKING:
    import pymizu

_MISSING = object()
_SLOT_RETRY_SECONDS = 0.01


def _process_chunk(fn, chunk):
    return [fn(*args) for args in chunk]


def _batched(iterable, n):
    it = iter(iterable)
    while batch := tuple(_islice(it, n)):
        yield batch


def _result_or_cancel(fut, timeout=None):
    try:
        try:
            return fut.result(timeout)
        finally:
            fut.cancel()
    finally:
        del fut


def _chain_from_iterable_of_lists(iterable):
    # unlike chain.from_iterable, do not retain each yielded result
    for element in iterable:
        element.reverse()
        while element:
            yield element.pop()


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
    `as_completed` / `result(timeout)` / `asyncio.wrap_future` and `map`
    with `ProcessPoolExecutor` chunking all work unchanged, so code written
    against `ProcessPoolExecutor` drops in. `create()` spawns an owned
    pool instead.

    Parameters
    ----------
    pool
        The pool to wrap. Its lifetime stays with its owner unless `stop_pool`
        is set, in which case `shutdown()` stops it.
    stop_pool
        When True, `shutdown()` stops the wrapped pool.

    Notes
    -----
    `submit()` (and the stdlib `map` over it) is thread-safe, as the
    `concurrent.futures` contract requires: an internal lock serializes the
    single-producer pool handle. Use this adapter when several threads must
    submit to one pool. Differences from the stdlib: a task's failure surfaces
    as `pymizu.TaskError`; `Future.running()` is always False (futures resolve
    at completion); cancellation works like [`Task.cancel()`](`pymizu.Task`)
    and never interrupts a running task; and `shutdown(cancel_futures=True)`
    cancels every outstanding future, since the pool exposes no
    queued-versus-running signal (a running task completes, but its result is
    discarded). An internal failure (for example the wrapped pool is stopped
    from under the executor) fails every outstanding future, and later
    submissions raise instead of queueing onto a broken executor. Futures
    resolve, and `add_done_callback` callbacks run, on pymizu's listener
    thread, the same discipline as the stdlib executors' management thread:
    keep callbacks short, and never block one on the pool itself. If the
    result table is full, `submit()` waits for the executor to collect a
    result; a submission from a listener-thread done callback instead raises
    `SlotsExhaustedError`, since waiting there would deadlock the drain.
    `map()` honors `chunksize` and `buffersize` like `ProcessPoolExecutor`;
    for bulk maps, [`Pool.map`](`pymizu.Pool.map`) remains the faster path.

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
        # the raw handle, stashed: the submit/drain verbs skip the
        # facade's attribute walk
        self._pool_h = pool._h
        self._stop_pool = stop_pool
        # the one lock: serializes the core submits (a pool handle's
        # injection ring is single-producer) and guards the outstanding
        # set. The drain never submits — its scan's atomic loads and
        # terminal collects need no lock: the binding declares the split
        # waiter, so a collect hands its releases to the submitting
        # thread instead of touching the free list
        self._cond = _threading.Condition()
        # serializes overlapping drains (the listener callback and the
        # submit-time re-check): the skipped one is covered by the
        # running one's re-check pass
        self._draining = _threading.Lock()
        self._drain_state = _threading.local()
        self._outstanding: dict[_TaskFuture, _pymizu._Task] = {}
        self._armed = False
        self._shutdown = False
        self._broken: Exception | None = None
        # the native-worker verdict, cached once known: the pool's worker
        # word never resets once set, and a None (no worker yet) re-reads
        self._native = False
        # a pool destroy publishes nothing: the facade's hook fails the
        # outstanding futures (its own teardown never wakes the drain)
        hooks = getattr(pool, "_destroy_hooks", None)
        if hooks is None:
            hooks = pool._destroy_hooks = []
        hooks.append(self._on_pool_destroy)

    @classmethod
    def create(cls, n_workers: int = 1, **pool_kwargs: _Any) -> _Self:
        """Spawn an owned pool and wrap it.

        Calls [`Pool.create()`](`pymizu.Pool.create`) with `n_workers` and
        `pool_kwargs`; `shutdown()` stops the pool.

        Parameters
        ----------
        n_workers
            Number of worker processes for the pool.
        pool_kwargs
            Forwarded to [`Pool.create()`](`pymizu.Pool.create`).

        Returns
        -------
            A PoolExecutor wrapping the spawned pool.
        """
        import pymizu

        return cls(
            pymizu.Pool.create(n_workers, **pool_kwargs), stop_pool=True
        )

    def map(
        self,
        fn: _Callable[..., _Any],
        *iterables: _Any,
        timeout: float | None = None,
        chunksize: int = 1,
        buffersize: int | None = None,
    ) -> _Any:
        """Return an iterator applying `fn` to every zipped input element.

        Parameters
        ----------
        fn
            The callable to run.
        iterables
            One or more iterables, zipped lazily as for the built-in `map`.
        timeout
            One overall deadline for generating the result iterator, in
            seconds; None means no limit.
        chunksize
            Elements per submitted task. Values greater than one use chunk
            tasks; the default submits one task per element.
        buffersize
            Maximum submitted tasks whose results have not been yielded. None
            collects all inputs eagerly; a positive value bounds pending tasks
            and consumes the input lazily.

        Returns
        -------
            An iterator of results in input order. An error in a chunk
            discards that chunk's earlier results, matching
            `ProcessPoolExecutor`.
        """
        if chunksize < 1:
            raise ValueError("chunksize must be >= 1.")
        chunksize = _operator.index(chunksize)
        if buffersize is not None and not isinstance(buffersize, int):
            raise TypeError("buffersize must be an integer or None")
        if buffersize is not None and buffersize < 1:
            raise ValueError("buffersize must be None or > 0")
        if timeout is not None:
            end_time = timeout + _time.monotonic()

        zipped_iterables = zip(*iterables, strict=False)
        chunked = chunksize > 1 and type(fn) is not _pkg.call and callable(fn)
        if chunked:
            zipped_iterables = zip(
                _batched(zipped_iterables, chunksize), strict=False
            )
            submit_fn = _partial(_process_chunk, fn)
        else:
            submit_fn = fn
        fs: _Any
        if buffersize:
            fs = _deque(
                self.submit(submit_fn, *args)
                for args in _islice(zipped_iterables, buffersize)
            )
        else:
            fs = [self.submit(submit_fn, *args) for args in zipped_iterables]

        # do not keep the executor alive through the result closure
        executor_weakref = _weakref.ref(self)

        def result_iterator():
            try:
                fs.reverse()
                while fs:
                    if (
                        buffersize
                        and (executor := executor_weakref())
                        and (args := next(zipped_iterables, None))
                    ):
                        # buffersize selects the deque above
                        # pyrefly: ignore [missing-attribute]
                        fs.appendleft(executor.submit(submit_fn, *args))
                    future = fs.pop()
                    try:
                        if timeout is None:
                            yield _result_or_cancel(future)
                        else:
                            yield _result_or_cancel(
                                future, end_time - _time.monotonic()
                            )
                    finally:
                        del future
            finally:
                for future in fs:
                    future.cancel()

        results = result_iterator()
        if chunked:
            return _chain_from_iterable_of_lists(results)
        return results

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
            The internal failure that broke the executor (every
            outstanding future failed with it).
        """
        with self._cond:
            while True:
                if self._shutdown:
                    raise RuntimeError(
                        "cannot schedule new futures after shutdown"
                    )
                if self._broken is not None:
                    raise self._broken
                if not self._armed:
                    # a NULL loop: the drain runs directly on the listener
                    # thread (set_result is thread-safe by design)
                    self._pool_h._notify_arm(None, self._drain)
                    self._armed = True
                try:
                    if type(fn) is _pkg.call or not callable(fn):
                        # keep the facade's call-spec and callable gates
                        task = self._pool.submit(fn, *args, **kwargs)
                        terminal = None
                    else:
                        if not self._native:
                            ident = self._pool_h._worker_ident()
                            if ident is not None:
                                _pkg._check_native(ident, "Pool.submit")
                                self._native = True
                        # the fused submit + announce + terminal re-check
                        task, terminal = self._exec_submit(fn, args, kwargs)
                except _pkg.SlotsExhaustedError:
                    # only the result table is retried: MIZU_FULL is the
                    # injection ring, whose wait belongs to the core. The
                    # listener must never wait for its own next drain.
                    if getattr(self._drain_state, "active", False):
                        raise
                    self._cond.wait(_SLOT_RETRY_SECONDS)
                    continue
                break
            if type(fn) is _pkg.call or not callable(fn):
                # the per-slot NOTIFY announce (announce before the
                # re-check is the parker transplant), then the scan
                self._pool_h._task_notify([task], True)
            if terminal is None:
                # the facade path's re-check, under the same hold: the
                # terminal verdict is in hand ahead of the insert
                terminal = self._scan([task])[0]
            future = _TaskFuture(task, self._detach)
            if not terminal:
                # the outstanding set is the drain's claim domain: a
                # terminal-at-submit task never enters it, so the inline
                # collect below is its sole executor-side claimant (the
                # core's collect CAS stays the backstop for outside
                # claimants)
                self._outstanding[future] = task
        # never outstanding: no drain pass can claim the slot first
        if terminal:
            try:
                value = task.collect(0)
            except Exception as e:
                with self._cond:
                    self._cond.notify_all()
                self._resolve(future, exception=e)
            else:
                with self._cond:
                    self._cond.notify_all()
                self._resolve(future, result=value)
        return future

    def _exec_submit(
        self, fn: _Callable[..., _Any], args: tuple, kwargs: dict
    ) -> tuple[_pymizu._Task, bool]:
        """The fused submit + announce + terminal re-check (one method
        for tests to seam)."""
        return self._pool_h._exec_submit(
            _pymizu._task_frame(fn, args, kwargs), None
        )

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        """Shut down the executor.

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
            self._cond.notify_all()
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
                self._pool_h._task_notify([task], False)
            except Exception:
                pass  # a destroyed pool: the announce dies with it

    def _disarm(self) -> None:
        if self._armed:
            self._armed = False
            self._pool_h._notify_disarm()

    def _scan(self, tasks: list) -> list:
        """The submit re-check's terminal scan (one method for tests to
        seam)."""
        return self._pool_h._tasks_terminal(tasks)

    def _collect_terminal(self, tasks: list) -> list:
        """The drain's fused scan-and-collect: (index, value-or-error,
        is_exc) per terminal task (one method for tests to seam)."""
        return self._pool_h._tasks_collect_terminal(tasks)

    def _drain(self) -> None:
        """One drain pass (the listener callback): re-arm the submitter
        word, scan the outstanding set, resolve what turned terminal —
        looping until a pass finds nothing new, so a publish racing the
        pass rides the re-armed word's next signal (the announce/recheck
        transplant)."""
        if not self._draining.acquire(blocking=False):
            return  # the running drain's re-check covers this call
        self._drain_state.active = True
        try:
            while True:
                try:
                    self._pool_h._notify_rearm()
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
                    results = self._collect_terminal([t for _, t in items])
                except Exception as e:
                    # infrastructure failure: fail everything outstanding
                    self._fail_all(e)
                    return
                for index, value, is_exc in results:
                    future = items[index][0]
                    with self._cond:
                        if self._outstanding.pop(future, None) is None:
                            continue
                    if is_exc:
                        self._resolve(future, exception=value)
                    else:
                        self._resolve(future, result=value)
                if results:
                    # one shutdown-waiter wake per pass, not per resolve
                    with self._cond:
                        self._cond.notify_all()
                else:
                    return
        finally:
            self._drain_state.active = False
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
