"""asyncio integration: event-driven futures over the notify endpoints.

One notify record per armed handle (the C half) bridges the core's
edge-triggered watch and an asyncio loop's ``call_soon_threadsafe``; this
module is the drain half: the per-handle pending registries, the bounded
drain quantum, and the enrollment discipline. No polling and no extra
threads anywhere — the watch rides the core's death-listener machinery,
and a burst collapses to one wake per idle-to-data transition.

Enrollment (announce before arm, then re-check) is the parker protocol
transplanted, exactly as the core documents it: a task's result slot
gains a NOTIFY announce, the submitter's word arms, and the synchronous
scan resolves whatever was already published. Settle (resolve, reject,
cancel) withdraws every announce, restoring each slot's one-writer regime
— a detached task or collection is blocking-collectible again. One
consumer per result: an enrolled task refuses blocking collects and a
second enrollment; cancelling the Future detaches the waiter, never the
task.

Every drain is bounded per callback (the quantum below) and re-scheduled
with ``loop.call_soon``: under sustained load the ring never empties, and
an unbounded drain would starve the loop it runs on. asyncio is imported
only here, and this module only from the async verbs, so ``import
pymizu`` never pays for it.
"""

from __future__ import annotations

import asyncio as _asyncio
import time as _time
from collections import deque as _deque
from typing import TYPE_CHECKING as _TYPE_CHECKING
from typing import Any as _Any

from pymizu._pymizu import CLOSED as _CLOSED
from pymizu._pymizu import PEER_GONE as _GONE

if _TYPE_CHECKING:  # pragma: no cover
    import pymizu

# The drain quantum: at most this many resolutions per callback, or this
# much wall time, whichever first — then re-schedule with call_soon.
# (mizu's tuned value: a 256-item quantum left the batch handler 2.2x off
# the sync row; 2048 lands within band.)
_QUANTUM_N = 2048
_QUANTUM_SEC = 0.001


class _WaiterFuture(_asyncio.Future):
    """The enrollment waiters: cancel() routes straight to the detach
    half (never the task or collection), dropping the per-settle
    done-callback that the add_done_callback form scheduled on every
    resolution. Detach runs on the cancelling thread — the loop's, by
    asyncio's own thread-confinement discipline."""

    __slots__ = ("_detach",)

    def __init__(self) -> None:
        super().__init__()
        self._detach: _Any = None

    def cancel(self, msg: _Any = None) -> bool:
        detach, self._detach = self._detach, None
        if detach is not None and not self.done():
            detach()
        return super().cancel(msg)


def _settle(fut: _asyncio.Future, value: _Any, exc: bool = False) -> None:
    """set_result / set_exception tolerating a closed loop (a waiter on a
    dead loop is unnotifyable either way — asyncio's own warnings stand)."""
    if fut.done():
        return
    try:
        if exc:
            fut.set_exception(value)
        else:
            fut.set_result(value)
    except RuntimeError:
        pass


# Channels -------------------------------------------------------------------


class _ChanState:
    """A channel's async consumer state: the pending recv FIFO, resolved
    in arrival order. Lives on the C handle's ``_aio`` slot; the notify
    record's drain is ``_drain``."""

    __slots__ = ("ch", "loop", "fifo", "armed")

    def __init__(self, ch: pymizu.Channel) -> None:
        self.ch = ch
        self.loop: _asyncio.AbstractEventLoop | None = None
        self.fifo: _deque[_asyncio.Future] = _deque()
        self.armed = False

    def _disarm(self) -> None:
        if self.armed:
            self.armed = False
            self.ch._h._notify_disarm()

    def _fail(self, exc: BaseException) -> None:
        for f in self.fifo:
            _settle(f, exc, exc=True)
        self.fifo.clear()

    def _drain(self) -> None:
        """One drain pass (synchronous at arm, then one per wake): recv
        without blocking, resolving the FIFO in arrival order. Ends
        disarmed (the FIFO emptied) or re-armed — the announce/recheck
        transplant: a publish racing the re-arm clears the bit and
        signals, riding the next wake. The receives ride one GIL-held
        burst verb per pass, not a recv(0) per message."""
        if not self.armed:
            return
        work = 0
        deadline = _time.monotonic() + _QUANTUM_SEC
        try:
            while True:
                while self.fifo and self.fifo[0].done():
                    # a cancelled waiter detaches: the next message goes
                    # to the next waiter
                    self.fifo.popleft()
                if not self.fifo:
                    self._disarm()
                    return
                want = min(len(self.fifo), _QUANTUM_N - work)
                if want <= 0:
                    assert self.loop is not None
                    self.loop.call_soon(self._drain)
                    return
                burst = self.ch._h._recv_burst(want)
                n = 0
                for x in burst:
                    if x is _CLOSED or x is _GONE:
                        for f in self.fifo:
                            if not f.done():
                                f.set_result(x)
                        self.fifo.clear()
                        self._disarm()
                        return
                    fut = self.fifo.popleft()
                    if not fut.done():
                        fut.set_result(x)
                    n += 1
                work += n
                if work >= _QUANTUM_N or (
                    (work & 63) == 0 and _time.monotonic() >= deadline
                ):
                    assert self.loop is not None
                    self.loop.call_soon(self._drain)
                    return
                if len(burst) < want:
                    # the ring drained: re-arm, then the availability
                    # report decides — False stays armed, waiting
                    if not self.ch._h._notify_rearm():
                        return
        except Exception as e:
            self._fail(e)
            self._disarm()


def chan_recv_async(ch: pymizu.Channel) -> _asyncio.Future:
    """[`Channel.recv_async()`](`pymizu.Channel.recv_async`)'s engine:
    enqueue one waiter, arm (the first call also registers the watch),
    and run the synchronous drain — loss-free: data already in the ring
    resolves from this pass; only genuinely pending entries ride the
    watch."""
    loop = _asyncio.get_running_loop()
    st = ch._h._aio
    if st is None:
        st = _ChanState(ch)
        ch._h._aio = st
    if st.armed and st.loop is not loop:
        # the old loop closed (the core disarms on the failed schedule):
        # fail its waiters and re-capture — asyncio's own warnings are
        # the user's signal, as for any asyncio primitive
        st._fail(RuntimeError("pymizu: event loop changed"))
        st.armed = False
    fut = _WaiterFuture()
    fut._detach = lambda: _detach_chan(st, fut)
    st.fifo.append(fut)
    if not st.armed:
        try:
            ch._h._notify_arm(loop, st._drain)
        except BaseException:
            # a spin channel, a destroyed handle: the waiter never enrolled
            st.fifo.remove(fut)
            raise
        st.armed = True
        st.loop = loop
    st._drain()
    return fut


def _detach_chan(st: _ChanState, fut: _asyncio.Future) -> None:
    """Future.cancel() detaches the waiter: removed from the FIFO, the
    next message going to the next waiter."""
    try:
        st.fifo.remove(fut)
    except ValueError:
        return  # already resolved and popped by the drain
    if not st.fifo:
        st._disarm()


def _settle_channel_now(st: _ChanState, value: _Any) -> None:
    for f in st.fifo:
        _settle(f, value)
    st.fifo.clear()
    st._disarm()


def settle_channel(ch: pymizu.Channel, value: _Any) -> None:
    """The facade teardown's settle: resolve every pending recv with the
    terminal value (CLOSED on close, PEER_GONE on destroy) and disarm.
    asyncio Futures are loop-thread-confined, so the settle rides
    call_soon_threadsafe when a loop captured one; the teardown never
    waits on it."""
    st = ch._h._aio
    if st is None:
        return
    loop = st.loop
    if loop is None:
        _settle_channel_now(st, value)
        return
    try:
        loop.call_soon_threadsafe(_settle_channel_now, st, value)
    except RuntimeError:
        _settle_channel_now(st, value)  # a closed loop: settle in place


# Pools ---------------------------------------------------------------------


class _Collection:
    """One Future per collect_any / collect_all / map collection: the
    enrolled tasks and the completion path. The any/all kinds settle
    through the sync verb (timeout=0), whose semantics they mirror
    exactly; a map settles through its handle's gather. Settle — resolve,
    reject, or detach — withdraws every member's announce."""

    __slots__ = (
        "kind",
        "tasks",
        "fut",
        "finish",
        "pre_scan",
        "ondetach",
        "done",
    )

    def __init__(
        self,
        kind: str,
        tasks: list,
        fut: _asyncio.Future,
        finish: _Any = None,
        pre_scan: _Any = None,
        ondetach: _Any = None,
    ) -> None:
        self.kind = kind
        self.tasks = tasks
        self.fut = fut
        self.finish = finish  # the map's gather (kind "map")
        self.pre_scan = pre_scan  # the map's abandon trim, per wake
        self.ondetach = ondetach  # restores the map handle on detach
        self.done = False


class _PoolState:
    """A pool handle's async consumer state: the single-task futures and
    the collections sharing the submitter's one coalescing word. Lives on
    the C handle's ``_aio`` slot; the notify record's drain is ``_pump``."""

    __slots__ = ("pool_h", "loop", "armed", "singles", "collections")

    def __init__(self, pool_h: _Any) -> None:
        self.pool_h = pool_h
        self.loop: _asyncio.AbstractEventLoop | None = None
        self.armed = False
        self.singles: dict[_Any, _asyncio.Future] = {}
        self.collections: list[_Collection] = []

    def _disarm(self) -> None:
        if self.armed:
            self.armed = False
            self.pool_h._notify_disarm()

    def _maybe_disarm(self) -> None:
        if not self.singles and not self.collections:
            self._disarm()

    def _enrolled(self) -> list:
        tasks = list(self.singles)
        for c in self.collections:
            if not c.done:
                tasks.extend(c.tasks)
        return tasks

    def _pump(self) -> None:
        """One drain pass (synchronous at arm, then one per wake): re-arm
        the submitter word, scan the enrolled set, resolve what newly
        turned terminal. The pass exits only when a scan finds nothing
        new — the re-arm then stands and a publish signals the next wake
        (the announce/recheck transplant)."""
        if not self.armed:
            return
        work = 0
        deadline = _time.monotonic() + _QUANTUM_SEC
        try:
            while True:
                self.pool_h._notify_rearm()
                tasks = self._enrolled()
                if not tasks:
                    self._disarm()
                    return
                term = self.pool_h._tasks_terminal_set(tasks)
                progressed = False
                for task in list(self.singles):
                    if task not in term:
                        continue
                    fut = self.singles.pop(task)
                    task._async_set(False)
                    if not fut.done():
                        try:
                            fut.set_result(task.collect(0))
                        except Exception as e:
                            fut.set_exception(e)
                    progressed = True
                    work += 1
                    if work >= _QUANTUM_N or (
                        (work & 63) == 0 and _time.monotonic() >= deadline
                    ):
                        assert self.loop is not None
                        self.loop.call_soon(self._pump)
                        return
                for c in list(self.collections):
                    if c.done:
                        continue
                    if not _collection_ready(c, term):
                        # the morsel trim, as the sync collect's passes:
                        # dead-weight runners cancel into a wake
                        if c.pre_scan is not None:
                            c.pre_scan(term)
                        continue
                    _settle_collection(self, c)
                    progressed = True
                    work += 1
                    if work >= _QUANTUM_N or (
                        (work & 63) == 0 and _time.monotonic() >= deadline
                    ):
                        assert self.loop is not None
                        self.loop.call_soon(self._pump)
                        return
                if not progressed:
                    return
        except Exception as e:
            _fail_all(self, e)
            self._disarm()


def _collection_ready(c: _Collection, term: set) -> bool:
    if c.kind == "any":
        return any(t in term for t in c.tasks)
    return all(t in term for t in c.tasks)


def _settle_collection(st: _PoolState, c: _Collection) -> None:
    c.done = True
    st.collections.remove(c)
    # un-enroll every member in one call: a consumed slot's announce was
    # consumed by its publish (the withdrawal restores the plain announce
    # only where this submitter's still stands), the unconsumed return to
    # blocking-collectible
    if c.tasks:
        st.pool_h._tasks_release(c.tasks)
    if c.fut.done():
        return
    try:
        if c.kind == "map":
            value = c.finish()
        elif c.kind == "any":
            value = st.pool_h.collect_any(c.tasks, 0)
        else:
            value = st.pool_h.collect_all(c.tasks, 0)
    except Exception as e:
        c.fut.set_exception(e)
    else:
        c.fut.set_result(value)


def _fail_all(st: _PoolState, exc: BaseException) -> None:
    """Settle every enrolled waiter with the same error (a broken scan —
    the pool destroyed mid-wait — or a loop change)."""
    singles = st.singles
    colls = st.collections
    st.singles = {}
    st.collections = []
    tasks = [*singles, *(t for c in colls for t in c.tasks)]
    if tasks:
        # best-effort: the pool may already be gone — nothing to withdraw
        # from then, and the flags still clear
        st.pool_h._tasks_release(tasks)
    for fut in singles.values():
        _settle(fut, exc, exc=True)
    for c in colls:
        c.done = True
        _settle(c.fut, exc, exc=True)


def _pool_state(pool_h: _Any) -> _PoolState:
    st = pool_h._aio
    if st is None:
        st = _PoolState(pool_h)
        pool_h._aio = st
    return st


def _arm(st: _PoolState, loop: _asyncio.AbstractEventLoop) -> None:
    if st.armed and st.loop is not loop:
        # as the channel's: the old loop closed; fail its waiters and
        # re-capture
        _fail_all(st, RuntimeError("pymizu: event loop changed"))
        st.armed = False
    if st.armed:
        # the announce stands (the core's re-arm is a no-op on an armed
        # word); the enrollment's synchronous re-check follows
        return
    st.pool_h._notify_arm(loop, st._pump)
    st.armed = True
    st.loop = loop


def _enroll_collection(
    st: _PoolState,
    kind: str,
    tasks: list,
    loop: _asyncio.AbstractEventLoop,
    finish: _Any = None,
    pre_scan: _Any = None,
    ondetach: _Any = None,
) -> _asyncio.Future:
    """The shared collection enrollment: one fused enroll (flag per task +
    announce, all-or-nothing — a second consumer raises here), arm, and
    the synchronous re-check. One Future per collection — never one per
    task."""
    fut = _WaiterFuture()
    c = _Collection(kind, list(tasks), fut, finish, pre_scan, ondetach)
    if c.tasks:
        st.pool_h._tasks_enroll(c.tasks)
    try:
        _arm(st, loop)
    except BaseException:
        if c.tasks:
            st.pool_h._tasks_release(c.tasks)  # best-effort, never raises
        raise
    st.collections.append(c)
    fut._detach = lambda: _detach_collection(st, c)
    if c.tasks:
        st._pump()
    else:
        # an empty map: nothing to wake for — settle from the arm's
        # synchronous half
        _settle_collection(st, c)
    return fut


def _detach_collection(st: _PoolState, c: _Collection) -> None:
    """Future.cancel() detaches the collection: every member's announce
    un-stores (the one-writer regime restored — each returns to
    blocking-collectible) and the map handle frees up. Never cancels the
    tasks."""
    if c.done:
        return
    c.done = True
    try:
        st.collections.remove(c)
    except ValueError:
        pass
    if c.tasks:
        # best-effort withdraw: a destroyed pool has nothing to withdraw
        # from (teardown settles everything)
        st.pool_h._tasks_release(c.tasks)
    if c.ondetach is not None:
        c.ondetach()
    st._maybe_disarm()


def task_collect_async(task: _Any) -> _asyncio.Future:
    """[`Task.collect_async()`](`pymizu.Task.collect_async`)'s engine (the
    C method forwards here)."""
    loop = _asyncio.get_running_loop()
    pool_h = task.pool
    st = _pool_state(pool_h)
    pool_h._tasks_enroll([task])  # raises on a second consumer
    try:
        _arm(st, loop)
    except BaseException:
        pool_h._tasks_release([task])
        raise
    # registered only after the arm: a loop change in _arm fails the old
    # loop's waiters, never this fresh one (the channel's discipline)
    fut = _WaiterFuture()
    fut._detach = lambda: _detach_single(st, task)
    st.singles[task] = fut
    st._pump()
    return fut


def _detach_single(st: _PoolState, task: _Any) -> None:
    """Future.cancel() detaches the waiter: the announce un-stores and the
    task leaves the armed set, so a later blocking collect() works."""
    if st.singles.pop(task, None) is None:
        return
    # best-effort withdraw: a destroyed pool has nothing to withdraw from
    st.pool_h._tasks_release([task])
    st._maybe_disarm()


def _check_tasks(pool: pymizu.Pool, tasks: _Any) -> list:
    tasks = list(tasks)
    if not tasks:
        raise ValueError(
            "pymizu: tasks must be a non-empty sequence of task handles"
        )
    for t in tasks:
        if t.pool is not pool._h:
            raise ValueError(
                "pymizu: task handles must belong to this pool handle"
            )
    return tasks


def collect_any_async(pool: pymizu.Pool, tasks: _Any) -> _asyncio.Future:
    """[`Pool.collect_any_async()`](`pymizu.Pool.collect_any_async`)'s
    engine: one Future, settled when the first enrolled task turns
    terminal."""
    loop = _asyncio.get_running_loop()
    st = _pool_state(pool._h)
    return _enroll_collection(st, "any", _check_tasks(pool, tasks), loop)


def collect_all_async(pool: pymizu.Pool, tasks: _Any) -> _asyncio.Future:
    """[`Pool.collect_all_async()`](`pymizu.Pool.collect_all_async`)'s
    engine: one Future, settled when every enrolled task is terminal."""
    loop = _asyncio.get_running_loop()
    st = _pool_state(pool._h)
    return _enroll_collection(st, "all", _check_tasks(pool, tasks), loop)


def map_collect_async(m: _Any) -> _asyncio.Future:
    """The map handle's collect_async engine: one Future off the runner
    arm, resolved with the same gather the blocking collect returns."""
    loop = _asyncio.get_running_loop()
    st = _pool_state(m.pool._h)
    tasks = [h for h in m.handles if h is not None]
    return _enroll_collection(
        st,
        "map",
        tasks,
        loop,
        finish=m._finish,
        pre_scan=m._pre_scan if m.kind == "region" else None,
        ondetach=m._async_detached,
    )


def _settle_pool_now(st: _PoolState, exc: BaseException) -> None:
    _fail_all(st, exc)
    st._disarm()


def settle_pool(pool_h: _Any, exc: BaseException) -> None:
    """The facade teardown's settle: fail every enrolled waiter with the
    closed-handle error and disarm. Rides call_soon_threadsafe (asyncio
    Futures are loop-thread-confined); the teardown never waits on it."""
    st = pool_h._aio
    if st is None:
        return
    loop = st.loop
    if loop is None:
        _settle_pool_now(st, exc)
        return
    try:
        loop.call_soon_threadsafe(_settle_pool_now, st, exc)
    except RuntimeError:
        _settle_pool_now(st, exc)  # a closed loop: settle in place
