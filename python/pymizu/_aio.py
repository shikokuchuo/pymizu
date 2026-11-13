"""asyncio integration: event-driven futures over the notify endpoints.

One notify record per armed handle (the C half) bridges the core's
edge-triggered watch and an asyncio loop's ``call_soon_threadsafe``; this
module is the drain half: the per-handle pending registries, the bounded
drain quantum, and the enrollment discipline. No polling and no extra
threads anywhere — the watch rides the core's death-listener machinery,
and a burst collapses to one wake per idle-to-data transition.

Enrollment is the parker protocol transplanted, in the R enroll's order:
a task's result slot gains a NOTIFY announce, its state is probed, and
only a pending task arms the submitter's word — an already-published task
settles at the call site with no arm cycle at all, and the post-arm
re-check covers the new entry only (every other slot's publish rides the
armed word's wake). The arm is sticky: it survives an emptied waiter set
for the next enrollment to reuse, dropping only on a wake that finds
nothing enrolled (one spurious pass per idle period) or at teardown.
Settle (resolve, reject, cancel) withdraws every announce, restoring each
slot's one-writer regime — a detached task or collection is
blocking-collectible again. One consumer per result: an enrolled task
refuses blocking collects and a second enrollment; cancelling the Future
detaches the waiter, never the task.

Every drain is bounded per callback (the quantum below) and re-scheduled
with ``loop.call_soon``: under sustained load the ring never empties, and
an unbounded drain would starve the loop it runs on. asyncio is imported
only here, and this module only from the async verbs, so ``import
pymizu`` never pays for it.
"""

from __future__ import annotations

import asyncio as _asyncio
import operator as _operator
import time as _time
from collections import deque as _deque
from typing import TYPE_CHECKING as _TYPE_CHECKING
from typing import Any as _Any

from pymizu._pymizu import CLOSED as _CLOSED
from pymizu._pymizu import PEER_GONE as _GONE
from pymizu._pymizu import ForeignPayloadError as _ForeignPayloadError

if _TYPE_CHECKING:  # pragma: no cover
    import pymizu

# The drain quantum: at most this many resolutions per callback, or this
# much wall time, whichever first — then re-schedule with call_soon.
# (mizu's tuned value: a 256-item quantum left the batch handler 2.2x off
# the sync row; 2048 lands within band.)
_QUANTUM_N = 2048
_QUANTUM_SEC = 0.001


def _quantum_hit(work: int, deadline: float) -> bool:
    """The drain quantum verdict: the item cap, or the wall-clock bound
    probed every 64 items (the clock read itself is not free)."""
    return work >= _QUANTUM_N or (
        (work & 63) == 0 and _time.monotonic() >= deadline
    )


class _WaiterFuture(_asyncio.Future):
    """The enrollment waiters: cancel() routes straight to the detach
    half (never the task or collection), dropping the per-settle
    done-callback that the add_done_callback form scheduled on every
    resolution. Detach runs on the cancelling thread — the loop's, by
    asyncio's own thread-confinement discipline. ``want`` is the channel
    waiter's batch size (1 for a single recv); ``batch`` distinguishes a
    one-element batch from a single receive. The pool half's futures are
    built by the C side with CallNoArgs, so the defaults cost them
    nothing. ``_detach`` is one of the module-level detach functions —
    stored plain, never a per-enrollment closure — taking the future
    itself, with ``_st`` / ``_arg`` carrying its state and argument."""

    __slots__ = ("_detach", "_st", "_arg", "want", "batch")

    def __init__(self) -> None:
        super().__init__()
        self._detach: _Any = None
        self._st: _Any = None
        self._arg: _Any = None
        self.want = 1
        self.batch = False

    def cancel(self, msg: _Any = None) -> bool:
        detach, self._detach = self._detach, None
        if detach is not None and not self.done():
            detach(self)
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
    record's drain is ``_drain``. The raw handle is stashed (the drain's
    per-pass verbs skip the facade's attribute walk; _PoolState's pool_h
    is the mirror). ``wants`` sums the LIVE members' batch sizes
    exactly: enrollment appends add, resolution pops subtract, and a
    cancel settles its waiter's count at detach — the cancelled entry
    then rides the FIFO as a dead slot the drain pops free (the lazy
    head-pop: detach is O(1), never a deque scan) — so the drain's ask
    is O(1): a per-pass sum made enrollment O(n·min(n, quantum)).
    Over-count is the hazard direction
    (an ask past the live wants over-reads the burst into a popleft that
    raises, failing every waiter); under-count only shortens a burst.
    ``batches`` counts the live batch waiters (including n = 1) under the
    same discipline, so the distribution can run the index-free one-element-
    per-pop form when the FIFO is all singles — the common case pays no
    per-message bookkeeping beyond the counter."""

    __slots__ = ("ch_h", "loop", "fifo", "wants", "batches", "armed")

    def __init__(self, ch: pymizu.Channel) -> None:
        self.ch_h = ch._h
        self.loop: _asyncio.AbstractEventLoop | None = None
        self.fifo: _deque[_WaiterFuture] = _deque()
        self.wants = 0
        self.batches = 0
        self.armed = False

    def _disarm(self) -> None:
        if self.armed:
            self.armed = False
            self.ch_h._notify_disarm()

    def _fail(self, exc: BaseException) -> None:
        for f in self.fifo:
            _settle(f, exc, exc=True)
        self.fifo.clear()
        self.wants = 0
        self.batches = 0

    def _drain(self) -> None:
        """One drain pass (synchronous at arm, then one per wake): recv
        without blocking, resolving the FIFO in arrival order — a single
        waiter takes one message, a batch waiter up to its ``want`` as
        one list (short when the burst runs out, never waiting to fill).
        Ends disarmed (the FIFO emptied) or re-armed — the
        announce/recheck transplant: a publish racing the re-arm clears
        the bit and signals, riding the next wake. The receives ride one
        GIL-held burst verb per pass, not a recv(0) per message."""
        if not self.armed:
            return
        work = 0
        deadline = _time.monotonic() + _QUANTUM_SEC
        try:
            while True:
                while self.fifo and self.fifo[0].done():
                    # a cancelled waiter's count settled at detach: the
                    # pop alone reclaims it, and the next message goes
                    # to the next waiter
                    self.fifo.popleft()
                if not self.fifo:
                    self._disarm()
                    return
                # the pass's ask: the members' wants total, capped by the
                # remaining quantum — the cap is also the allocation
                # bound: _recv_burst(n) preallocates PyList_New(n), so an
                # uncapped recv_batch_async(10**9) would attempt an 8 GB
                # allocation; a want past the quantum resolves
                # quantum-bounded short, per pass
                cap = _QUANTUM_N - work
                if cap <= 0:
                    assert self.loop is not None
                    self.loop.call_soon(self._drain)
                    return
                want = min(cap, self.wants)
                try:
                    burst = self.ch_h._recv_burst(want)
                except _ForeignPayloadError as e:
                    # a declined foreign payload: the burst consumed the
                    # slot as its first message, so reject the head
                    # waiter only (a batch waiter at the head rejects
                    # whole, as recv_batch raises) — the drain keeps
                    # flowing (recv_batch's exact semantics, mizu's
                    # handler discipline). The generic except below stays
                    # the fail-all path
                    fut = self.fifo.popleft()
                    w = fut.want
                    self.wants -= w
                    if fut.batch:
                        self.batches -= 1
                    if not fut.done():
                        fut.set_exception(e)
                    work += 1
                    continue
                if burst and (burst[0] is _CLOSED or burst[0] is _GONE):
                    # a terminal reports only on an empty read, so the
                    # burst is exactly [sentinel] — one guarded test up
                    # front, no per-element is-test, and a batch waiter
                    # never splits data from a trailing sentinel
                    _settle_channel_now(self, burst[0])
                    return
                nb = len(burst)
                if not self.batches:
                    # all live members singles (wants == their count
                    # here): the index-free form, one element per pop,
                    # the counter settled in bulk — exactly nb live
                    # pops happened; cancelled entries pop free
                    for x in burst:
                        fut = self.fifo.popleft()
                        while fut.done():
                            fut = self.fifo.popleft()
                        fut.set_result(x)
                    self.wants -= nb
                else:
                    i = 0
                    while i < nb:
                        fut = self.fifo.popleft()
                        if fut.done():
                            # settled at detach: pops free, consumes
                            # nothing
                            continue
                        w = fut.want
                        self.wants -= w
                        if fut.batch:
                            self.batches -= 1
                        if not fut.batch:
                            fut.set_result(burst[i])
                            i += 1
                        else:
                            # the slice is short when the burst runs
                            # out — within contract (>= 1 when the
                            # waiter is reached, the wants summing
                            # to the ask)
                            fut.set_result(burst[i : i + w])
                            i += w
                work += nb
                # the quantum check rides progress only: an empty burst
                # falls through to the re-arm below, never to a
                # reschedule — (work & 63) == 0 holds at work == 0, and a
                # slow empty pass past the deadline would otherwise queue
                # a bare call_soon drain that bypasses a disarm landing
                # before it runs (the trampoline's skip is C-side)
                # the quantum expression stays inline: this branch fires
                # per pass, and a helper call showed up at ~70 ns/message
                # on the one-await-per-message row
                if nb and (
                    work >= _QUANTUM_N
                    or ((work & 63) == 0 and _time.monotonic() >= deadline)
                ):
                    assert self.loop is not None
                    self.loop.call_soon(self._drain)
                    return
                if nb < want:
                    # the ring drained: re-arm, then the availability
                    # report decides — False stays armed, waiting. A
                    # burst cut short by a decline past its first message
                    # lands here too: the re-arm reports data available,
                    # the loop continues, and the next burst raises the
                    # decline as its first message
                    if not self.ch_h._notify_rearm():
                        return
        except Exception as e:
            self._fail(e)
            self._disarm()


def chan_recv_async(
    ch: pymizu.Channel, want: int = 1, batch: bool = False
) -> _asyncio.Future:
    """[`Channel.recv_async()`](`pymizu.Channel.recv_async`)'s and
    [`Channel.recv_batch_async()`](`pymizu.Channel.recv_batch_async`)'s
    engine: enqueue one waiter (a batch waiter's ``want`` messages as
    one list), arm (the first call also registers the watch), and run
    the synchronous drain — loss-free: data already in the ring resolves
    from this pass; only genuinely pending entries ride the watch."""
    if batch:
        # the batch path alone pays validation: recv_async's literal 1
        # skips it (the codebase's exact-type discipline: bool still
        # resolves through __index__)
        if type(want) is not int:
            want = _operator.index(want)
        if want < 1:
            # the sync verb's own text, raised at the call site: inside
            # the drain's burst it would fail every pending waiter
            raise ValueError("pymizu: n must be at least 1")
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
    if batch:
        fut.batch = True
    if want > 1:
        # __init__ already stores 1: singles skip the slot store
        fut.want = want
    fut._detach = _detach_chan
    fut._st = st
    st.fifo.append(fut)
    st.wants += want
    if batch:
        st.batches += 1
    if not st.armed:
        try:
            ch._h._notify_arm(loop, st._drain)
        except BaseException:
            # a spin channel, a destroyed handle: the waiter never
            # enrolled — it is still the just-appended tail
            assert st.fifo.pop() is fut
            st.wants -= want
            if batch:
                st.batches -= 1
            fut._detach = None
            raise
        st.armed = True
        st.loop = loop
    st._drain()
    return fut


def _detach_chan(fut: _WaiterFuture) -> None:
    """Future.cancel() detaches the waiter: its count settles here and
    the cancelled entry stays for the drain's lazy head-pop — O(1),
    never a deque scan; the next message goes to the next waiter. The
    arm is the drain's to drop when the FIFO empties (a wake finding
    only dead entries), not the detach's."""
    st = fut._st
    st.wants -= fut.want
    if fut.batch:
        st.batches -= 1


def _settle_channel_now(st: _ChanState, value: _Any) -> None:
    for f in st.fifo:
        _settle(f, value)
    st.fifo.clear()
    st.wants = 0
    st.batches = 0
    st._disarm()


def _settle_via_loop(st: _Any, settle: _Any, arg: _Any) -> None:
    """The teardown settle's thread discipline, shared by both handle
    kinds: asyncio Futures are loop-thread-confined, so the settle rides
    call_soon_threadsafe when a loop captured one (settling in place on a
    closed loop); the teardown never waits on it."""
    loop = st.loop
    if loop is None:
        settle(st, arg)
        return
    try:
        loop.call_soon_threadsafe(settle, st, arg)
    except RuntimeError:
        settle(st, arg)  # a closed loop: settle in place


def settle_channel(ch: pymizu.Channel, value: _Any) -> None:
    """The facade teardown's settle: resolve every pending recv with the
    terminal value (CLOSED on close, PEER_GONE on destroy) and disarm."""
    st = ch._h._aio
    if st is None:
        return
    _settle_via_loop(st, _settle_channel_now, value)


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
    """A pool handle's async consumer state: the collections sharing the
    submitter's one coalescing word (the single-task waiters live in the
    C registry on the handle). Lives on the C handle's ``_aio`` slot; the
    notify record's drain is ``_pump``."""

    __slots__ = ("pool_h", "loop", "armed", "collections")

    def __init__(self, pool_h: _Any) -> None:
        self.pool_h = pool_h
        self.loop: _asyncio.AbstractEventLoop | None = None
        self.armed = False
        self.collections: list[_Collection] = []

    def _disarm(self) -> None:
        if self.armed:
            self.armed = False
            self.pool_h._notify_disarm()

    def _pump(self) -> None:
        """One drain pass (one per wake): re-arm the submitter word, scan
        the enrolled set, resolve what newly turned terminal. The singles
        half is one C scan over the enrolled words; the collections half
        scans its own tasks. The pass exits only when a scan finds nothing
        new — the re-arm then stands and a publish signals the next wake
        (the announce/recheck transplant). The arm is sticky: a pass that
        worked leaves it standing for the next enrollment to reuse; only
        a wake finding nothing enrolled at all disarms (one spurious pass
        per idle period)."""
        if not self.armed:
            return
        work = 0
        deadline = _time.monotonic() + _QUANTUM_SEC
        try:
            first = True
            while True:
                self.pool_h._notify_rearm()
                n, left = self.pool_h._pump_singles(_QUANTUM_N - work)
                work += n
                if n and _quantum_hit(work, deadline):
                    assert self.loop is not None
                    self.loop.call_soon(self._pump)
                    return
                colls = [c for c in self.collections if not c.done]
                if not colls and left == 0:
                    # the sticky arm drops only on a wake that finds
                    # nothing enrolled; a worked pass leaves it standing
                    if first and n == 0:
                        self._disarm()
                    return
                first = False
                progressed = n > 0
                if colls:
                    term = self.pool_h._tasks_terminal_set(
                        [t for c in colls for t in c.tasks]
                    )
                    for c in colls:
                        if not _collection_ready(c, term):
                            # the morsel trim, as the sync collect's
                            # passes: dead-weight runners cancel into a
                            # wake
                            if c.pre_scan is not None:
                                c.pre_scan(term)
                            continue
                        _settle_collection(self, c)
                        progressed = True
                        work += 1
                        if _quantum_hit(work, deadline):
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
        return not term.isdisjoint(c.tasks)
    return term.issuperset(c.tasks)


def _collection_ready_now(st: _PoolState, c: _Collection) -> bool:
    """The enrollment probe's cheap readiness verdict: a bool list plus
    the short-circuiting builtin, never a set — burst enrollments (every
    member still pending) cost one C walk and no hashing."""
    term = st.pool_h._tasks_terminal(c.tasks)
    if c.kind == "any":
        return any(term)
    return all(term)


def _settle_collection(st: _PoolState, c: _Collection) -> None:
    c.done = True
    try:
        st.collections.remove(c)
    except ValueError:
        pass  # settling at the enrollment call site: never registered
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
    colls = st.collections
    st.collections = []
    tasks = [t for c in colls for t in c.tasks]
    if tasks:
        # best-effort: the pool may already be gone — nothing to withdraw
        # from then, and the flags still clear
        st.pool_h._tasks_release(tasks)
    # the singles half, one C sweep over the registry
    st.pool_h._singles_fail(exc)
    for c in colls:
        c.done = True
        _settle(c.fut, exc, exc=True)


def _pool_state(pool_h: _Any) -> _PoolState:
    st = pool_h._aio
    if st is None:
        st = _PoolState(pool_h)
        pool_h._aio = st
    return st


def _arm(st: _PoolState, loop: _asyncio.AbstractEventLoop) -> bool:
    """Arm the endpoint; True when the enrollment must re-check — a fresh
    arm, or a loop re-capture: while the record pointed at the dead loop,
    a publish's signal ended at its closed call_soon_threadsafe (the C
    callback's RuntimeError disarm), so no pump rides that wake."""
    if st.armed and st.loop is not loop:
        # as the channel's: the old loop closed; fail its waiters and
        # re-capture
        _fail_all(st, RuntimeError("pymizu: event loop changed"))
        st.armed = False
    if st.armed:
        # the announce stands (the core's re-arm is a no-op on an armed
        # word); the signals keep landing on this live loop
        return False
    st.pool_h._notify_arm(loop, st._pump)
    st.armed = True
    st.loop = loop
    return True


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
    announce, all-or-nothing — a second consumer raises here), the
    terminal probe, and only then the arm — a collection whose readiness
    already holds settles at the call site with no arm cycle at all. One
    Future per collection — never one per task."""
    fut = _WaiterFuture()
    c = _Collection(kind, list(tasks), fut, finish, pre_scan, ondetach)
    if c.tasks:
        st.pool_h._tasks_enroll(c.tasks)
    try:
        if c.tasks and _collection_ready_now(st, c):
            # already terminal: settle here, never arming
            _settle_collection(st, c)
            return fut
        fresh_arm = _arm(st, loop)
    except BaseException:
        if c.tasks:
            st.pool_h._tasks_release(c.tasks)  # best-effort, never raises
        raise
    st.collections.append(c)
    fut._detach = _detach_collection
    fut._st = st
    fut._arg = c
    if c.tasks:
        # the post-arm re-check covers this collection only, and only an
        # arm that (re-)captures needs it: under a standing arm on this
        # same loop a racing publish signals the wake whose pump scans
        # the set (the R enroll's fence-when-armed rule)
        if fresh_arm and _collection_ready_now(st, c):
            _settle_collection(st, c)
    else:
        # an empty map: nothing to wake for — settle from the call site
        _settle_collection(st, c)
    return fut


def _detach_collection(fut: _WaiterFuture) -> None:
    """Future.cancel() detaches the collection: every member's announce
    un-stores (the one-writer regime restored — each returns to
    blocking-collectible) and the map handle frees up. Never cancels the
    tasks."""
    st, c = fut._st, fut._arg
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


def _collect_async_pending(task: _Any) -> _asyncio.Future:
    """[`Task.collect_async()`](`pymizu.Task.collect_async`)'s pending
    half, called from the C fast path with the enrollment already on
    (flag + announce) and the task probed pending: arm, register in the
    C registry, re-check the one entry."""
    loop = _asyncio.get_running_loop()
    st = _pool_state(task.pool)
    try:
        _arm(st, loop)
        # registered only after the arm: a loop change in _arm fails the
        # old loop's waiters, never this fresh one (the channel's
        # discipline)
        fut = _WaiterFuture()
        fut._detach = _detach_single
        fut._st = st
        fut._arg = task
        st.pool_h._singles_add(task, fut)
    except BaseException:
        # best-effort, idempotent (the C caller unwinds the same way)
        st.pool_h._tasks_release([task])
        raise
    return fut


def _detach_single(fut: _WaiterFuture) -> None:
    """Future.cancel() detaches the waiter: the registry drops the entry,
    clearing the flag and withdrawing the announce, so a later blocking
    collect() works. The sticky arm is the next empty wake's to drop, not
    the detach's."""
    fut._st.pool_h._singles_remove(fut._arg)


def _task_list(tasks: _Any) -> list:
    """The collect_*_async gate: a materialized non-empty list. The C
    enroll validates the handles themselves (types, pool ownership, a
    pending consumer)."""
    tasks = list(tasks)
    if not tasks:
        raise ValueError(
            "pymizu: tasks must be a non-empty sequence of task handles"
        )
    return tasks


def collect_any_async(pool: pymizu.Pool, tasks: _Any) -> _asyncio.Future:
    """[`Pool.collect_any_async()`](`pymizu.Pool.collect_any_async`)'s
    engine: one Future, settled when the first enrolled task turns
    terminal."""
    loop = _asyncio.get_running_loop()
    st = _pool_state(pool._h)
    return _enroll_collection(st, "any", _task_list(tasks), loop)


def collect_all_async(pool: pymizu.Pool, tasks: _Any) -> _asyncio.Future:
    """[`Pool.collect_all_async()`](`pymizu.Pool.collect_all_async`)'s
    engine: one Future, settled when every enrolled task is terminal."""
    loop = _asyncio.get_running_loop()
    st = _pool_state(pool._h)
    return _enroll_collection(st, "all", _task_list(tasks), loop)


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
    closed-handle error and disarm."""
    st = pool_h._aio
    if st is None:
        return
    _settle_via_loop(st, _settle_pool_now, exc)
