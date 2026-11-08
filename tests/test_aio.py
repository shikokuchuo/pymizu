"""asyncio integration: the event-driven futures over the notify
endpoints — channel recv_async / async-for, task collect_async / await,
one-Future collect_any / collect_all / map collections, cancellation
detachment, and the edge-triggered signal accounting. No polling, no
extra threads: the watch rides the core's listener machinery.
"""

import asyncio
import subprocess
import sys
import textwrap
import threading
import time

import pytest
from tests.helpers import (
    chan_pair,
    fail_at,
    np_f64,
    sleep_ident,
    sleep_on,
    square,
)

import pymizu
from pymizu import _pymizu

ECHO = """
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE or x == "stop":
        break
    ch.send(x)
"""


@pytest.fixture
def ch():
    c = pymizu.Channel.create(ECHO)
    yield c
    c.destroy()


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


# -- channel ---------------------------------------------------------------


def test_recv_async(ch):
    async def main():
        ch.send(41)
        assert await ch.recv_async() == 41

    asyncio.run(main())


def test_recv_async_fifo(ch):
    # several outstanding calls resolve in FIFO call order
    async def main():
        f1 = ch.recv_async()
        f2 = ch.recv_async()
        ch.send("a")
        ch.send("b")
        assert await f1 == "a"
        assert await f2 == "b"

    asyncio.run(main())


def test_recv_async_arm_after_data():
    # data already in the ring resolves from the arm's synchronous drain
    host, peer = chan_pair()
    try:

        async def main():
            peer.send(1)  # physically in the ring before the arm
            fut = host.recv_async()
            assert fut.done()
            assert fut.result() == 1

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_async_terminal(ch):
    # the peer's close resolves the pending waiter with CLOSED (a value)
    async def main():
        fut = ch.recv_async()
        ch.send("stop")
        assert await fut is pymizu.CLOSED

    asyncio.run(main())


def test_async_for(ch):
    async def main():
        ch.send(1)
        ch.send(2)
        ch.send("stop")
        got = []
        async for x in ch:
            got.append(x)
        assert got == [1, 2]

    asyncio.run(main())


def test_channel_spin_arm_errors():
    ch = pymizu.Channel.create(ECHO, spin=True)
    try:

        async def main():
            with pytest.raises(pymizu.MizuError):
                ch.recv_async()

        asyncio.run(main())
    finally:
        ch.destroy()


def test_channel_destroy_settles(ch):
    async def main():
        fut = ch.recv_async()
        await asyncio.sleep(0)  # the arm lands
        ch.destroy()
        assert await fut is pymizu.PEER_GONE

    asyncio.run(main())


def test_channel_recv_async_on_destroyed_raises(ch):
    ch.destroy()

    async def main():
        with pytest.raises(ValueError, match="closed"):
            ch.recv_async()

    asyncio.run(main())


def test_channel_edge_trigger():
    # N publishes while armed bump notify_sends by exactly 1 (the bit is
    # clear-on-send), and by exactly 1 again after the re-arm
    host, peer = chan_pair()
    try:

        async def main():
            futs = [host.recv_async() for _ in range(8)]
            # the burst publishes synchronously, ahead of any drain turn
            assert peer.send_batch(list(range(8))) == 8
            assert await asyncio.gather(*futs) == list(range(8))
            assert peer.info()["notify_sends"] == 1
            futs = [host.recv_async() for _ in range(4)]
            assert peer.send_batch(list(range(4))) == 4
            assert await asyncio.gather(*futs) == list(range(4))
            assert peer.info()["notify_sends"] == 2

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_channel_quantum_split():
    # a burst over the drain quantum splits across callbacks: order
    # preserved, zero loss, and a probe callback runs between quanta
    # (the loop is not starved)
    h = _pymizu._channel_new(4096, 256, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    p.ready_set()
    assert h.ready_wait(5)
    host, peer = pymizu.Channel._wrap(h), pymizu.Channel._wrap(p)
    n = _quantum_n() + 1000
    try:

        async def main():
            futs = [host.recv_async() for _ in range(n)]
            probe = []
            asyncio.get_running_loop().call_soon(probe.append, True)
            assert peer.send_batch(list(range(n))) == n
            assert await asyncio.gather(*futs) == list(range(n))
            assert probe == [True]

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_channel_signal_coalesces_into_one_wake():
    # two armed transitions while a wake is still queued collapse to one
    # scheduled drain: nothing is lost, order is preserved
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_async()
            peer.send(1)  # transition 1: signals
            time.sleep(0.05)  # the wake is queued behind the busy loop
            host._h._notify_rearm()  # re-arm ahead of the drain
            peer.send(2)  # transition 2: coalesces into the queued wake
            time.sleep(0.05)
            f2 = host.recv_async()
            assert await f1 == 1
            assert await f2 == 2
            assert peer.info()["notify_sends"] == 2

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def _quantum_n():
    from pymizu import _aio

    return _aio._QUANTUM_N


def test_channel_closed_loop_mid_arm(ch):
    # a closed loop at wake time: the schedule fails, the core disarms,
    # and a later arm on a fresh loop resolves
    holder = []

    async def first():
        holder.append(ch.recv_async())

    asyncio.run(first())
    ch.send(1)
    time.sleep(0.2)  # the wake bounces off the closed loop

    async def second():
        assert await ch.recv_async() == 1

    asyncio.run(second())
    # the stale waiter failed with the loop-change error
    assert holder[0].exception() is not None


def test_channel_closed_loop_settle_with_callback(ch):
    # a stale waiter carrying a done callback: the loop-change failure's
    # settle raises inside asyncio's own callback scheduling (the loop is
    # closed) — swallowed, the waiter's exception is the user's signal
    holder = []

    async def first():
        fut = ch.recv_async()
        fut.add_done_callback(lambda f: None)
        holder.append(fut)

    asyncio.run(first())
    ch.send(1)
    time.sleep(0.2)  # the wake bounces off the closed loop

    async def second():
        assert await ch.recv_async() == 1

    asyncio.run(second())
    assert isinstance(holder[0].exception(), RuntimeError)


def test_recv_async_cancelled_head_skipped():
    # a cancelled waiter at the FIFO head detaches in the drain: the
    # message goes to the next waiter and the late cancel callback no-ops
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_async()
            f2 = host.recv_async()
            f1.cancel()
            peer.send(1)
            f3 = host.recv_async()  # the arm's synchronous drain
            assert await f2 == 1
            f3.cancel()
            await asyncio.sleep(0)

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_async_terminal_skips_cancelled():
    # the terminal broadcast skips a cancelled waiter
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_async()
            f2 = host.recv_async()
            f2.cancel()
            f3 = host.recv_async()
            peer._close_signal()
            assert await asyncio.gather(f1, f3) == [pymizu.CLOSED] * 2
            assert f2.cancelled()

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_async_cancelled_middle_keeps_order():
    # a cancelled waiter mid-FIFO consumes nothing: the live waiters
    # resolve in arrival order
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_async()
            f2 = host.recv_async()
            f3 = host.recv_async()
            f2.cancel()
            for x in ("a", "b"):
                peer.send(x)
            assert await f1 == "a"
            assert await f3 == "b"

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_batch_async_cancelled_batch_skipped():
    # a cancelled batch waiter settles the counters: the live waiters
    # resolve in order
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_batch_async(2)
            f2 = host.recv_async()
            f1.cancel()
            st = host._h._aio
            assert (st.wants, st.batches) == (1, 0)
            for x in ("a", "b"):
                peer.send(x)
            assert await f2 == "a"
            # a batch waiter never waits to fill: "b" alone resolves it
            assert await host.recv_batch_async(2) == ["b"]
            peer.send("c")
            assert await host.recv_async() == "c"

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_channel_drain_error_fails_waiters():
    # a handle broken under an armed drain fails the waiters with the
    # closed-handle error
    host, peer = chan_pair()
    try:

        async def main():
            fut = host.recv_async()
            host._h.destroy()
            host._h._aio._drain()
            with pytest.raises(ValueError, match="closed"):
                await fut

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_async_foreign_payload_rejects_head_only():
    # [ok, declined, ok]: the declined message rejects only its own
    # waiter with ForeignPayloadError — the drain keeps flowing
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_async()
            f2 = host.recv_async()
            f3 = host.recv_async()
            peer.send("a")
            _pymizu._channel_send_raw(peer._h, b"R")
            peer.send("c")
            assert await f1 == "a"
            with pytest.raises(pymizu.ForeignPayloadError, match="R payload"):
                await f2
            assert await f3 == "c"

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_async_for_reentry_skips_a_foreign_payload():
    # a decline ends the async-for statement; re-entering on the same
    # channel resumes with the next message (the drain kept flowing)
    host, peer = chan_pair()
    try:

        async def main():
            peer.send("a")
            _pymizu._channel_send_raw(peer._h, b"R")
            peer.send("c")
            peer._close_signal()
            got = []
            with pytest.raises(pymizu.ForeignPayloadError):
                async for x in host:
                    got.append(x)
            assert got == ["a"]
            async for x in host:
                got.append(x)
            assert got == ["a", "c"]

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_batch_async():
    # a batch waiter resolves with the burst as one list
    host, peer = chan_pair()
    try:

        async def main():
            fut = host.recv_batch_async()
            peer.send_batch(["a", "b", "c"])  # in the ring before the drain
            assert await fut == ["a", "b", "c"]

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_batch_async_fills_n():
    # the batch resolves with exactly n; the rest keeps its order
    host, peer = chan_pair()
    try:

        async def main():
            fut = host.recv_batch_async(4)
            peer.send_batch(list(range(6)))
            assert await fut == [0, 1, 2, 3]
            assert await host.recv_async() == 4

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_batch_async_mixed_fifo():
    # single and batch waiters share the FIFO: call order, right arities
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_async()
            f2 = host.recv_batch_async(2)
            f3 = host.recv_async()
            peer.send_batch(["a", "b", "c", "d", "e"])
            assert await f1 == "a"
            assert await f2 == ["b", "c"]
            assert await f3 == "d"
            assert await host.recv_async() == "e"

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_batch_async_terminal():
    # data ahead of the close resolves the batch with the list; the next
    # batch waiter gets the lone CLOSED sentinel as a value
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_batch_async()
            f2 = host.recv_batch_async()
            peer.send_batch(["a", "b"])
            peer._close_signal()
            assert await f1 == ["a", "b"]
            assert await f2 is pymizu.CLOSED

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_batch_async_foreign_payload_resolves_short():
    # [ok, declined, ok]: the batch resolves short with the prefix; the
    # waiter reaching the declined message rejects, the next keeps going
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_batch_async()
            f2 = host.recv_batch_async()
            f3 = host.recv_batch_async()
            peer.send("a")
            _pymizu._channel_send_raw(peer._h, b"R")
            peer.send("c")
            assert await f1 == ["a"]
            with pytest.raises(pymizu.ForeignPayloadError, match="R payload"):
                await f2
            assert await f3 == ["c"]

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_batch_async_foreign_payload_at_head():
    # the declined message at the ring head rejects the batch waiter
    # whole; the message behind it flows to the next waiter
    host, peer = chan_pair()
    try:

        async def main():
            f1 = host.recv_batch_async()
            f2 = host.recv_async()
            _pymizu._channel_send_raw(peer._h, b"R")
            peer.send("c")
            with pytest.raises(pymizu.ForeignPayloadError, match="R payload"):
                await f1
            assert await f2 == "c"

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_recv_batch_async_validates_n(ch):
    # n < 1 raises at the call site: nothing enrolled, nothing armed
    async def main():
        for bad in (0, -1):
            with pytest.raises(ValueError, match="at least 1"):
                ch.recv_batch_async(bad)
        assert ch._h._aio is None
        ch.send(1)
        assert await ch.recv_async() == 1

    asyncio.run(main())


def test_recv_batch_async_index_n(ch):
    # n ducks like the sync verb's: anything __index__-able
    class Index:
        def __init__(self, n):
            self.n = n

        def __index__(self):
            return self.n

    async def main():
        fut = ch.recv_batch_async(Index(2))
        ch.send_batch(["a", "b", "c"])
        assert await fut == ["a", "b"]
        assert await ch.recv_async() == "c"

    asyncio.run(main())


def test_recv_batch_async_on_destroyed_rolls_back(ch):
    # the arm failure unwinds the batch enrollment: nothing enrolled,
    # nothing armed, the counters back to zero
    ch.destroy()

    async def main():
        with pytest.raises(ValueError, match="closed"):
            ch.recv_batch_async(4)

    asyncio.run(main())
    st = ch._h._aio
    assert st is not None and not st.armed and not st.fifo
    assert (st.wants, st.batches) == (0, 0)


def test_recv_batch_async_armed_refuses_blocking_recv(ch):
    # a blocking recv from another thread while a batch waiter is armed
    # raises ConcurrentUseError at the call site (the kind-agnostic C
    # guard the verb relies on)
    async def main():
        fut = ch.recv_batch_async()
        err = []

        def block():
            try:
                ch.recv(timeout=1)
            except pymizu.ConcurrentUseError as e:
                err.append(e)

        t = threading.Thread(target=block)
        t.start()
        t.join()
        assert len(err) == 1
        fut.cancel()
        await asyncio.sleep(0)

    asyncio.run(main())


def test_recv_batch_async_quantum():
    # an n past the drain quantum resolves with exactly one quantum per
    # pass; the rest follows, and the burst cost one wake (clear-on-send)
    h = _pymizu._channel_new(4096, 256, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    p.ready_set()
    assert h.ready_wait(5)
    host, peer = pymizu.Channel._wrap(h), pymizu.Channel._wrap(p)
    n = _quantum_n()
    try:

        async def main():
            fut = host.recv_batch_async(n + 1000)
            assert peer.send_batch(list(range(n + 500))) == n + 500
            assert await fut == list(range(n))
            assert await host.recv_batch_async(1000) == list(range(n, n + 500))
            assert peer.info()["notify_sends"] == 1

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_channel_quantum_cap_reschedule():
    # a decline carrying the pass's work to exactly the quantum skips the
    # post-burst reschedule, so the loop-top cap reschedules instead —
    # the tail waiter resolving proves the next pass ran
    h = _pymizu._channel_new(4096, 256, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    p.ready_set()
    assert h.ready_wait(5)
    host, peer = pymizu.Channel._wrap(h), pymizu.Channel._wrap(p)
    n = _quantum_n()
    try:

        async def main():
            batch = host.recv_batch_async(n - 1)
            declined = host.recv_async()
            tail = host.recv_async()
            assert peer.send_batch(list(range(n - 1))) == n - 1
            _pymizu._channel_send_raw(peer._h, b"R")
            peer.send("tail")
            assert await batch == list(range(n - 1))
            with pytest.raises(pymizu.ForeignPayloadError, match="R payload"):
                await declined
            assert await tail == "tail"

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_iter_batches_async():
    # the async batch iterator yields the burst lists and ends on CLOSED
    host, peer = chan_pair()
    try:

        async def main():
            peer.send_batch(["a", "b", "c"])
            peer._close_signal()
            got = []
            async for batch in host.iter_batches_async():
                got.append(batch)
            assert got == [["a", "b", "c"]]

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_iter_batches_async_empty_on_close():
    # a close with nothing pending ends the iteration immediately
    host, peer = chan_pair()
    try:

        async def main():
            peer._close_signal()
            async for batch in host.iter_batches_async():
                pytest.fail(f"unexpected batch: {batch}")

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_iter_batches_async_foreign_payload_reentry():
    # a decline ends the iteration; re-entering on the same channel
    # resumes with the message after the (consumed) declined one
    host, peer = chan_pair()
    try:

        async def main():
            peer.send("a")
            _pymizu._channel_send_raw(peer._h, b"R")
            peer.send("c")
            peer._close_signal()
            got = []
            with pytest.raises(pymizu.ForeignPayloadError, match="R payload"):
                async for batch in host.iter_batches_async():
                    got.append(batch)
            assert got == [["a"]]
            async for batch in host.iter_batches_async():
                got.append(batch)
            assert got == [["a"], ["c"]]

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_iter_batches_async_validates_n(ch):
    # n < 1 surfaces from the underlying verb on first iteration
    async def main():
        with pytest.raises(ValueError, match="at least 1"):
            await anext(ch.iter_batches_async(0))

    asyncio.run(main())


def test_close_settles_pending_recv(ch):
    # a completed close settles pending recv futures with CLOSED
    async def main():
        fut = ch.recv_async()
        assert ch.close()
        assert await fut is pymizu.CLOSED

    asyncio.run(main())


def test_settle_tolerates_a_done_future():
    # the settle helper's contract: set_result/set_exception on an
    # already-settled waiter is a no-op
    from pymizu import _aio

    async def main():
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(1)
        _aio._settle(fut, 2)
        _aio._settle(fut, RuntimeError(), exc=True)
        assert fut.result() == 1

    asyncio.run(main())


def test_notify_arm_rejects_bad_args(ch, pool):
    with pytest.raises(TypeError):
        ch._h._notify_arm(None)
    with pytest.raises(TypeError):
        ch._h._notify_arm(None, 42)
    with pytest.raises(TypeError):
        pool._h._notify_arm(None)
    with pytest.raises(TypeError):
        pool._h._notify_arm(None, 42)


def test_aio_state_cannot_be_deleted(ch, pool):
    with pytest.raises(TypeError, match="cannot delete"):
        del ch._h._aio
    with pytest.raises(TypeError, match="cannot delete"):
        del pool._h._aio


def test_trampoline_skips_a_disarmed_drain():
    # a wake scheduled before a disarm fires after it: the skipped drain
    # leaves the waiter pending and the payload still receives
    host, peer = chan_pair()
    try:

        async def main():
            fut = host.recv_async()
            peer.send(1)
            host._h._notify_disarm()
            await asyncio.sleep(0.05)  # the queued trampoline runs, skipping
            assert not fut.done()
            fut.cancel()
            await asyncio.sleep(0)
            assert host.recv(timeout=5) == 1

        asyncio.run(main())
    finally:
        host.destroy()
        peer.destroy()


def test_channel_destroy_inside_direct_drain_defers():
    # a teardown landing inside the direct-mode drain (on the listener
    # thread) hands off to a detached thread rather than deadlocking the
    # watch stop on the watched callback
    host, peer = chan_pair()
    done = []
    try:
        host._h._notify_arm(None, lambda: (host.destroy(), done.append(True)))
        peer.send(1)
        deadline = time.monotonic() + 5
        while not done:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        with pytest.raises(ValueError, match="closed"):
            host.send(1)
    finally:
        host.destroy()
        peer.destroy()


def test_destroy_racing_an_in_flight_callback_does_not_deadlock():
    # the publish signals the armed endpoint; the listener callback claims
    # the wake and blocks in PyGILState_Ensure while the main thread, GIL
    # held by the spin, enters destroy -> watch stop, which waits for that
    # callback. The GIL release around the stop is the fix; a subprocess
    # under a timeout keeps a regression from hanging the suite.
    script = textwrap.dedent("""
        import time
        from pymizu import _pymizu

        for _ in range(20):
            h = _pymizu._channel_new(64, 256, 1 << 16, False, b"")
            p, _ = _pymizu._channel_attach(h.token)
            p.ready_set()
            assert h.ready_wait(5.0)
            h._notify_arm(None, lambda: None)  # direct mode: cb takes the GIL
            p.send(1)                          # signals the armed endpoint
            t0 = time.perf_counter()
            while time.perf_counter() - t0 < 0.002:
                pass                           # GIL held, no I/O
            h.destroy()
            p.destroy()
        print("ok", flush=True)
    """)
    res = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert res.returncode == 0, res.stderr
    assert "ok" in res.stdout


# -- pool: tasks ------------------------------------------------------------


def test_await_task(pool):
    async def main():
        task = pool.submit(square, 8)
        assert await task == 64

    asyncio.run(main())


def test_collect_async(pool):
    async def main():
        task = pool.submit(square, 3)
        assert await task.collect_async() == 9

    asyncio.run(main())


def test_await_task_error(pool):
    # a task error rejects with the flattened remote error
    async def main():
        task = pool.submit(fail_at, 4, 4)
        with pytest.raises(pymizu.TaskError):
            await task

    asyncio.run(main())


def test_collect_async_cancel_restores_collectibility(pool):
    # cancelling the Future un-stores the announce: a later blocking
    # collect works (and the task is never cancelled)
    async def main():
        task = pool.submit(sleep_ident, 0.3)
        fut = task.collect_async()
        fut.cancel()
        await asyncio.sleep(0)  # the detach callback runs
        assert task.collect(timeout=5) == 0.3

    asyncio.run(main())


def test_blocking_collect_refuses_armed_task(pool):
    async def main():
        task = pool.submit(square, 5)
        fut = task.collect_async()
        with pytest.raises(pymizu.MizuError, match="one consumer per result"):
            task.collect(timeout=0)
        with pytest.raises(pymizu.MizuError, match="one consumer per result"):
            pool.collect_all([task], timeout=0)
        with pytest.raises(pymizu.MizuError, match="one consumer per result"):
            task.collect_async()  # a second enrollment
        assert await fut == 25

    asyncio.run(main())


def test_failed_collection_enroll_keeps_existing_consumer(pool):
    # a conflicting collection enrollment is all-or-nothing: it must not
    # disturb the conflicting task's existing consumer (the old per-task
    # rollback cleared every task's flag, including that legitimate one)
    async def main():
        t1 = pool.submit(square, 5)
        t2 = pool.submit(square, 6)
        fut = t1.collect_async()
        with pytest.raises(pymizu.MizuError, match="one consumer per result"):
            pool.collect_all_async([t2, t1])
        # t1's consumer survived: blocking collects still refuse it
        with pytest.raises(pymizu.MizuError, match="one consumer per result"):
            t1.collect(timeout=0)
        # t2 was not left flagged by the failed enrollment
        assert t2.collect(timeout=5) == 36
        assert await fut == 25

    asyncio.run(main())


def test_tasks_enroll_validates_pool_membership(pool):
    other = pymizu.Pool.create(1)
    try:
        task = other.submit(square, 5)
        with pytest.raises(ValueError, match="belong"):
            pool._h._tasks_enroll([task])
        assert task.collect(timeout=5) == 25  # untouched by the failure
    finally:
        other.destroy()


def test_pool_destroy_settles():
    p = pymizu.Pool.create(1)

    async def main():
        task = p.submit(sleep_ident, 0.5)
        fut = task.collect_async()
        await asyncio.sleep(0)
        p.destroy()
        with pytest.raises(ValueError, match="closed"):
            await fut
        assert not p._h._aio.armed  # the teardown drops the sticky arm

    try:
        asyncio.run(main())
    finally:
        p.destroy()


def test_pool_pump_error_fails_waiters():
    # a handle broken under an armed pump fails the waiters with the
    # closed-handle error
    p = pymizu.Pool.create(1)
    try:

        async def main():
            task = p.submit(sleep_ident, 0.5)
            fut = task.collect_async()
            p._h.destroy()
            p._h._aio._pump()
            with pytest.raises(ValueError, match="closed"):
                await fut

        asyncio.run(main())
    finally:
        p.destroy()


def test_single_cancel_at_settle_detaches_late(pool):
    # a future cancelled as its wake is queued: the pump skips the
    # resolve, the late detach no-ops, and the result stays collectible
    async def main():
        task = pool.submit(sleep_ident, 0.2)
        fut = task.collect_async()
        deadline = time.monotonic() + 5
        while not pool._h._tasks_terminal([task])[0]:
            assert time.monotonic() < deadline
            time.sleep(0.001)  # hold the loop: the wake queues behind it
        time.sleep(0.05)  # the wake's trampoline is queued
        fut.cancel()
        await asyncio.sleep(0.05)
        assert fut.cancelled()
        assert task.collect(timeout=5) == 0.2

    asyncio.run(main())


def test_collect_async_on_destroyed_pool_rolls_back():
    # an enrollment failing at the announce leaves no flag behind: the
    # next attempt raises the same way
    p = pymizu.Pool.create(1)
    task = p.submit(square, 2)
    p.destroy()
    try:

        async def main():
            with pytest.raises(ValueError, match="closed"):
                task.collect_async()
            with pytest.raises(ValueError, match="closed"):
                task.collect_async()  # no flag left behind

        asyncio.run(main())
    finally:
        p.destroy()


def test_collect_async_arm_failure_rolls_back(pool, monkeypatch):
    # the pending half's arm failure unwinds the enrollment (flag and
    # announce restored): the task is blocking-collectible again
    from pymizu import _aio

    def boom(st, loop):
        raise RuntimeError("arm failed")

    monkeypatch.setattr(_aio, "_arm", boom)

    async def main():
        task = pool.submit(sleep_ident, 0.3)
        with pytest.raises(RuntimeError, match="arm failed"):
            task.collect_async()
        assert task.collect(timeout=5) == 0.3

    asyncio.run(main())


def test_collect_async_terminal_error_rejects(pool):
    # a task that already failed settles through the C fast path as
    # set_exception, never arming
    async def main():
        task = pool.submit(fail_at, 4, 4)
        deadline = time.monotonic() + 5
        while not pool._h._tasks_terminal([task])[0]:
            assert time.monotonic() < deadline
            time.sleep(0.001)
        fut = task.collect_async()
        assert fut.done()
        with pytest.raises(pymizu.TaskError):
            await fut

    asyncio.run(main())


def test_waiter_future_does_not_pin_pool():
    # the C registry is GC-traversed: once the closed loop bounces the
    # wake (the C disarm drops the drain refs), a waiter future whose
    # callback closes over the pool does not pin it
    import gc
    import weakref

    def make():
        pool = pymizu.Pool.create(1)

        async def enroll():
            task = pool.submit(sleep_ident, 0.2)
            fut = task.collect_async()
            fut.add_done_callback(lambda f: pool)

        asyncio.run(enroll())
        return weakref.ref(pool)

    ref = make()
    time.sleep(0.5)  # the publish bounces off the closed loop: C disarm
    deadline = time.monotonic() + 5
    while ref() is not None:
        gc.collect()
        assert time.monotonic() < deadline
        time.sleep(0.05)


def test_terminal_settle_needs_no_arm(pool):
    # an already-published task settles at the call site: the notify
    # record's arm count never moves
    from pymizu import _aio

    real_h = pool._h

    class Rec:
        arm_calls = 0

        def _notify_arm(self, loop, cb):
            self.arm_calls += 1
            return real_h._notify_arm(loop, cb)

        def __getattr__(self, name):
            return getattr(real_h, name)

    async def main():
        rec = Rec()
        pool._h._aio = _aio._PoolState(rec)
        task = pool.submit(square, 5)
        deadline = time.monotonic() + 5
        while not pool._h._tasks_terminal([task])[0]:
            assert time.monotonic() < deadline
            time.sleep(0.001)
        fut = task.collect_async()
        assert fut.done()
        assert fut.result() == 25
        assert rec.arm_calls == 0
        assert not pool._h._aio.armed

    asyncio.run(main())


def test_arm_survives_settle_and_detach(pool):
    # the sticky arm: a settled waiter leaves it standing for the next
    # enrollment to reuse; a detach never disarms; the next empty wake does
    from pymizu import _aio

    real_h = pool._h

    class Rec:
        arm_calls = 0

        def _notify_arm(self, loop, cb):
            self.arm_calls += 1
            return real_h._notify_arm(loop, cb)

        def __getattr__(self, name):
            return getattr(real_h, name)

    async def main():
        rec = Rec()
        pool._h._aio = _aio._PoolState(rec)
        t1 = pool.submit(sleep_ident, 0.2)
        assert await t1.collect_async() == 0.2
        assert pool._h._aio.armed  # the emptied set kept the arm
        t2 = pool.submit(sleep_ident, 0.2)
        f2 = t2.collect_async()
        assert rec.arm_calls == 1  # reused, never re-armed
        f2.cancel()
        await asyncio.sleep(0)
        assert pool._h._aio.armed  # a detach is not a disarm
        pool._h._aio._pump()  # the empty wake: the disarm lands here
        assert not pool._h._aio.armed
        assert t2.collect(timeout=5) == 0.2

    asyncio.run(main())


def test_enrollment_never_scans_the_enrolled_set(pool):
    # k pending singles: every enrollment's probe and post-arm re-check
    # are one-word C reads — no scan of the enrolled set is ever built
    from pymizu import _aio

    sizes = []
    real_h = pool._h

    class Rec:
        def _tasks_terminal_set(self, tasks):
            sizes.append(len(tasks))
            return real_h._tasks_terminal_set(tasks)

        def __getattr__(self, name):
            return getattr(real_h, name)

    async def main():
        pool._h._aio = _aio._PoolState(Rec())
        tasks = [pool.submit(sleep_ident, 0.3) for _ in range(8)]
        futs = [t.collect_async() for t in tasks]
        assert sizes == []
        assert await asyncio.gather(*futs) == [0.3] * 8
        assert sizes == []

    asyncio.run(main())


def test_blocking_collect_beside_an_armed_pool(pool):
    # a non-enrolled task's blocking collect works while the pool is armed
    async def main():
        t1 = pool.submit(sleep_ident, 0.3)
        fut = t1.collect_async()
        t2 = pool.submit(square, 4)
        assert t2.collect(timeout=5) == 16
        assert await fut == 0.3

    asyncio.run(main())


def test_task_verbs_reject_foreign_pool_tasks():
    h1 = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    h2 = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        p2 = pymizu.Pool._wrap(h2)
        task = p2.submit(square, 2)  # queued: no worker ever claims it
        with pytest.raises(TypeError):
            h1._task_notify([task])
        with pytest.raises(ValueError, match="belong to this pool"):
            h1._task_notify([task], True)
        with pytest.raises(ValueError, match="belong to this pool"):
            h1._tasks_terminal([task])
        h2.destroy()
        with pytest.raises(ValueError, match="closed"):
            h2._tasks_terminal([task])
    finally:
        h1.destroy()
        h2.destroy()


# -- pool: collections ------------------------------------------------------


def test_collect_any_async(pool):
    async def main():
        # the 0.5s spacing keeps the winner deterministic on loaded runners
        tasks = [pool.submit(sleep_ident, 0.5 * (i + 1)) for i in range(3)]
        idx, val = await pool.collect_any_async(tasks)
        assert idx == 0
        assert abs(val - 0.5) < 1e-9
        # the losers stay collectible
        rest = [t.collect(timeout=5) for t in tasks[1:]]
        assert rest[0] == pytest.approx(1.0)
        assert rest[1] == pytest.approx(1.5)

    asyncio.run(main())


def test_collect_all_async(pool):
    async def main():
        tasks = [pool.submit(square, i) for i in range(6)]
        assert await pool.collect_all_async(tasks) == [i * i for i in range(6)]

    asyncio.run(main())


def test_collect_all_async_one_future(pool):
    # one Future per collection, never one per task
    async def main():
        tasks = [pool.submit(square, i) for i in range(8)]
        fut = pool.collect_all_async(tasks)
        st = pool._h._aio
        assert len(st.collections) == 1
        assert pool._h._pump_singles(0) == (0, 0)  # never one per task
        assert st.collections[0].fut is fut
        assert await fut == [i * i for i in range(8)]

    asyncio.run(main())


def test_collect_all_async_error_index(pool):
    # the first erroring handle by position rejects with `index`; only
    # that handle is consumed, the rest stay collectible
    async def main():
        tasks = [
            pool.submit(square, 1),
            pool.submit(fail_at, 2, 2),
            pool.submit(square, 3),
        ]
        with pytest.raises(pymizu.TaskError) as exc_info:
            await pool.collect_all_async(tasks)
        assert exc_info.value.index == 1
        assert tasks[0].collect(timeout=5) == 1
        assert tasks[2].collect(timeout=5) == 9

    asyncio.run(main())


def test_collect_any_async_error_index(pool):
    # the first terminal is the erroring one: rejects with its position
    async def main():
        tasks = [pool.submit(sleep_ident, 0.5), pool.submit(fail_at, 0, 0)]
        with pytest.raises(pymizu.TaskError) as exc_info:
            await pool.collect_any_async(tasks)
        assert exc_info.value.index == 1

    asyncio.run(main())


def test_collection_cancel_unenrolls(pool):
    # cancelling the collection's Future un-enrolls every task: each is
    # blocking-collectible again, and nothing is cancelled
    async def main():
        tasks = [pool.submit(sleep_ident, 0.2) for _ in range(3)]
        fut = pool.collect_all_async(tasks)
        fut.cancel()
        await asyncio.sleep(0)
        assert [t.collect(timeout=5) for t in tasks] == [0.2, 0.2, 0.2]

    asyncio.run(main())


def test_reenroll_on_a_fresh_loop_rechecks(pool, monkeypatch):
    # a collection enrolled under a loop re-capture must re-check: its
    # members can publish while the record still points at the previous
    # loop's closed call_soon_threadsafe, where their wake ends (the C
    # callback's RuntimeError disarm) — skipping the re-check on the
    # strength of the stale standing arm waits forever. The patched arm
    # widens that window to a certainty.
    from pymizu import _aio

    async def first():
        t = pool.submit(square, 1)
        assert await pool.collect_all_async([t]) == [1]

    asyncio.run(first())  # the sticky arm now rides a closed loop

    real_arm = _aio._arm

    def slow_arm(st, loop):
        # the enrollment gap: the members publish, the watch fires, the
        # closed-loop RuntimeError disarm eats the wake
        time.sleep(0.5)
        return real_arm(st, loop)

    monkeypatch.setattr(_aio, "_arm", slow_arm)

    async def second():
        tasks = [pool.submit(sleep_ident, 0.1) for _ in range(2)]
        fut = pool.collect_all_async(tasks)
        assert await asyncio.wait_for(fut, 5) == [0.1, 0.1]

    asyncio.run(second())


def test_wake_to_a_closed_loop_does_not_brick(pool):
    # a wake scheduled against a closed loop fails its call_soon_threadsafe
    # with no trampoline ever running: the coalesce flag must release with
    # it, or every later signal is absorbed as already-scheduled and the
    # next loop's waiters hang. The publish below fires exactly that wake;
    # second() settles only if a fresh signal still pumps.
    async def first():
        t = pool.submit(sleep_ident, 0.1)
        assert await pool.collect_all_async([t]) == [0.1]

    asyncio.run(first())  # a pump settle leaves the sticky word standing

    t = pool.submit(sleep_ident, 0.1)
    pool._h._task_notify([t], 1)  # announced: its publish signals the
    time.sleep(0.3)  # endpoint on the closed loop before a blocking
    assert t.collect(timeout=5) == 0.1  # collect swaps the announce

    async def second():
        tasks = [pool.submit(sleep_ident, 0.2) for _ in range(2)]
        fut = pool.collect_all_async(tasks)
        assert await asyncio.wait_for(fut, 5) == [0.2, 0.2]

    asyncio.run(second())


def test_collect_async_validation(pool):
    async def main():
        with pytest.raises(ValueError, match="non-empty"):
            pool.collect_any_async([])
        with pytest.raises(ValueError, match="non-empty"):
            pool.collect_all_async([])
        p2 = pymizu.Pool.create(1)
        try:
            foreign = p2.submit(square, 2)
            with pytest.raises(ValueError, match="belong to this pool"):
                pool.collect_all_async([foreign])
            assert foreign.collect(timeout=5) == 4
        finally:
            p2.stop()

    asyncio.run(main())


def test_collection_enrollment_conflict_rolls_back(pool):
    # a collection enrolling over an enrolled single raises and restores
    # the other members' flags
    async def main():
        t1 = pool.submit(sleep_ident, 0.3)
        t2 = pool.submit(square, 2)
        fut = t1.collect_async()
        with pytest.raises(pymizu.MizuError, match="one consumer per result"):
            pool.collect_all_async([t2, t1])
        assert await fut == 0.3
        assert t2.collect(timeout=5) == 4

    asyncio.run(main())


def test_collection_cancel_at_settle_detaches_late(pool):
    # a collection future cancelled as its wake is queued: the pump's
    # settle skips it, the late detach no-ops, the task stays collectible
    async def main():
        task = pool.submit(sleep_ident, 0.2)
        fut = pool.collect_all_async([task])
        deadline = time.monotonic() + 5
        while not pool._h._tasks_terminal([task])[0]:
            assert time.monotonic() < deadline
            time.sleep(0.001)  # hold the loop: the wake queues behind it
        time.sleep(0.05)  # the wake's trampoline is queued
        fut.cancel()
        await asyncio.sleep(0.05)
        assert fut.cancelled()
        assert task.collect(timeout=5) == 0.2

    asyncio.run(main())


def test_pool_loop_change_fails_collection(pool):
    # arming on a fresh loop after the old one closed fails the old
    # loop's waiters with the loop-change error, collections included
    holder = []

    async def first():
        task = pool.submit(sleep_ident, 1)
        holder.append(pool.collect_all_async([task]))

    asyncio.run(first())

    async def second():
        task = pool.submit(square, 2)
        assert await task.collect_async() == 4

    asyncio.run(second())
    assert isinstance(holder[0].exception(), RuntimeError)


def test_pool_destroy_settles_a_loop_less_state():
    # an enrollment that failed before any arm leaves async state with no
    # captured loop: the teardown settles in place
    p = pymizu.Pool.create(1)
    try:

        async def main():
            task = p.submit(sleep_ident, 0.3)
            task._async_set(True)  # a consumer outside the async state
            with pytest.raises(pymizu.MizuError, match="one consumer"):
                p.collect_all_async([task])
            p.destroy()

        asyncio.run(main())
    finally:
        p.destroy()


# -- pool: the map handle ---------------------------------------------------


def test_map_await(pool):
    async def main():
        assert await pool.map(square, range(10)) == [i * i for i in range(10)]

    asyncio.run(main())


def test_map_collect_async(pool):
    async def main():
        m = pool.map(square, range(4))
        assert await m.collect_async() == [0, 1, 4, 9]

    asyncio.run(main())


def test_map_collect_async_numpy_scalars(pool):
    # the typed map-result stream decodes on the same collect path
    np = pytest.importorskip("numpy")

    async def main():
        out = await pool.map(np_f64, list(range(200)))
        assert all(type(v) is np.float64 for v in out)
        assert out == [v * 2 for v in range(200)]

    asyncio.run(main())


def test_map_collect_async_error(pool):
    # a runner error rejects with the failing element's index
    async def main():
        m = pool.map(fail_at, [1, 2, 3, 3], args=(3,))
        with pytest.raises(pymizu.TaskError) as exc_info:
            await m
        assert exc_info.value.index == 2

    asyncio.run(main())


def test_map_collect_async_cancel_detaches(pool):
    # cancelling the Future detaches: the handle collects blocking again
    async def main():
        m = pool.map(square, range(4))
        fut = m.collect_async()
        fut.cancel()
        await asyncio.sleep(0)
        assert m.collect() == [0, 1, 4, 9]

    asyncio.run(main())


def test_map_promised_refuses_blocking_collect(pool):
    async def main():
        m = pool.map(square, range(4))
        fut = m.collect_async()
        with pytest.raises(pymizu.MizuError, match="one consumer per result"):
            m.collect(timeout=0)
        assert await fut == [0, 1, 4, 9]

    asyncio.run(main())


def test_map_stream_refuses_collect_async(pool):
    async def main():
        m = pool.map(square, range(4), stream=True)
        with pytest.raises(pymizu.MizuError, match="blocking"):
            m.collect_async()
        assert m.collect() == [0, 1, 4, 9]

    asyncio.run(main())


def test_map_cancel_then_collect(pool):
    m = pool.map(sleep_ident, [0.5] * 4)
    m.cancel()
    with pytest.raises(pymizu.CancelledError, match="cancelled"):
        m.collect(timeout=5)


def test_map_cancel_rejects_future(pool):
    async def main():
        m = pool.map(sleep_ident, [0.5] * 4)
        fut = m.collect_async()
        await asyncio.sleep(0.1)  # the runners are in flight
        m.cancel()
        with pytest.raises(pymizu.CancelledError, match="cancelled"):
            await fut

    asyncio.run(main())


def test_map_cancel_rejects_future_with_queued_runners(pool):
    # cancelled with its runners still queued behind the blockers: every
    # runner abandons and nothing splices — the reject crosses the
    # cancelled gather, never a runner's result
    async def main():
        blockers = [pool.submit(time.sleep, 2) for _ in range(2)]
        m = pool.map(
            sleep_on, list(range(2000)), args=(set(range(2000)), 0.05)
        )
        fut = m.collect_async()
        m.cancel()
        with pytest.raises(pymizu.CancelledError, match="cancelled"):
            await fut
        assert [b.collect(timeout=5) for b in blockers] == [None, None]

    asyncio.run(main())


def test_map_empty_collect_async(pool):
    async def main():
        assert await pool.map(square, []) == []

    asyncio.run(main())


def test_map_collect_async_after_collect_raises(pool):
    async def main():
        m = pool.map(square, range(4))
        assert m.collect() == [0, 1, 4, 9]
        with pytest.raises(pymizu.MizuError, match="already collected"):
            m.collect_async()

    asyncio.run(main())


def test_map_collect_async_twice_raises(pool):
    async def main():
        m = pool.map(sleep_ident, [0.2] * 4)
        fut = m.collect_async()
        with pytest.raises(pymizu.MizuError, match="one consumer per result"):
            m.collect_async()
        assert await fut == [0.2] * 4

    asyncio.run(main())


def test_map_collect_async_after_cancel_raises(pool):
    async def main():
        m = pool.map(square, range(4))
        m.cancel()
        with pytest.raises(pymizu.CancelledError, match="cancelled"):
            m.collect_async()

    asyncio.run(main())


def test_map_collect_async_on_destroyed_pool_raises():
    # the enrollment error restores the promised flag
    p = pymizu.Pool.create(1)
    m = p.map(sleep_ident, [0.5] * 4)
    p.destroy()
    try:

        async def main():
            with pytest.raises(ValueError, match="closed"):
                m.collect_async()
            assert not m._promised

        asyncio.run(main())
    finally:
        p.destroy()


def test_pool_edge_trigger():
    # several tasks publishing to one armed submitter send one signal
    # per burst (the submitter word is clear-on-send)
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    wk = _pymizu._pool_worker_join(h.token, 0)
    th = None
    try:
        pool = pymizu.Pool._wrap(h)

        async def main():
            nonlocal th
            tasks = [pool.submit(square, i) for i in range(8)]
            fut = pool.collect_all_async(tasks)  # announce + arm first
            # the worker executes only now: every publish is announced
            th = threading.Thread(target=wk.run, daemon=True)
            th.start()
            # hold the loop: every publish lands ahead of the first drain
            deadline = time.monotonic() + 5
            while any(t.state in ("pending", "running") for t in tasks):
                assert time.monotonic() < deadline
                time.sleep(0.001)
            assert wk.dump()["local"]["notify_sends"] == 1
            assert await fut == [i * i for i in range(8)]
            # a busy worker publishes only after the announces land
            blocker = pool.submit(sleep_ident, 0.3)
            tasks = [pool.submit(square, i) for i in range(4)]
            fut = pool.collect_all_async(tasks)
            deadline = time.monotonic() + 5
            while any(t.state in ("pending", "running") for t in tasks):
                assert time.monotonic() < deadline
                time.sleep(0.001)
            assert wk.dump()["local"]["notify_sends"] == 2
            assert await fut == [i * i for i in range(4)]
            assert blocker.collect(timeout=5) == 0.3

        asyncio.run(main())
    finally:
        h.destroy()  # the shutdown broadcast breaks the worker's park
        if th is not None:
            th.join(timeout=5)
            assert not th.is_alive()
        wk.destroy()


def test_pool_signal_coalesces_into_one_wake():
    # the submitter word's two armed transitions while a wake is queued
    # collapse to one scheduled pump: nothing is lost
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    wk = _pymizu._pool_worker_join(h.token, 0)
    th = None
    try:
        pool = pymizu.Pool._wrap(h)

        async def main():
            nonlocal th
            t1 = pool.submit(square, 1)
            f1 = t1.collect_async()
            th = threading.Thread(target=wk.run, daemon=True)
            th.start()
            deadline = time.monotonic() + 5
            while t1.state in ("pending", "running"):
                assert time.monotonic() < deadline
                time.sleep(0.001)  # hold the loop: the wake queues
            time.sleep(0.05)  # the wake is queued behind the busy loop
            pool._h._notify_rearm()  # re-arm ahead of the pump
            blocker = pool.submit(sleep_ident, 0.5)  # occupy the worker
            t2 = pool.submit(square, 2)
            f2 = t2.collect_async()  # its announce lands while it sleeps
            deadline = time.monotonic() + 5
            while t2.state in ("pending", "running"):
                assert time.monotonic() < deadline
                time.sleep(0.001)
            time.sleep(0.05)  # the second wake coalesces into the first
            assert await f1 == 1
            assert await f2 == 4
            assert wk.dump()["local"]["notify_sends"] == 2
            assert blocker.collect(timeout=5) == 0.5

        asyncio.run(main())
    finally:
        h.destroy()  # the shutdown broadcast breaks the worker's park
        if th is not None:
            th.join(timeout=5)
            assert not th.is_alive()
        wk.destroy()


def test_pool_pump_quantum_split():
    # a terminal burst over the drain quantum splits the pump across
    # callbacks: order preserved, zero loss, the loop not starved
    n = _quantum_n() + 1
    h = _pymizu._pool_new(1, 1, 4096, 64, 4096, 512)
    wk = _pymizu._pool_worker_join(h.token, 0)
    th = None
    try:
        pool = pymizu.Pool._wrap(h)

        async def main():
            nonlocal th
            tasks = [pool.submit(square, i) for i in range(n)]
            futs = [t.collect_async() for t in tasks]
            th = threading.Thread(target=wk.run, daemon=True)
            th.start()
            deadline = time.monotonic() + 30
            while any(t.state in ("pending", "running") for t in tasks):
                assert time.monotonic() < deadline
                time.sleep(0.001)
            probe = []
            asyncio.get_running_loop().call_soon(probe.append, True)
            assert await asyncio.gather(*futs) == [i * i for i in range(n)]
            assert probe == [True]

        asyncio.run(main())
    finally:
        h.destroy()  # the shutdown broadcast breaks the worker's park
        if th is not None:
            th.join(timeout=5)
            assert not th.is_alive()
        wk.destroy()


def test_collection_pump_quantum_split():
    # the same split on the pump's collection pass
    n = _quantum_n() + 1
    h = _pymizu._pool_new(1, 1, 4096, 64, 4096, 512)
    wk = _pymizu._pool_worker_join(h.token, 0)
    th = None
    try:
        pool = pymizu.Pool._wrap(h)

        async def main():
            nonlocal th
            tasks = [pool.submit(square, i) for i in range(n)]
            futs = [pool.collect_all_async([t]) for t in tasks]
            th = threading.Thread(target=wk.run, daemon=True)
            th.start()
            deadline = time.monotonic() + 30
            while any(t.state in ("pending", "running") for t in tasks):
                assert time.monotonic() < deadline
                time.sleep(0.001)
            assert await asyncio.gather(*futs) == [[i * i] for i in range(n)]

        asyncio.run(main())
    finally:
        h.destroy()  # the shutdown broadcast breaks the worker's park
        if th is not None:
            th.join(timeout=5)
            assert not th.is_alive()
        wk.destroy()
