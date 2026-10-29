"""The threading contract: one thread per handle role at a time.

Concurrent verbs within one role raise ConcurrentUseError (a check, not
a lock) instead of racing the core's single-writer words — the failure
modes the guard replaced were silent: torn ring entries, aliased result
slots, duplicated or dropped channel messages.
"""

import asyncio
import threading
import time

import pytest
from tests.helpers import busy, identity, slow

import pymizu
from pymizu import _pymizu

PAYLOAD = b"x" * (1 << 20)  # 1 MB: a staging window wide enough for overlap


def test_channel_concurrent_verbs_raise():
    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    try:
        received = []
        parked = threading.Thread(target=lambda: received.append(h.recv()))
        parked.start()
        time.sleep(0.1)  # the recv parks inside the guard
        with pytest.raises(pymizu.ConcurrentUseError):
            h.send(b"x")
        with pytest.raises(pymizu.ConcurrentUseError):
            h.recv(0)
        with pytest.raises(pymizu.ConcurrentUseError):
            h.recv_batch(1, 0)
        p.send(1)  # the parked recv still works after the guard
        parked.join(timeout=5)
        assert received == [1]
    finally:
        h.destroy()
        p.destroy()


class _SlowStage:
    """Staging parks in __reduce__ (GIL released), holding the side's
    guard long enough for a second verb to trip deterministically."""

    def __reduce__(self):
        time.sleep(0.3)
        return (int, (1,))


def test_channel_concurrent_sends_raise():
    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    try:
        sender = threading.Thread(target=lambda: h.send(_SlowStage()))
        sender.start()
        time.sleep(0.1)  # the slow stage is mid-send inside the guard
        with pytest.raises(pymizu.ConcurrentUseError):
            h.send(1)
        sender.join(timeout=5)
        assert not sender.is_alive()
        assert p.recv(timeout=5) == 1  # the slow send landed intact
    finally:
        h.destroy()
        p.destroy()


def _capture(fn, *args):
    try:
        fn(*args)
        return "?"
    except ValueError:
        return "closed"


def test_pool_concurrent_submit_raises():
    # injection_cap 2 with no workers: the third submit parks on ring
    # space holding the producer guard
    pool = _pymizu._pool_new(1, 1, 2, 8, 64, 512)
    pool.submit(None)
    pool.submit(None)
    blocked = []
    parked = threading.Thread(
        target=lambda: blocked.append(_capture(pool.submit, None))
    )
    parked.start()
    time.sleep(0.1)  # the submit parks inside the guard
    try:
        with pytest.raises(pymizu.ConcurrentUseError):
            pool.submit(None)
        with pytest.raises(pymizu.ConcurrentUseError):
            pool.submit_batch([lambda: None])
    finally:
        pool.destroy()
        parked.join(timeout=5)
    assert not parked.is_alive()
    assert blocked == ["closed"]  # the parked submit broke out cleanly


def test_pool_concurrent_submit_collect_stays_loud_and_exact():
    # the shiny-bench P3 repro: threads submit+collect on ONE handle.
    # Overlaps raise ConcurrentUseError; every accepted task collects
    # its own value; nothing hangs.
    n_threads, iters = 4, 20
    barrier = threading.Barrier(n_threads)
    outcomes = []
    lock = threading.Lock()

    def drive(tid, pool):
        barrier.wait()
        for i in range(iters):
            try:
                t = pool.submit(identity, (tid, i, PAYLOAD))
                v = t.collect(timeout=30)
                outcome = ("ok", v[0] == tid and v[1] == i)
            except pymizu.ConcurrentUseError:
                outcome = ("concurrent", None)
            except BaseException as e:
                outcome = ("error", repr(e))
            with lock:
                outcomes.append(outcome)

    with pymizu.Pool.create(2) as pool:
        threads = [
            threading.Thread(target=drive, args=(tid, pool))
            for tid in range(n_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)
            assert not t.is_alive()

    oks = [o for o in outcomes if o[0] == "ok"]
    errors = [o for o in outcomes if o[0] == "error"]
    concurrent = [o for o in outcomes if o[0] == "concurrent"]
    assert concurrent, "expected the producer guard to fire"
    assert not errors, errors[:3]
    assert all(o[1] for o in oks)  # no misdelivered results
    assert oks, "expected some serialized submits to succeed"


def test_channel_ready_wait_holds_the_side():
    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    waiting = []
    parked = threading.Thread(
        target=lambda: waiting.append(h.ready_wait(None))
    )
    parked.start()
    time.sleep(0.1)  # the ready_wait parks inside the guard
    try:
        with pytest.raises(pymizu.ConcurrentUseError):
            h.recv(0)
        with pytest.raises(pymizu.ConcurrentUseError):
            h.send(b"x")
    finally:
        h.destroy()
        parked.join(timeout=5)
    assert not parked.is_alive()
    assert waiting == [False]  # the mid-wait destroy broke it out


def test_pool_concurrent_collects_raise():
    with pymizu.Pool.create(1) as pool:
        tasks = [pool.submit(slow, i) for i in range(2)]
        parked = threading.Thread(target=lambda: pool.collect_all(tasks, None))
        parked.start()
        time.sleep(0.1)  # the collect_all parks inside the waiter guard
        t = pool.submit(identity, 1)
        with pytest.raises(pymizu.ConcurrentUseError):
            t.collect(0.1)
        with pytest.raises(pymizu.ConcurrentUseError):
            pool.collect_any([t], 0.1)
        with pytest.raises(pymizu.ConcurrentUseError):
            pool.stop(0.1)
        # a zero-timeout poll never parks: unguarded by design (the
        # executor's drain settles with collect(0) alongside an outside
        # blocking collect)
        assert t.collect(0) is pymizu.TIMEOUT
        parked.join(timeout=30)
        assert not parked.is_alive()
        assert t.collect(timeout=5) == 1  # the freed guard collects fine


def test_pool_concurrent_ready_wait_raises():
    pool = _pymizu._pool_new(1, 1, 2, 8, 64, 512)
    waiting = []
    parked = threading.Thread(
        target=lambda: waiting.append(pool.ready_wait([0], None))
    )
    parked.start()
    time.sleep(0.1)  # the ready_wait parks inside the waiter guard
    try:
        with pytest.raises(pymizu.ConcurrentUseError):
            pool.ready_wait([0], 0.1)
        t = pool.submit(None)  # the producer half is a separate role
        with pytest.raises(pymizu.ConcurrentUseError):
            t.collect(0.1)
        assert pool.ready_wait([0], 0) is False  # a poll stays unguarded
    finally:
        pool.destroy()
        parked.join(timeout=5)
    assert not parked.is_alive()
    assert waiting == [False]


def test_pool_producer_and_waiter_on_two_threads():
    # the halves are separate roles: a parked collect never trips a
    # concurrent submit's guard (libmizu 'Handle threading')
    errors = []
    with pymizu.Pool.create(1) as pool:
        tasks = [pool.submit(busy, i) for i in range(6)]

        def produce():
            try:
                for i in range(6, 24):
                    tasks.append(pool.submit(busy, i))
                    time.sleep(0.02)  # stay producing into the collects
            except BaseException as e:
                errors.append(e)

        producer = threading.Thread(target=produce)
        producer.start()
        values = []
        for i in range(24):
            while len(tasks) <= i:
                time.sleep(0.001)
            values.append(tasks[i].collect(timeout=30))
        producer.join(timeout=30)
    assert not errors
    assert values == [i * 2 for i in range(24)]


def _chan_pair():
    """An in-process channel pair (host, peer) as facade Channels."""
    h = _pymizu._channel_new(64, 256, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    p.ready_set()
    assert h.ready_wait(5)
    return pymizu.Channel._wrap(h), pymizu.Channel._wrap(p)


def test_blocking_recv_while_async_armed_raises():
    h, p = _chan_pair()
    try:

        async def main():
            fut = h.recv_async()  # arms synchronously
            # on the loop thread a blocking receive stays legal: the
            # drain can never run concurrently with it
            p.send(40)
            assert h.recv(5) == 40

            def off_loop():
                with pytest.raises(pymizu.ConcurrentUseError):
                    h.recv(0)
                with pytest.raises(pymizu.ConcurrentUseError):
                    h.recv_batch(1, 0)

            await asyncio.to_thread(off_loop)
            p.send(41)
            assert await fut == 41
            p.send(42)
            assert h.recv(5) == 42  # disarmed at the settle: blocking works

        asyncio.run(main())
    finally:
        h.destroy()


def test_arm_thread_follows_a_loop_change():
    # a pending recv_async abandoned at loop close, then a re-arm from a
    # new loop on another thread: the arm thread must move with it, or
    # the blocking-recv check refuses the new loop's thread
    h, p = _chan_pair()
    holder = []
    idents = []
    release = threading.Event()

    async def arm():
        holder.append(h.recv_async())

    def t1_main():
        asyncio.run(arm())
        idents.append(threading.get_ident())
        release.wait(5)  # keep the ident alive (it recycles on thread exit)

    t1 = threading.Thread(target=t1_main)
    t1.start()
    while not idents:
        time.sleep(0.01)
    try:

        async def rearm():
            assert threading.get_ident() != idents[0]  # the premise
            h.recv_async()  # the loop change re-arms from this thread
            p.send(1)
            assert h.recv(5) == 1  # the drain's own thread: still legal

            def off_loop():
                with pytest.raises(pymizu.ConcurrentUseError):
                    h.recv(0)

            await asyncio.to_thread(off_loop)

        errs = []

        def run_rearm():
            try:
                asyncio.run(rearm())
            except BaseException as e:
                errs.append(e)

        t2 = threading.Thread(target=run_rearm)
        t2.start()
        t2.join(timeout=5)
        assert not t2.is_alive()
        assert not errs
        # the stale waiter failed with the loop-change error
        assert holder[0].exception() is not None
    finally:
        release.set()
        t1.join(timeout=5)
        h.destroy()
        p.destroy()
        p.destroy()
