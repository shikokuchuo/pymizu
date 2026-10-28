"""The threading contract: one thread per handle role at a time.

Concurrent verbs within one role raise ConcurrentUseError (a check, not
a lock) instead of racing the core's single-writer words — the failure
modes the guard replaced were silent: torn ring entries, aliased result
slots, duplicated or dropped channel messages.
"""

import threading
import time

import pytest
from tests.helpers import identity

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
