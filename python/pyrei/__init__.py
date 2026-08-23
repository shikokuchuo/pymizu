"""pyrei — lock-free shared-memory IPC: SPSC channels and work-stealing
task pools (the Python binding for librei)."""

import re as _re
import subprocess as _subprocess
import sys as _sys
import threading as _threading
import warnings as _warnings

from pyrei._pyrei import (
    CLOSED,
    FULL,
    PEER_GONE,
    TIMEOUT,
    CancelledError,
    ReiError,
    ShmError,
    SlotsExhaustedError,
    StartupError,
    StoppedError,
    SubmitTimeoutError,
    TaskError,
    WorkerDiedError,
    __core_version__,
    _channel_attach,
    _channel_new,
    _pool_attach,
    _pool_new,
    _pool_worker_join,
    _Task as Task,
    abi_version,
    is_sentinel,
)

__version__ = "0.1.0"

_DROP_SOURCE = 0x53  # 'S': UTF-8 source text in the peer's language
_TOKEN_RE = _re.compile(r"[0-9a-f]+_[0-9a-f]+\Z")


def _default_launcher():
    """Spawn the channel peer as ``python -m pyrei.child <token>``."""

    def launch(token):
        return _subprocess.Popen([_sys.executable, "-m", "pyrei.child", token])

    return launch


class Channel:
    """A shared-memory SPSC channel handle (process-private).

    Create the host side with :meth:`create` (which spawns the peer);
    the peer side attaches with :meth:`attach` — ``python -m pyrei.child``
    does this. Handles do not survive ``fork()``.
    """

    def __init__(self):
        raise TypeError("use Channel.create() or Channel.attach()")

    @classmethod
    def _wrap(cls, handle):
        self = cls.__new__(cls)
        self._h = handle
        self._proc = None
        return self

    @classmethod
    def create(
        cls,
        peer,
        *,
        capacity=16384,
        slot_size=256,
        arena_size=4 * 1024 * 1024,
        spin=False,
        startup_timeout=30.0,
        launcher=None,
    ):
        """Create a channel and spawn its peer.

        ``peer`` is a Python source string, evaluated in the peer process
        with ``ch`` bound to the peer-side handle. ``capacity`` and
        ``slot_size`` are powers of two; the inline payload budget is
        ``slot_size - 16``. ``launcher`` is a ``callable(token)`` arranging
        for a Python process to run ``python -m pyrei.child <token>``; the
        default spawns ``sys.executable`` directly.
        """
        if not isinstance(peer, str) or not peer:
            raise TypeError("pyrei: peer must be a non-empty source string")
        drop = bytes([_DROP_SOURCE]) + peer.encode("utf-8")
        h = _channel_new(capacity, slot_size, arena_size, spin, drop)
        token = h.token
        proc = (launcher or _default_launcher())(token)
        if not h.ready_wait(startup_timeout):
            h.destroy()
            raise StartupError(
                "pyrei: child failed to attach within "
                f"{startup_timeout} seconds"
            )
        ch = cls._wrap(h)
        ch._proc = proc if isinstance(proc, _subprocess.Popen) else None
        return ch

    @classmethod
    def attach(cls, token):
        """Attach to the channel named by a join token (the peer side).

        Low-level: the caller consumes ``ch.drop`` before signalling
        ``ch.ready_set()``. ``python -m pyrei.child`` is the reference
        peer entry.
        """
        if not _TOKEN_RE.fullmatch(token):
            raise ValueError("pyrei: malformed join token")
        h, drop = _channel_attach(token)
        ch = cls._wrap(h)
        ch.drop = drop
        return ch

    @property
    def token(self):
        """The join token for the peer's attach."""
        return self._h.token

    def send(self, x):
        """Send one payload; return None, or the FULL / CLOSED /
        PEER_GONE sentinel (identity-tested). ``None`` itself is a
        valid payload; bytes and numpy arrays travel raw, everything
        else rides pickle protocol 4."""
        return self._h.send(x)

    def send_batch(self, xs):
        """Send several payloads in one crossing; return the number
        accepted (short on ring-full or a terminal state)."""
        return self._h.send_batch(xs)

    def recv(self, timeout=None):
        """Receive one payload, waiting up to ``timeout`` seconds
        (None waits indefinitely); the TIMEOUT / CLOSED / PEER_GONE
        sentinel on the non-payload outcomes."""
        return self._h.recv(timeout)

    def recv_batch(self, n=256, timeout=None):
        """Receive up to ``n`` payloads in one crossing; a list
        (possibly short or empty), or a terminal sentinel."""
        return self._h.recv_batch(n, timeout)

    def close(self, timeout=5.0):
        """Orderly close: signal the peer and wait up to ``timeout``
        seconds for it to observe the close. True on a clean handshake;
        False (with a warning) on timeout, in which case resources
        release when the handle is garbage collected."""
        ok = self._h.close(timeout)
        if not ok:
            _warnings.warn(
                "pyrei: close timed out waiting for the peer; resources "
                "release when the handle is garbage collected"
            )
        return ok

    def close_signal(self):
        """Signal close without waiting (the peer's recv side sees
        CLOSED once the ring drains)."""
        return self._h.close_signal()

    def destroy(self):
        """Tear down the handle immediately, without the close
        handshake. The peer sees PEER_GONE."""
        return self._h.destroy()

    def alive(self):
        """True while the peer process is alive."""
        return self._h.alive()

    def info(self):
        """A read-only wire-state snapshot of the channel (dict)."""
        return self._h.info()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _default_worker_launcher():
    """Spawn one pool worker as ``python -m pyrei.worker <token> <slot>``."""

    def launch(token, slot):
        return _subprocess.Popen(
            [_sys.executable, "-m", "pyrei.worker", token, str(slot)]
        )

    return launch


class Pool:
    """A shared-memory work-stealing task pool handle (process-private).

    Create the controller side with :meth:`create` (which spawns the
    workers, ``python -m pyrei.worker``); other processes join as
    submitters with :meth:`attach`. The pool's lifetime is bound to the
    creating process: dropping the handle shuts the pool down as
    :meth:`stop` does, but without the wait. Handles do not survive
    ``fork()``.

    Task callables ride pickle: under stock pickle a submitted callable
    must be an importable reference (the multiprocessing constraint);
    installing cloudpickle lifts that transparently.
    """

    def __init__(self):
        raise TypeError("use Pool.create() or Pool.attach()")

    @classmethod
    def _wrap(cls, handle):
        self = cls.__new__(cls)
        self._h = handle
        return self

    @classmethod
    def create(
        cls,
        workers=1,
        *,
        max_workers=None,
        max_submitters=8,
        injection_cap=1024,
        per_worker_cap=1024,
        result_slots=4096,
        slot_size=512,
        launcher=None,
        startup_timeout=30.0,
    ):
        """Create a pool and spawn its worker processes.

        ``workers`` worker processes join the pool's registry (capacity
        ``max_workers``). ``result_slots`` bounds each submitter's
        outstanding (uncollected) tasks; ``slot_size`` is the bytes per
        queue entry and result slot — a payload past the inline budget
        travels in a fresh region per payload. ``launcher`` is a
        ``callable(token, slot)`` arranging for a Python process to run
        ``python -m pyrei.worker <token> <slot>``; the default spawns
        ``sys.executable`` directly.
        """
        workers = int(workers)
        if workers < 1:
            raise ValueError("pyrei: workers must be at least 1")
        if max_workers is None:
            max_workers = workers
        if workers > max_workers:
            raise ValueError("pyrei: workers exceeds max_workers")
        h = _pool_new(
            max_workers,
            max_submitters,
            injection_cap,
            per_worker_cap,
            result_slots,
            slot_size,
        )
        token = h.token
        launch = launcher or _default_worker_launcher()
        slots = list(range(workers))
        for slot in slots:
            launch(token, slot)
        if not h.ready_wait(slots, startup_timeout):
            h.destroy()
            raise StartupError(
                "pyrei: workers failed to attach within "
                f"{startup_timeout} seconds"
            )
        return cls._wrap(h)

    @classmethod
    def attach(cls, token):
        """Attach to a live pool as a submitter, by its join token.

        The token travels out of band: it is ``pool.token`` on the
        creator.
        """
        if not _TOKEN_RE.fullmatch(token):
            raise ValueError("pyrei: malformed join token")
        return cls._wrap(_pool_attach(token))

    @property
    def token(self):
        """The join token for worker/submitter attach."""
        return self._h.token

    def submit(self, fn, /, *args, timeout=None, **kwargs):
        """Submit ``fn(*args, **kwargs)`` as a task; return a Task handle.

        Blocks only for injection-ring space, up to ``timeout`` seconds
        (None waits indefinitely): SubmitTimeoutError on expiry,
        SlotsExhaustedError / StoppedError on the fatal outcomes.
        """
        if not callable(fn):
            raise TypeError("pyrei: fn must be callable")
        return self._h.submit((fn, args, kwargs), timeout)

    def submit_batch(self, fns, *, timeout=None):
        """Submit one task per zero-arg callable in ``fns`` in one crossing.

        Ring-full past ``timeout`` ends the batch short — the returned
        handles stay valid and collectible. Use functools.partial to bind
        arguments.
        """
        payloads = []
        for fn in fns:
            if not callable(fn):
                raise TypeError("pyrei: batch items must be callable")
            payloads.append((fn, (), {}))
        return self._h.submit_batch(payloads, timeout)

    def collect_any(self, tasks, timeout=None):
        """Wait on several tasks; return ``(index, value)`` of the first
        terminal one, or the TIMEOUT sentinel. A non-OK outcome raises
        with an ``index`` attribute (0-based)."""
        return self._h.collect_any(tasks, timeout)

    def collect_all(self, tasks, timeout=None):
        """Wait until every task is terminal; return all values in input
        order, or the TIMEOUT sentinel (which consumes nothing)."""
        return self._h.collect_all(tasks, timeout)

    def map(self, fn, x, *, args=(), kwargs=None, chunks=None, seed=None,
            timeout=None):
        """Map ``fn`` over the elements of ``x`` on the pool; return the
        results as a list in input order.

        One call stages ``fn``, the constant ``args``/``kwargs``, and
        ``x`` exactly once (a shared region, or inline in chunk tasks when
        small), then submits one *runner* task per live worker; runners
        self-schedule adaptively sized element batches off a shared
        cursor. A C-contiguous buffer of a supported dtype
        (float64/int32/complex128/uint8) travels as bare bytes — workers
        wrap it once and index per element. ``chunks`` overrides the
        morsel count (the scheduling granularity). ``seed`` (an int or
        bytes) derives deterministic per-element streams of the stdlib
        ``random`` module: element ``i`` runs under
        ``random.seed(SHA-256(seed_bytes + i.to_bytes(8, "little")))``,
        identical for any chunking, worker count, or steal order.

        A task error re-raises as TaskError carrying the failing element's
        0-based ``index``; failure is fail-fast (peers stop within about
        one batch). Worker death raises WorkerDiedError carrying the lost
        element ranges as ``lost`` (0-based half-open ``(lo, hi)`` pairs,
        conservative). On ``timeout`` expiry the outstanding work is
        cancelled and the TIMEOUT sentinel is returned, never raised.
        """
        from pyrei import _map

        return _map.pool_map(self, fn, x, args, kwargs, chunks, seed,
                             timeout)

    def retire(self, slot):
        """Ask the worker in ``slot`` to exit cleanly (non-blocking)."""
        return self._h.retire(slot)

    def spawn_workers(self, n=1, *, launcher=None, startup_timeout=30.0):
        """Spawn ``n`` additional workers into free registry slots and wait
        for them to join. Returns the slot indices spawned into."""
        n = int(n)
        if n < 1:
            raise ValueError("pyrei: n must be at least 1")
        free = [
            i for i, s in enumerate(self.status()["workers"]) if s == "free"
        ]
        if len(free) < n:
            raise ReiError(
                f"pyrei: not enough free worker slots ({len(free)} free)"
            )
        slots = free[:n]
        token = self._h.token
        launch = launcher or _default_worker_launcher()
        for slot in slots:
            launch(token, slot)
        if not self._h.ready_wait(slots, startup_timeout):
            raise StartupError(
                "pyrei: workers failed to attach within "
                f"{startup_timeout} seconds"
            )
        return slots

    def stop(self, timeout=5.0):
        """Orderly shutdown (controller only): broadcast shutdown, cancel
        pending tasks, wait up to ``timeout`` seconds for clean worker
        exits, and unlink. Idempotent."""
        ok = self._h.stop(timeout)
        if not ok:
            _warnings.warn(
                "pyrei: pool stop timed out waiting for workers; they exit "
                "on their own once they observe shutdown"
            )
        return ok

    def destroy(self):
        """Tear down the pool handle immediately, without the shutdown
        broadcast or the wait. Workers exit once they observe the
        controller gone."""
        return self._h.destroy()

    def status(self):
        """A read-only wire-state snapshot of the pool (dict)."""
        return self._h.status()

    def dump(self):
        """A read-only debugging snapshot of the whole pool region (dict)."""
        return self._h.dump()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()
        return False


_worker_local = _threading.local()


def current_pool():
    """The evaluating worker's own pool handle, inside a task.

    A task uses it for nested submission: a nested submit pushes onto the
    worker's own work-stealing deque (no ring, no wait), and a nested
    collect helps — executes work — instead of parking, so nested fan-outs
    run at fork/join cost and never deadlock the pool. None outside a
    task.
    """
    return getattr(_worker_local, "pool", None)


__all__ = [
    "CLOSED",
    "FULL",
    "PEER_GONE",
    "TIMEOUT",
    "CancelledError",
    "Channel",
    "Pool",
    "ReiError",
    "ShmError",
    "SlotsExhaustedError",
    "StartupError",
    "StoppedError",
    "SubmitTimeoutError",
    "Task",
    "TaskError",
    "WorkerDiedError",
    "__core_version__",
    "__version__",
    "abi_version",
    "current_pool",
    "is_sentinel",
]
