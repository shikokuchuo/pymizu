"""pyrei — lock-free shared-memory IPC: SPSC channels and work-stealing
task pools (the Python binding for librei)."""

import re as _re
import subprocess as _subprocess
import sys as _sys
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
    WorkerDiedError,
    __core_version__,
    _channel_attach,
    _channel_new,
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
        return self._h.send(x)

    def send_batch(self, xs):
        return self._h.send_batch(xs)

    def recv(self, timeout=None):
        return self._h.recv(timeout)

    def recv_batch(self, n=256, timeout=None):
        return self._h.recv_batch(n, timeout)

    def close(self, timeout=5.0):
        ok = self._h.close(timeout)
        if not ok:
            _warnings.warn(
                "pyrei: close timed out waiting for the peer; resources "
                "release when the handle is garbage collected"
            )
        return ok

    def close_signal(self):
        return self._h.close_signal()

    def destroy(self):
        return self._h.destroy()

    def alive(self):
        return self._h.alive()

    def info(self):
        return self._h.info()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


__all__ = [
    "CLOSED",
    "FULL",
    "PEER_GONE",
    "TIMEOUT",
    "CancelledError",
    "Channel",
    "ReiError",
    "ShmError",
    "SlotsExhaustedError",
    "StartupError",
    "StoppedError",
    "SubmitTimeoutError",
    "WorkerDiedError",
    "__core_version__",
    "__version__",
    "abi_version",
    "is_sentinel",
]
