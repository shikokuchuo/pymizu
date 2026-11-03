"""pymizu — Thread-speed async and parallel Python over shared memory."""

from __future__ import annotations

import contextlib as _contextlib
import os as _os
import re as _re
import subprocess as _subprocess
import sys as _sys
import threading as _threading
import warnings as _warnings
from collections.abc import Callable as _Callable
from collections.abc import Generator as _Generator
from collections.abc import Iterable as _Iterable
from collections.abc import Iterator as _Iterator
from importlib import metadata as _metadata
from typing import TYPE_CHECKING
from typing import Any as _Any

from pymizu import _pymizu
from pymizu._executor import PoolExecutor
from pymizu._pymizu import (
    CLOSED,
    FULL,
    PEER_GONE,
    TIMEOUT,
    CancelledError,
    ConcurrentUseError,
    DeclinedError,
    ForeignPayloadError,
    Frame,
    MizuError,
    Sentinel,
    ShmError,
    SlotsExhaustedError,
    StartupError,
    StoppedError,
    SubmitTimeoutError,
    TaskError,
    TaskLostError,
    WorkerDiedError,
    _call_frame,
    _channel_attach,
    _channel_new,
    _pool_attach,
    _pool_new,
    _task_frame,
    is_sentinel,
)
from pymizu._pymizu import (
    _abi_version as _abi_version,
)
from pymizu._pymizu import (
    _core_version as _core_version,
)
from pymizu._pymizu import (
    _Task as Task,
)
from pymizu._pymizu import (
    prune as _prune,
)
from pymizu._r import r_launcher

if TYPE_CHECKING:
    import numpy as _np

    from pymizu._dimarray import DimArray
    from pymizu._map import Map, PreparedMap

try:
    __version__ = _metadata.version("pymizu")
except _metadata.PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

_DROP_SOURCE = 0x53  # 'S': UTF-8 source text in the peer's language
_TOKEN_RE = _re.compile(r"[0-9a-f]+_[0-9a-f]+\Z")


def _token_of(token: str) -> str:
    """The bare join token: itself, or extracted from a pasted full
    region name (the ``/mizu_``-prefixed form R's status verbs show)."""
    m = _TOKEN_RE.search(token)
    if m is None:
        raise ValueError("pymizu: malformed join token")
    return m.group()


def launcher(
    *,
    python: str | None = None,
    stdout: _Any = None,
    stderr: _Any = None,
) -> _Callable[..., _subprocess.Popen]:
    """Return a launcher spawning Python processes running pymizu.

    [`Channel.create()`](`pymizu.Channel.create`) calls it with `token` alone
    to spawn the peer; [`Pool.create()`](`pymizu.Pool.create`) calls it with
    `token` and `slot` to spawn each worker. The process runs ``python -m
    pymizu.child <token>`` or ``python -m pymizu.worker <token> <slot>``. This
    is the default of every ``launcher=`` argument, and the counterpart of the
    R package's ``mizu_launcher()``.

    Parameters
    ----------
    python
        Path to the Python interpreter; the default is
        `sys.executable`.
    stdout
        Forwarded to `subprocess.Popen`; the default inherits the
        console, where the peer's error messages land.
    stderr
        Forwarded to `subprocess.Popen`.

    Returns
    -------
        A `callable(token, slot=None)` launcher for
        [`Channel.create()`](`pymizu.Channel.create`) and
        [`Pool.create()`](`pymizu.Pool.create`).

    Examples
    --------
    ```python
    import pymizu

    with pymizu.Pool.create(
        4, launcher=pymizu.launcher(stderr="worker.log")
    ) as pool:
        print(pool.submit(abs, -1).collect())
    ```
    """
    py = python or _sys.executable

    def launch(token: str, slot: int | None = None) -> _subprocess.Popen:
        args = (
            [py, "-m", "pymizu.child", token]
            if slot is None
            else [py, "-m", "pymizu.worker", token, str(slot)]
        )
        return _subprocess.Popen(args, stdout=stdout, stderr=stderr)

    return launch


# the internal name under the launcher= parameters' own shadowing
_default_launcher = launcher


class Channel:
    """A shared-memory channel handle (process-private).

    Create the host side with [`Channel.create()`](`pymizu.Channel.create`),
    which spawns the peer. The peer side attaches with ``Channel._attach()``,
    as ``python -m pymizu.child`` does. Handles do not survive ``fork()``.

    Threading: a side belongs to one thread at a time. Call its verbs
    ([`send()`](`pymizu.Channel.send`), [`recv()`](`pymizu.Channel.recv`),
    their batch forms) from one thread only; a second verb from another thread
    raises [`ConcurrentUseError`](`pymizu.ConcurrentUseError`). Blocking and
    async receives are likewise exclusive on a side: a blocking receive while a
    [`recv_async()`](`pymizu.Channel.recv_async`) consumer is active raises it
    too, from any thread but the consumer's event loop, whose own receives can
    never race it. Two-way traffic from two threads needs two channels.
    Teardown is the exception: [`destroy()`](`pymizu.Channel.destroy`) is safe
    from any thread.
    """

    _h: _pymizu._Channel
    _proc: _subprocess.Popen | None
    drop: bytes  # set by _attach()

    def __init__(self):
        raise TypeError("use Channel.create() or Channel._attach()")

    @classmethod
    def _wrap(cls, handle: _pymizu._Channel) -> Channel:
        self = cls.__new__(cls)
        self._h = handle
        self._proc = None
        return self

    @classmethod
    def create(
        cls,
        peer: str,
        *,
        capacity: int = 16384,
        slot_size: int = 256,
        arena_size: int = 4 * 1024 * 1024,
        spin: bool = False,
        startup_timeout: float = 30.0,
        launcher: _Callable[[str], _Any] | None = None,
    ) -> Channel:
        """Create a channel and spawn its peer.

        Parameters
        ----------
        peer
            A Python source string, evaluated in the peer process with
            ``ch`` bound to the peer-side handle.
        capacity
            Ring capacity in slots; a power of two.
        slot_size
            Bytes per ring slot; a power of two. A payload of up to
            ``slot_size - 16`` bytes fits in a slot.
        arena_size
            Bytes of shared staging area for larger payloads.
        spin
            Busy-wait rather than sleep while waiting.
        startup_timeout
            Seconds to wait for the peer to attach.
        launcher
            A ``callable(token)`` arranging for a Python process to run
            ``python -m pymizu.child <token>``; the default is
            [`launcher()`](`pymizu.launcher`) (spawns `sys.executable`
            directly). [`r_launcher()`](`pymizu.r_launcher`) returns
            one spawning an R peer (the R package ``mizu``).

        Returns
        -------
            The host-side channel handle.

        Examples
        --------
        >>> import pymizu
        >>> with pymizu.Channel.create(
        ...     \"\"\"
        ... import pymizu
        ... while True:
        ...     x = ch.recv()
        ...     if x is pymizu.CLOSED:
        ...         break
        ...     ch.send(x)
        ... \"\"\"
        ... ) as ch:
        ...     ch.send([1, "a", None])
        ...     received = ch.recv(timeout=5)
        >>> received
        [1, 'a', None]
        """
        if not isinstance(peer, str) or not peer:
            raise TypeError("pymizu: peer must be a non-empty source string")
        drop = bytes([_DROP_SOURCE]) + peer.encode("utf-8")
        h = _channel_new(capacity, slot_size, arena_size, spin, drop)
        token = h.token
        # a channel that never comes up is destroyed, not left to its
        # finalizer — on any error exit (a raising launcher, an
        # interrupt), not just a startup timeout
        try:
            proc = (launcher or _default_launcher())(token)
            if not h.ready_wait(startup_timeout):
                raise StartupError(
                    "pymizu: child failed to attach within "
                    f"{startup_timeout} seconds"
                )
        except BaseException:
            h.destroy()
            raise
        ch = cls._wrap(h)
        ch._proc = proc if isinstance(proc, _subprocess.Popen) else None
        return ch

    @classmethod
    def _attach(cls, token: str) -> Channel:
        """Attach to the channel named by a join token (the peer side).

        Parameters
        ----------
        token
            The join token from the host side's ``ch.token``.

        Returns
        -------
            The peer-side channel handle.

        Details
        -------
        Low-level: the caller consumes ``ch.drop`` before signaling
        ``ch.ready_set()``. ``python -m pymizu.child`` is the reference
        peer entry.
        """
        h, drop = _channel_attach(_token_of(token))
        ch = cls._wrap(h)
        ch.drop = drop
        return ch

    @property
    def token(self) -> str:
        """The join token for the peer's attach."""
        return self._h.token

    def send(self, x: _Any) -> Sentinel | None:
        """Send one value.

        Parameters
        ----------
        x
            The value to send. ``None`` is a valid value; bytes and
            numpy arrays cross without serialization, everything else
            is pickled (protocol 4).

        Returns
        -------
            None, or the `FULL` / `CLOSED` / `PEER_GONE` sentinel
            (identity-tested).

        Raises
        ------
        ConcurrentUseError
            Another verb is in flight on this side from a second
            thread: a side is confined to one thread at a time.
        """
        return self._h.send(x)

    def _send_error(self, exc: BaseException) -> bool:
        """The peer shim's uncaught-error send: frame the exception as an
        'I' err stream and publish it, whatever the peer's language.
        Bounded (never blocks for ring space); False when the ring was
        full or the channel already closed. ``python -m pymizu.child``
        runs this before exit; user code should not need to."""
        return self._h._send_error(exc)

    def send_batch(self, xs: _Iterable[_Any]) -> int:
        """Send several values in one call.

        Returns the number accepted, which is fewer than ``len(xs)`` when
        the ring fills or a terminal state is reached.
        """
        return self._h.send_batch(xs)

    def recv(self, timeout: float | None = None) -> _Any:
        """Receive one value.

        Parameters
        ----------
        timeout
            Seconds to wait; None waits indefinitely.

        Returns
        -------
            The value, or the `TIMEOUT` / `CLOSED` / `PEER_GONE`
            sentinel.

        Raises
        ------
        ConcurrentUseError
            Another verb is in flight on this side from a second
            thread, or a [`recv_async()`](`pymizu.Channel.recv_async`)
            consumer is armed and this is not its event-loop thread:
            a side is confined to one thread and one consumer form at
            a time.
        """
        return self._h.recv(timeout)

    def recv_async(self) -> _Any:
        """Receive the next value as an `asyncio.Future`.

        The event-driven receive: the Future resolves when the value arrives,
        without blocking or polling.

        Returns
        -------
            An `asyncio.Future` resolving with the same object a blocking
            [`recv()`](`pymizu.Channel.recv`) would have returned, zero-copy
            views included (the receive itself runs on the event-loop thread).
            Terminal states stay values: the Future resolves with the `CLOSED`
            / `PEER_GONE` sentinel.

        Details
        -------
        Several outstanding calls on one channel resolve in call order, one
        message each. Nothing is lost while arming: values already in the ring
        resolve immediately, and only values not yet sent wait for the wake-up.
        Cancelling the Future detaches the waiter, and the next message goes to
        the next waiter. There is no `timeout` argument: use `asyncio.timeout`
        or `asyncio.wait_for`. A channel created with ``spin=True`` cannot arm
        and raises. The async consumer is the side's one consumer: a blocking
        [`recv()`](`pymizu.Channel.recv`) on the same side while waiters are
        pending raises [`ConcurrentUseError`](`pymizu.ConcurrentUseError`).

        A message the peer sent outside the portable interchange subset (a
        language-private stream) rejects only the waiter that receives it, with
        [`ForeignPayloadError`](`pymizu.ForeignPayloadError`). The message is
        consumed and the async consumer keeps flowing, so the other outstanding
        calls are unaffected.
        """
        from pymizu import _aio

        return _aio.chan_recv_async(self)

    async def __aiter__(self) -> _Any:
        """Iterate over received payloads until CLOSED or PEER_GONE.

        The async form of the sync iterator. Yields payloads as they arrive,
        awaiting each through [`recv_async()`](`pymizu.Channel.recv_async`).
        Between iterations nothing is armed; the next iteration re-arms, and
        the arm's synchronous drain picks up whatever arrived meanwhile, so
        nothing is lost.

        A declined foreign payload (see
        [`recv_async()`](`pymizu.Channel.recv_async`)) propagates out of the
        iteration and ends the ``async for`` statement, so a try/except inside
        the loop body cannot catch it. To skip one, wrap the whole loop and
        re-enter on the same channel: the consumer kept flowing, and the new
        iterator picks up the next message.
        """
        while True:
            x = await self.recv_async()
            if x is CLOSED or x is PEER_GONE:
                return
            yield x

    def recv_batch(
        self, n: int = 256, timeout: float | None = None
    ) -> list[_Any] | Sentinel:
        """Receive up to ``n`` values in one call.

        Returns a list (possibly short or empty), or a terminal sentinel.
        """
        return self._h.recv_batch(n, timeout)

    def close(self, timeout: float = 5.0) -> bool:
        """Close the channel in an orderly way.

        Tells the peer to close and waits up to ``timeout`` seconds for it to
        finish. Returns True on a clean close, or False (with a warning) on
        timeout, in which case the resources are released when the handle is
        garbage collected.

        Raises
        ------
        ConcurrentUseError
            A verb is in flight on this side from a second thread: the
            side is confined to one thread (the class docstring).
        """
        ok = self._h.close(timeout)
        if ok and self._h._aio is not None:
            # a completed close settles pending recv futures with CLOSED
            from pymizu import _aio

            _aio.settle_channel(self, CLOSED)
        if not ok:
            _warnings.warn(
                "pymizu: close timed out waiting for the peer; resources "
                "release when the handle is garbage collected",
                stacklevel=2,
            )
        return ok

    def _close_signal(self) -> None:
        """Signal close without waiting (the peer's recv side sees
        CLOSED once it has read what remains)."""
        return self._h.close_signal()

    def destroy(self) -> None:
        """Tear down the handle immediately, without the close handshake.

        The peer sees the channel as closed (PEER_GONE if it died hard).
        This is the one verb that is safe from any thread. A verb still
        running on another thread keeps the handle alive, and teardown
        completes when the last one returns; an indefinite wait (recv,
        recv_batch, ready_wait, close) wakes promptly so that this can
        happen. Pending recv futures settle with PEER_GONE. Every other
        verb stays confined to one thread (see the class docstring).
        """
        if self._h._aio is not None:
            from pymizu import _aio

            _aio.settle_channel(self, PEER_GONE)
        return self._h.destroy()

    def alive(self) -> bool:
        """True while the peer process is alive."""
        return self._h.alive()

    def info(self) -> dict:
        """A read-only snapshot of the channel's state (dict)."""
        return self._h.info()

    def __enter__(self) -> Channel:
        return self

    def __exit__(self, *exc: _Any) -> bool:
        self.close()
        return False

    def __iter__(self) -> _Iterator[_Any]:
        """Iterate over received payloads until CLOSED or PEER_GONE.

        Blocks indefinitely between payloads (``recv()`` with no
        timeout); use [`Channel.recv()`](`pymizu.Channel.recv`) directly
        when a bound is needed.
        """
        while True:
            x = self.recv()
            if x is CLOSED or x is PEER_GONE:
                return
            yield x


def _launch_workers(
    h: _pymizu._Pool,
    launcher: _Callable[[str, int], _Any],
    slots: list[int],
    startup_timeout: float,
) -> None:
    """Spawn one worker per registry slot and wait for their joins;
    StartupError on timeout (the caller decides the pool's fate)."""
    token = h.token
    for slot in slots:
        launcher(token, slot)
    if not h.ready_wait(slots, startup_timeout):
        raise StartupError(
            "pymizu: workers failed to attach within "
            f"{startup_timeout} seconds"
        )


_LANG_R = 2
_LANG_PYTHON = 3

_R_NAME_RE = _re.compile(r"[A-Za-z0-9.]+:{2,3}[A-Za-z.][A-Za-z0-9._]*\Z")
_PY_NAME_RE = _re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+\Z"
)


class call:
    """A task specification for a pool of another language's workers.

    Describes a call for [`Pool.submit()`](`pymizu.Pool.submit`): a qualified
    name (``"mod.fn"`` for Python workers, ``"pkg::fn"`` for R workers) or a
    ``source=`` string in the workers' language, plus the constant arguments.
    The spec describes a call, not a value. The language is not named at the
    call site, because `Pool.submit` reads the workers' language from the pool
    itself, and a bare unqualified name errors at submit, not here.

    Parameters
    ----------
    name
        A qualified name: ``"mod.fn"`` for Python workers,
        ``"pkg::fn"`` for R workers.
    args
        Constant positional arguments of the call.
    source
        A source string in the workers' language. Exactly one of
        ``name`` or ``source`` must be given.
    kwargs
        Constant keyword arguments of the call.

    Details
    -------
    Unnamed arguments map to the positional list and keyword arguments to the
    named dict, matching R's calling convention. Arguments must be portable
    values (the portable subset documented under
    [`Channel.send()`](`pymizu.Channel.send`)). A non-portable argument raises
    [`DeclinedError`](`pymizu.DeclinedError`) at submit.

    A ``source=`` task evaluates in a fresh namespace with the keyword
    arguments bound as names and positional arguments bound as ``_1``,
    ``_2``, ... The result is the trailing expression's value, or None
    when the source ends with a statement.

    Large arguments cross to foreign workers by reference rather than by copy.
    A fresh buffer argument past the zero-copy floor is written to shared
    memory once, and the worker reads a view over it. An argument that is
    already a shared view crosses as its name alone, so no bytes move. On a
    pool whose workers predate this mechanism, such a task fails at submit with
    an error naming the remedy.

    Examples
    --------
    A spec task also runs on a normal (Python-worker) pool:

    >>> import pymizu
    >>> with pymizu.Pool.create(2) as pool:
    ...     task = pool.submit(pymizu.call("math.sqrt", 2.0))
    ...     results = pool.collect_all([task], timeout=10)
    >>> results
    [1.4142135623730951]
    """

    __slots__ = ("code", "kind", "args", "kwargs")

    def __init__(
        self,
        name: str | None = None,
        /,
        *args: _Any,
        source: str | None = None,
        **kwargs: _Any,
    ):
        if (name is None) == (source is None):
            raise TypeError(
                "pymizu: exactly one of 'name' or 'source' must be given"
            )
        code = name if source is None else source
        if not isinstance(code, str):
            raise TypeError("pymizu: 'name' and 'source' must be str")
        self.code = code
        self.kind = 0 if source is None else 1
        self.args = args
        self.kwargs = kwargs

    def __repr__(self) -> str:
        head = self.code if self.kind == 0 else f"source={self.code!r}"
        return f"pymizu.call({head}, ...)"


def _check_qualified(code: str, kind: int, lang: int) -> None:
    """The name-kind qualifier check, syntax-shaped per worker language.

    Runs at submit (where the language is always known), before args
    staging: a bare name with non-portable args raises this error, never
    the portability one. An unknown language defers to the worker's
    resolution error stream.
    """
    if kind != 0:
        return
    if lang == _LANG_R:
        ok = _R_NAME_RE.fullmatch(code)
    elif lang == _LANG_PYTHON:
        ok = _PY_NAME_RE.fullmatch(code)
    else:
        return
    if not ok:
        raise TypeError(
            "pymizu: a name-kind task needs a qualified name "
            f"('mod.fn' for Python workers, 'pkg::fn' for R workers): {code!r}"
        )


def _check_native(ident: tuple[int, int] | None, verb: str) -> None:
    """The private-frame guard: a foreign pool takes spec tasks only."""
    if ident is not None and ident[0] != _LANG_PYTHON:
        raise MizuError(
            "pymizu: this pool's workers are not Python — "
            f"{verb} needs a pymizu.call() spec on a foreign pool"
        )


def _split_source(code: str) -> tuple[_Any, _Any]:
    """The ast split shared by task and map source execution: ``(prefix,
    tail)`` — the statements ahead of a trailing expression and that
    expression's node (prefix None for a bare expression, tail None when
    the source is statements only). Uncompiled: the caller's filename is
    its own."""
    import ast

    tree = ast.parse(code)
    body = tree.body
    if body and isinstance(body[-1], ast.Expr):
        tail = ast.fix_missing_locations(ast.Expression(body[-1].value))
        if len(body) > 1:
            prefix = ast.fix_missing_locations(
                ast.Module(body=body[:-1], type_ignores=[])
            )
        else:
            prefix = None
        return prefix, tail
    return tree, None


def _exec_source(source: str, ns: dict) -> _Any:
    """Run a source-kind task: the arguments are bound in ``ns``; the
    result is the trailing expression's value, else None (the ast split:
    exec the prefix, eval the tail). Called by the worker's exec hook."""
    prefix, tail = _split_source(source)
    if prefix is not None:
        exec(compile(prefix, "<task>", "exec"), ns)
    if tail is not None:
        return eval(compile(tail, "<task>", "eval"), ns)
    return None


class Pool:
    """A shared-memory work-stealing task pool handle (process-private).

    Create the controller side with [`Pool.create()`](`pymizu.Pool.create`),
    which spawns the workers (``python -m pymizu.worker``). Other processes
    join as submitters with [`Pool.attach()`](`pymizu.Pool.attach`). The pool
    exists only as long as the process that created it: dropping the handle
    shuts the pool down as [`Pool.stop()`](`pymizu.Pool.stop`) does, but
    without waiting for the workers. Handles do not survive ``fork()``.

    Task callables travel by pickle. With the standard pickle a submitted
    callable must be an importable reference (as with `multiprocessing`), and
    installing cloudpickle lifts that restriction.

    Threading: a handle has two single-threaded halves. The producing half
    ([`submit()`](`pymizu.Pool.submit`),
    [`submit_batch()`](`pymizu.Pool.submit_batch`),
    [`map()`](`pymizu.Pool.map`)) belongs to one thread, and a concurrent
    second producer raises [`ConcurrentUseError`](`pymizu.ConcurrentUseError`).
    The waiting half (the blocking collects and [`stop()`](`pymizu.Pool.stop`))
    likewise belongs to one thread, which may be a different one from the
    producer's. To produce from several threads,
    [`attach()`](`pymizu.Pool.attach`) one handle per thread (each is its own
    producer, with its own submission queue), or use
    [`PoolExecutor`](`pymizu.PoolExecutor`), whose submits are thread-safe.
    Teardown is the exception: [`destroy()`](`pymizu.Pool.destroy`) is safe
    from any thread.
    """

    _h: _pymizu._Pool
    _destroy_hooks: list  # the PoolExecutor's settle callbacks (lazily)

    def __init__(self):
        raise TypeError("use Pool.create() or Pool.attach()")

    @classmethod
    def _wrap(cls, handle: _pymizu._Pool) -> Pool:
        self = cls.__new__(cls)
        self._h = handle
        return self

    @classmethod
    def create(
        cls,
        n_workers: int = 1,
        *,
        max_workers: int | None = None,
        max_submitters: int = 8,
        injection_cap: int = 1024,
        per_worker_cap: int = 1024,
        result_slots: int = 4096,
        slot_size: int = 512,
        startup_timeout: float = 30.0,
        launcher: _Callable[[str, int], _Any] | None = None,
    ) -> Pool:
        """Create a pool and spawn its worker processes.

        Parameters
        ----------
        n_workers
            Number of worker processes to spawn.
        max_workers
            The worker limit; defaults to ``n_workers``.
        max_submitters
            Maximum number of submitter processes.
        injection_cap
            Capacity of each submitter's queue.
        per_worker_cap
            Capacity of each worker's queue.
        result_slots
            Bounds each submitter's outstanding (uncollected) tasks.
        slot_size
            Bytes per queue entry and result slot; a larger payload
            travels in its own shared-memory region.
        startup_timeout
            Seconds to wait for the workers to attach.
        launcher
            A ``callable(token, slot)`` arranging for a Python process
            to run ``python -m pymizu.worker <token> <slot>``; the
            default is [`launcher()`](`pymizu.launcher`) (spawns
            `sys.executable` directly).

        Returns
        -------
            The controller-side pool handle.

        Examples
        --------
        >>> import pymizu
        >>> with pymizu.Pool.create(2) as pool:
        ...     task = pool.submit(pow, 2, 16)
        ...     results = pool.collect_all([task], timeout=10)
        >>> results
        [65536]
        """
        if n_workers < 1:
            raise ValueError("pymizu: n_workers must be at least 1")
        if max_workers is None:
            max_workers = n_workers
        if n_workers > max_workers:
            raise ValueError("pymizu: n_workers exceeds max_workers")
        h = _pool_new(
            max_workers,
            max_submitters,
            injection_cap,
            per_worker_cap,
            result_slots,
            slot_size,
        )
        slots = list(range(n_workers))
        try:
            _launch_workers(
                h,
                launcher or _default_launcher(),
                slots,
                startup_timeout,
            )
        except BaseException:
            # as at Channel.create: any failure before the pool serves
            # destroys it, not just a startup timeout
            h.destroy()
            raise
        return cls._wrap(h)

    @classmethod
    def attach(cls, token: str) -> Pool:
        """Attach to a live pool as a submitter, by its join token.

        Parameters
        ----------
        token
            The join token, ``pool.token`` on the creator.

        Returns
        -------
            The submitter-side pool handle.

        Details
        -------
        Each attached handle is its own producer with its own
        submission queue: attaching one handle per thread (or per
        process) is how several submitters drive one pool at once.
        """
        return cls._wrap(_pool_attach(_token_of(token)))

    @property
    def token(self) -> str:
        """The join token for worker/submitter attach."""
        return self._h.token

    def submit(
        self,
        fn: _Callable[..., _Any],
        /,
        *args: _Any,
        timeout: float | None = None,
        **kwargs: _Any,
    ) -> Task:
        """Submit ``fn(*args, **kwargs)`` as a task; return a Task handle.

        Blocks only for queue space, up to ``timeout`` seconds
        (None waits indefinitely).

        Parameters
        ----------
        fn
            The callable to run, or a [`call`](`pymizu.call`) spec.
        args
            Positional arguments for ``fn``.
        timeout
            Seconds to wait for queue space; None waits
            indefinitely.
        kwargs
            Keyword arguments for ``fn``.

        Returns
        -------
            A [`Task`](`pymizu.Task`) handle.

        Raises
        ------
        SubmitTimeoutError
            On ``timeout`` expiry.
        SlotsExhaustedError
            When no result slot is free.
        StoppedError
            When the pool is stopped.
        ConcurrentUseError
            Another submit-family verb is in flight on this handle
            from a second thread: a pool handle is single-producer
            (the class docstring).

        Details
        -------
        ``timeout`` belongs to the submission, not to ``fn``: a callable taking
        its own ``timeout=`` keyword argument cannot receive it through
        ``**kwargs`` here. Bind it first with `functools.partial` as
        ``functools.partial(fn, timeout=...)``.

        A buffer-protocol argument (for example a numpy array) larger
        than 32 KB crosses as a read-only view over shared pages, not
        a writable copy.

        With a [`call`](`pymizu.call`) spec as ``fn`` (no ``*args`` or
        ``**kwargs``, since the spec carries them), the task crosses in a
        language-neutral format. This is how you drive a pool of another
        language's workers, spawned with [`r_launcher()`](`pymizu.r_launcher`).
        On a foreign pool a plain callable raises an error naming `call`.

        Examples
        --------
        See [`Pool.create()`](`pymizu.Pool.create`) for a submit and
        collect example.
        """
        if type(fn) is call:
            if args or kwargs:
                raise TypeError(
                    "pymizu: a call spec carries its own arguments"
                )
            ident = self._h._worker_ident()
            if ident is None:
                raise MizuError("pymizu: no worker has joined this pool")
            _check_qualified(fn.code, fn.kind, ident[0])
            return self._h.submit(_call_frame(fn), timeout)
        if not callable(fn):
            raise TypeError("pymizu: fn must be callable")
        _check_native(self._h._worker_ident(), "Pool.submit")
        return self._h.submit(_task_frame(fn, args, kwargs), timeout)

    def submit_batch(
        self,
        fns: _Iterable[_Callable[[], _Any]],
        *,
        timeout: float | None = None,
    ) -> list[Task]:
        """Submit one task per zero-arg callable in ``fns`` in one call.

        Parameters
        ----------
        fns
            Zero-arg callables, one task each. Use `functools.partial`
            to bind arguments.
        timeout
            Seconds to wait for queue space; None waits
            indefinitely.

        Returns
        -------
            A list of [`Task`](`pymizu.Task`) handles. If the queue stays full
            past ``timeout`` the batch ends short, and the returned handles
            stay valid and collectible. A mid-batch failure raises with an
            ``accepted`` attribute holding the handles already submitted, which
            stay valid and collectible.

        Raises
        ------
        ConcurrentUseError
            Another submit-family verb is in flight on this handle
            from a second thread: a pool handle is single-producer
            (the class docstring).
        """
        _check_native(self._h._worker_ident(), "Pool.submit_batch")
        # the C verb frames the zero-arg callables itself (one crossing)
        return self._h.submit_batch(fns, timeout)

    def collect_any(
        self, tasks: _Iterable[Task], timeout: float | None = None
    ) -> tuple[int, _Any] | Sentinel:
        """Wait on several tasks.

        Parameters
        ----------
        tasks
            The [`Task`](`pymizu.Task`) handles to wait on.
        timeout
            Seconds to wait; None waits indefinitely.

        Returns
        -------
            ``(index, value)`` of the first task to finish, or the
            `TIMEOUT` sentinel. A failure raises with an ``index``
            attribute (0-based).

        Raises
        ------
        ConcurrentUseError
            Another wait-half verb (a blocking collect, `stop()`) is
            in flight on this handle from a second thread: the
            waiting half is confined to one thread (the class
            docstring).
        """
        return self._h.collect_any(tasks, timeout)

    def collect_all(
        self, tasks: _Iterable[Task], timeout: float | None = None
    ) -> list[_Any] | Sentinel:
        """Wait until every task is terminal.

        Parameters
        ----------
        tasks
            The [`Task`](`pymizu.Task`) handles to wait on.
        timeout
            Seconds to wait; None waits indefinitely.

        Returns
        -------
            All values in input order. On the first failure by position, raises
            with an ``index`` attribute (0-based). Only the reported handle is
            consumed, and every other handle, including the results ahead of
            it, stays collectible. A result that fails to read raises with
            ``index`` and a ``values`` attribute carrying the consumed prefix.
            The `TIMEOUT` sentinel consumes nothing.

        Raises
        ------
        ConcurrentUseError
            Another wait-half verb (a blocking collect, `stop()`) is
            in flight on this handle from a second thread: the
            waiting half is confined to one thread (the class
            docstring).
        """
        return self._h.collect_all(tasks, timeout)

    def collect_any_async(self, tasks: _Iterable[Task]) -> _Any:
        """The event-driven [`collect_any()`](`pymizu.Pool.collect_any`).

        One `asyncio.Future` for the whole collection, resolved when a task
        finishes.

        Parameters
        ----------
        tasks
            The [`Task`](`pymizu.Task`) handles to wait on.

        Returns
        -------
            An `asyncio.Future` resolving with the first finished task's
            ``(index, value)`` (with the sync verb's tie-breaks). A failure
            rejects with the exception the sync verb raises, ``index``
            attribute included. There is no `timeout` argument: use
            `asyncio.timeout` or `asyncio.wait_for`. Cancelling the Future
            detaches every task, so each can be collected blocking again, and
            never cancels them.
        """
        from pymizu import _aio

        return _aio.collect_any_async(self, tasks)

    def collect_all_async(self, tasks: _Iterable[Task]) -> _Any:
        """The event-driven [`collect_all()`](`pymizu.Pool.collect_all`).

        One `asyncio.Future` for the whole collection, resolved when every task
        is terminal.

        Parameters
        ----------
        tasks
            The [`Task`](`pymizu.Task`) handles to wait on.

        Returns
        -------
            An `asyncio.Future` resolving with all values in input order. The
            first failure by position rejects with the exception the sync verb
            raises, ``index`` attribute included; only that handle is consumed,
            and the rest stay collectible. Cancelling the Future detaches every
            task and never cancels them.
        """
        from pymizu import _aio

        return _aio.collect_all_async(self, tasks)

    def map(
        self,
        fn: _Callable[..., _Any],
        x: _Iterable[_Any],
        *,
        args: _Iterable[_Any] = (),
        kwargs: dict[str, _Any] | None = None,
        template: _Any = None,
        n_chunks: int | None = None,
        seed: int
        | bytes
        | bytearray
        | tuple[int | bytes | bytearray, int]
        | None = None,
        timeout: float | None = None,
        stream: bool = False,
        collect: str | None = None,
    ) -> _Any:
        """Map ``fn`` over ``x`` on the pool; return a map handle.

        Parameters
        ----------
        fn
            The callable to apply, or a [`call`](`pymizu.call`) spec.
        x
            The iterable of elements.
        args
            Constant positional arguments appended to every call.
        kwargs
            Constant keyword arguments of every call.
        template
            An example buffer (for example
            ``numpy.empty(m, dtype=...)``)
            declaring that every ``fn`` result is ``m`` values of that
            dtype; see Details.
        n_chunks
            Overrides the number of batches the elements are split
            into.
        seed
            An int or bytes giving every element a deterministic
            stream of the stdlib `random` module; see Details. Pass
            ``seed=(seed, offset)`` to shift every element's stream by
            ``offset`` positions, for maps split across runs or
            processes.
        timeout
            Seconds to wait for the submissions to go through; None waits
            indefinitely. Bounds submission only; the collection deadline is
            the handle's ``collect(timeout)``.
        stream
            Stream slices of ``x`` to workers instead of writing the
            whole of ``x`` to shared memory first; see Details.
        collect
            ``"copy"`` (the default) or ``"view"``; governs how a
            template-backed output area is returned.

        Returns
        -------
            A [`Map`](`pymizu.Map`) handle. ``handle.collect()`` blocks for the
            results (a list in input order, or with ``template`` the gathered
            output area, a view with ``collect="view"``) and raises the errors
            below. ``handle.collect_async()`` or ``await handle`` resolves one
            `asyncio.Future` with the same results, except on a streaming map,
            which collects blocking only because its sliding task window
            advances in the blocking collect loop. ``handle.cancel()`` stops
            the run. On ``timeout`` expiry mid-submission, `map()` itself
            returns the `TIMEOUT` sentinel (no handle). A length-0 ``x``
            returns a handle whose collect yields the empty result.

        Raises
        ------
        TaskError
            A task error re-raises from the collect as TaskError
            carrying the failing element's 0-based ``index``; the map
            fails fast (workers stop within about one batch).
        WorkerDiedError
            Worker death carries the lost element ranges as ``lost``
            (0-based half-open ``(lo, hi)`` pairs, possibly wider than
            what was lost; mizu's ``mizu_error_worker_died$lost``
            carries the same set as a two-column matrix of inclusive
            1-based ranges).
        TypeError
            A [`call`](`pymizu.call`) spec as ``fn`` with
            ``stream=True``.
        DeclinedError
            With a spec, a non-portable constant or element at stage
            time.

        Details
        -------
        One call writes ``fn``, the constant ``args``/``kwargs``, and ``x`` to
        shared memory once (or sends them inside chunk tasks when small), then
        submits one *runner* task per live worker. Runners claim adaptively
        sized element batches off a shared counter. A C-contiguous buffer of a
        supported dtype (float64/int32/int64/complex128/uint8) travels as bare
        bytes, and each worker wraps it once and indexes into it per element.

        ``seed`` gives every element a deterministic stream of the
        stdlib `random` module: element ``i`` runs under
        ``random.seed(SHA-256(seed_bytes + i.to_bytes(8, "little")))``,
        identical for any chunking, worker count, or steal order.
        ``seed`` covers the stdlib `random` module only: a task drawing
        from numpy calls [`current_rng()`](`pymizu.current_rng`) inside
        the task (the element's own ``numpy.random.Generator``,
        created once per element from the same seed material); other
        RNG libraries are out of scope. The legacy ``np.random.*``
        module functions draw from the worker's shared global
        RandomState and stay order-dependent. A seeded map element must not
        submit and collect nested tasks: while a worker waits it runs other
        work, possibly another map's batches mid-element, which wipes this
        element's [`current_rng()`](`pymizu.current_rng`) stash. A later call
        rebuilds from the seed, restarting the stream instead of continuing it
        (the stdlib streams survive).

        ``fn`` may be a [`call`](`pymizu.call`) specification instead of a
        callable. This is how you map over a foreign pool, such as one spawned
        with [`r_launcher()`](`pymizu.r_launcher`). A spec always uses a
        shared-memory region: the description crosses in a language-neutral
        format that any worker language reads. The element fills the spec's
        first positional slot (name kind) or binds as ``x`` (source kind), and
        the spec's own constant arguments travel with it, so ``args`` and
        ``kwargs`` must be empty with a spec. Constants and elements must be
        portable values, and a non-portable one raises
        [`DeclinedError`](`pymizu.DeclinedError`) at the `map()` call.
        ``seed=`` crosses as a language-neutral pair and each worker language
        derives its own streams, so a spec map takes int seeds only (on R
        workers, the seed within 32-bit range and the offset at most 2^53).
        Results are reproducible within a worker language but not identical
        across languages.

        ``template`` declares that every ``fn`` result is ``m`` values of the
        example buffer's dtype: results are written in place into a shared ``n
        x m`` output area and never serialized. Each result must be a matching
        buffer or, for ``m == 1``, a plain Python scalar. With a template,
        ``collect="copy"`` (the default) returns the area as one gathered numpy
        array (a memoryview without numpy) of shape ``(n, m)``, or ``(n,)`` for
        ``m == 1``. ``collect="view"`` returns it zero-copy, and the shared
        memory is released only when the view is gone.

        With ``stream=True``, the map does not write the whole of ``x`` to
        shared memory. It streams slices of ``x`` to workers as they take work,
        and the return value is unchanged. Slices travel as ordinary chunk
        tasks under a sliding submit/collect window of at most ``min(n_chunks,
        2 * live workers, free result slots)`` outstanding tasks, so
        shared-memory use is bounded by ``window x slice`` instead of
        ``sizeof(x)``. With the default chunk count (``min(len(x), 32 * live
        workers)``) that is roughly ``(2 * workers) / n_chunks`` of the
        serialized ``x``. ``n_chunks=`` overrides the chunk count
        (``n_chunks=len(x)`` is the extreme of one element per task).
        Everything else (result order, ``template`` and ``collect``, ``seed``
        reproducibility, the errors) is the same as for the non-streaming map.
        The trade-offs: failure detection coarsens from about one adaptive
        batch to about one chunk (the bound moves with ``n_chunks``), the
        adaptive batch sizing is lost (raise ``n_chunks`` to reduce skew), and
        slices are serialized like ordinary task payloads instead of taking the
        zero-copy path a raw array gets. A streaming map with Python workers
        shares one description of the map with every worker, each read at most
        once. A [`call`](`pymizu.call`) spec as ``fn`` streams too, over
        workers of any language. Each chunk in the window then writes a small
        shared-memory region of its own (the slice plus the task that runs on
        it, one runner task per chunk), removed as soon as the chunk completes.
        Shared-memory use stays bounded by the window plus a small per-worker
        cache rather than by ``sizeof(x)``. Each chunk also writes its results
        separately, so they cannot combine into one view: ``collect="view"``
        raises ``TypeError`` on a streaming spec map, as does preparing one
        with [`map_prepare()`](`pymizu.Pool.map_prepare`).

        ``timeout`` bounds submission only (the wait for queue space while the
        tasks go out). The collection deadline is the handle's
        ``collect(timeout)``, whose expiry cancels the outstanding work and
        returns the `TIMEOUT` sentinel rather than raising.

        [`starmap()`](`pymizu.Pool.starmap`) is the unpacking variant, calling
        ``fn(*element, *args, **kwargs)`` in the `multiprocessing.Pool.starmap`
        convention.

        Threading: a map is a producer on the handle, so it raises
        `ConcurrentUseError` when another map or a submit is in flight from a
        second thread (see the class docstring).

        Examples
        --------
        >>> import pymizu
        >>> with pymizu.Pool.create(2) as pool:
        ...     results = pool.map(abs, range(-5, 5)).collect()
        >>> results
        [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]
        """
        from pymizu import _map

        return _map.pool_map(
            self,
            fn,
            x,
            args,
            kwargs,
            n_chunks=n_chunks,
            seed=seed,
            timeout=timeout,
            template=template,
            collect=collect,
            stream=stream,
        )

    def starmap(
        self,
        fn: _Callable[..., _Any],
        x: _Iterable[_Any],
        *,
        args: _Iterable[_Any] = (),
        kwargs: dict[str, _Any] | None = None,
        template: _Any = None,
        n_chunks: int | None = None,
        seed: int
        | bytes
        | bytearray
        | tuple[int | bytes | bytearray, int]
        | None = None,
        timeout: float | None = None,
        stream: bool = False,
        collect: str | None = None,
    ) -> _Any:
        """Map ``fn`` over ``x``, unpacking each element as call arguments.

        Calls ``fn(*element, *args, **kwargs)`` and returns the run's
        [`Map`](`pymizu.Map`) handle, as [`map()`](`pymizu.Pool.map`) does.
        This is the `multiprocessing.Pool.starmap` convention. For the
        multi-iterable shape of `concurrent.futures.Executor.map`, zip first:
        ``pool.starmap(fn, zip(xs, ys))``. Elements must be iterables (a 2-D
        buffer's rows qualify; a 1-D buffer's scalars do not). A
        [`call`](`pymizu.call`) spec cannot starmap, because element unpacking
        is Python-only. Everything else (seeding, templates, streaming, the
        errors) works as in [`map()`](`pymizu.Pool.map`).
        """
        from pymizu import _map

        if type(fn) is call:
            raise TypeError(
                "pymizu: a call spec cannot starmap — element unpacking "
                "is Python-only"
            )
        if not callable(fn):
            raise TypeError("pymizu: fn must be callable")
        return _map.pool_map(
            self,
            _map._Star(fn),
            list(x),
            args,
            kwargs,
            n_chunks=n_chunks,
            seed=seed,
            timeout=timeout,
            template=template,
            collect=collect,
            stream=stream,
        )

    def map_prepare(
        self,
        fn: _Callable[..., _Any],
        x: _Iterable[_Any],
        *,
        args: _Iterable[_Any] = (),
        kwargs: dict[str, _Any] | None = None,
        template: _Any = None,
        n_chunks: int | None = None,
        stream: bool = False,
    ) -> PreparedMap:
        """Stage a map once for repeated runs; return a `PreparedMap`.

        Takes the same arguments as [`map()`](`pymizu.Pool.map`) except
        ``seed``, ``timeout``, and ``collect``, which are per-run options of
        [`PreparedMap.run()`](`pymizu.PreparedMap.run`). The set-up (pickling
        the description, creating the shared region, the worker-side attach) is
        paid once here. Each ``pm.run()`` re-arms cheaply, reuses the workers'
        cached set-up, and returns the run's [`Map`](`pymizu.Map`) handle, on
        which the results are collected. A run collected with
        ``collect="view"`` hands its region to the view, so the next run stages
        a fresh one. With ``stream=True`` the staged ``x`` stays on the
        submitter, so a replacement ``x`` of any shape re-slices. Only a length
        change under ``template`` stages fresh shared memory, because the
        output area is sized for the staged length. Close the handle (or use it
        as a context manager) to release the region.
        """
        from pymizu import _map

        return _map.PreparedMap(
            self,
            fn,
            x,
            args=args,
            kwargs=kwargs,
            n_chunks=n_chunks,
            template=template,
            stream=stream,
        )

    def retire(self, slot: int) -> None:
        """Ask the worker in ``slot`` to exit cleanly.

        Non-blocking; controller only.
        """
        return self._h.retire(slot)

    def spawn_workers(
        self,
        n: int = 1,
        *,
        startup_timeout: float = 30.0,
        launcher: _Callable[[str, int], _Any] | None = None,
    ) -> list[int]:
        """Spawn ``n`` additional workers and wait for them to join.

        Controller only.

        Parameters
        ----------
        n
            Number of workers to spawn.
        startup_timeout
            Seconds to wait for the workers to attach.
        launcher
            As for [`Pool.create()`](`pymizu.Pool.create`); the default
            is [`launcher()`](`pymizu.launcher`).

        Returns
        -------
            The slot indices spawned into.
        """
        if n < 1:
            raise ValueError("pymizu: n must be at least 1")
        # the role gate precedes the launcher: a non-controller's spawn
        # would error only at the join wait, after real worker processes
        # had started
        st = self.info()
        if st["role"] != "controller":
            raise MizuError("pymizu: only the controller can resize a pool")
        free = [i for i, s in enumerate(st["workers"]) if s == "free"]
        if len(free) < n:
            raise MizuError(
                f"pymizu: not enough free worker slots ({len(free)} free)"
            )
        slots = free[:n]
        _launch_workers(
            self._h,
            launcher or _default_launcher(),
            slots,
            startup_timeout,
        )
        return slots

    def stop(self, timeout: float = 5.0) -> bool:
        """Orderly shutdown (controller only).

        Tell the workers to exit, cancel pending tasks, wait up to
        ``timeout`` seconds for clean exits, and release the shared
        memory. Safe to call more than once.

        Returns
        -------
            True on clean worker exits within ``timeout``.

        Raises
        ------
        ConcurrentUseError
            A blocking collect or a submit is in flight on this handle
            from a second thread: each half is confined to one thread
            (the class docstring).
        """
        ok = self._h.stop(timeout)
        if not ok:
            _warnings.warn(
                "pymizu: pool stop timed out waiting for workers; they exit "
                "on their own once they observe shutdown",
                stacklevel=2,
            )
        return ok

    def destroy(self) -> None:
        """Tear down the pool handle immediately, without waiting.

        No shutdown is signalled to the workers; they exit once they
        detect that the controller is gone. This is the one verb that is
        safe from any thread. A verb still running on another thread
        keeps the handle alive, and teardown completes when the last one
        returns; an indefinite wait (submit, collect, collect_any,
        collect_all, ready_wait) wakes promptly, while stop() waits out
        its own deadline and a worker's idle run() can delay teardown
        until it returns. Pending async consumers fail with the
        closed-handle error. Every other verb stays confined to one
        thread (see the class docstring).
        """
        for hook in getattr(self, "_destroy_hooks", ()):
            hook()
        if self._h._aio is not None:
            from pymizu import _aio

            _aio.settle_pool(
                self._h, ValueError("pymizu: pool handle is closed")
            )
        return self._h.destroy()

    def info(self) -> dict:
        """A read-only snapshot of the pool's state (dict).

        It includes the ``token`` to pass to
        [`Pool.attach()`](`pymizu.Pool.attach`).
        """
        d = self._h.info()
        d["token"] = self.token
        return d

    def dump(self) -> dict:
        """A read-only debugging snapshot of the whole pool region (dict)."""
        return self._h.dump()

    def stats(self) -> dict:
        """Cumulative per-worker and per-submitter counters.

        Counted since each participant joined; a dict with ``workers``
        and ``submitters`` lists.
        """
        return self._h.stats()

    def trace(self, fn: _Callable[[str, str], None] | None) -> None:
        """Register a hook called as ``fn(event, id)`` at each task event.

        The events this process observes are ``"submit"`` on the
        submitting thread, and ``"start"``, ``"done"``, ``"error"``,
        ``"drop"``, and ``"rehome"`` on worker handles. ``id`` is
        ``"<submitter slot>:<counter>"``, stable across processes.
        Registration is per-handle and per-process; ``None`` removes the
        hook. An exception in the hook is reported as unraisable, never
        raised into the pool.
        """
        self._h.set_trace(fn)

    def __enter__(self) -> Pool:
        return self

    def __exit__(self, *exc: _Any) -> bool:
        self.stop()
        return False


_worker_local = _threading.local()

# The process-wide default pool registry: one (pool, pid-at-set) tuple, so
# a reader never sees a torn pair across threads. The registry anchors the
# handle — an unreferenced pool's finalizer stops the pool — and the pid
# lets a forked child read the default as unset without touching a handle
# it does not own (the C-side fork guard on handle unwrap is the backstop).
_default_state: tuple[Pool | None, int] = (None, 0)


def prune() -> list[str]:
    """Remove orphaned shared memory regions.

    Removes the pymizu shared memory regions that dead processes leave behind.
    This is needed only in exceptional circumstances. Cleanup after a crash is
    automatic as long as any participant of the pool or channel survives it,
    because the survivor detects the death and reclaims the dead process's
    regions itself. Orphans arise only when every attached process dies at once
    (for example, the whole process group is killed), so no survivor remains to
    run that cleanup. They persist until pruned or until the machine reboots.

    Regions of running processes are never touched. Do not run this while
    another pymizu session on the machine is recovering from a crash: values
    sent by a now-dead peer stay readable by its survivor until consumed, and
    this function would remove them.

    A crashed process cannot clean up after itself, and a new process
    that happens to reuse its PID cannot reap its orphans either (it
    reads its own PID as alive). Run ``prune()`` while the PID is free,
    before reuse.

    Returns
    -------
        The region names removed, or an empty list if none were. On
        platforms whose shared memory namespace cannot be enumerated
        (Windows, where orphans cannot exist), always an empty list.
    """
    return _prune()


def is_remote_error(x: _Any) -> bool:
    """Test whether a received channel value is a remote error.

    An uncaught error in a channel peer crosses as a value, not a raised
    exception. The value is a
    [`TaskError`](`pymizu.TaskError`) carrying the original exception's
    class name as ``remote_type`` and its traceback text as
    ``detail``; raise it to propagate the error.

    Returns
    -------
        True for a received remote error, False otherwise.
    """
    return isinstance(x, TaskError)


def current_pool() -> Pool | None:
    """The pool handle of the worker running the task.

    Returns
    -------
        The worker's own pool handle inside a task; None outside one.

    Details
    -------
    A task uses it for nested submission: a nested submit goes onto the
    worker's own queue, with no ring and no wait, and a nested collect runs
    other work instead of sleeping, so nested fan-outs are cheap and cannot
    deadlock the pool. The process-wide default set with
    [`default_pool()`](`pymizu.default_pool`) never overrides this binding.
    """
    return getattr(_worker_local, "pool", None)


def current_rng() -> _np.random.Generator | None:
    """The current element's own numpy Generator, inside a seeded map.

    Returns
    -------
        The element's own ``numpy.random.Generator`` inside a
        seeded map element; None outside one.

    Raises
    ------
    TypeError
        When numpy is not installed.

    Details
    -------
    [`Pool.map()`](`pymizu.Pool.map`) ``seed=`` seeds the stdlib
    `random` module per element; a task drawing from numpy calls this
    instead: the element's own ``numpy.random.Generator``, derived
    from the same seed material on first call in the element
    (kept separate from the stdlib derivation, so those streams are
    unchanged). Two calls in one element continue one stream, and distinct
    elements get distinct streams, so results are identical for any chunking,
    worker count, or steal order.

    Outside a seeded map element (an unseeded map, an ordinary task, the
    submitter process) the return is None. A seeded map element must
    not submit and collect nested tasks: while the worker waits it runs
    other work, possibly another map's batches mid-element, which wipes this
    element's stash. A later call rebuilds from the seed, restarting the stream
    instead of continuing it.
    """
    from pymizu import _map

    return _map._current_rng()


def default_pool() -> Pool | None:
    """The process-wide default pool, or None when none is set.

    Set with [`set_default_pool()`](`pymizu.set_default_pool`); scoped
    use with [`using_pool()`](`pymizu.using_pool`). Package code taking
    an optional pool resolves it in this order: an explicit ``pool``
    argument, then [`current_pool()`](`pymizu.current_pool`) inside a
    task (the evaluating worker's own pool, for nested submission), then
    ``default_pool()``, then the caller's own fallback, such as sequential
    execution or an error.

    Setting a default checks only the type: a stopped pool is accepted and
    fails at use time with the usual stopped-pool errors. Handles from
    [`Pool.attach()`](`pymizu.Pool.attach`) are valid defaults; ownership and
    teardown stay with the pool's creator.

    The default is process-global: every thread sees the same pool, and
    [`current_pool()`](`pymizu.current_pool`) remains the thread-local
    mechanism. After a ``fork()``, a child process reads it as unset, because
    handles are process-private.
    """
    pool, pid = _default_state
    return pool if pid == _os.getpid() else None


def set_default_pool(pool: Pool | None) -> Pool | None:
    """Set the process-wide default pool; None clears it.

    Returns
    -------
        The previous default (a Pool or None), so callers can save and
        restore.

    Details
    -------
    Setting a default keeps the pool alive: it survives even after its
    variable is deleted, until the default is cleared or replaced. See
    [`default_pool()`](`pymizu.default_pool`) for the full semantics.
    """
    global _default_state
    if pool is not None and not isinstance(pool, Pool):
        raise TypeError("pymizu: pool must be a Pool or None")
    prev = _default_state[0]
    _default_state = (pool, _os.getpid())
    return prev


@_contextlib.contextmanager
def using_pool(pool: Pool | None) -> _Generator[Pool | None, None, None]:
    """Use ``pool`` as the default for the with block, then restore.

    Parameters
    ----------
    pool
        The pool to make the default; ``None`` scopes a cleared
        default.

    Returns
    -------
        A context manager yielding ``pool``. The previous default
        returns on exit, including on exception.
    """
    prev = set_default_pool(pool)
    try:
        yield pool
    finally:
        set_default_pool(prev)


def __getattr__(name: str) -> _Any:
    # Lazy re-export: pymizu._map try-imports numpy, which stays deferred
    # until the first map call rather than pymizu's own import.
    if name == "Map":
        from pymizu._map import Map

        return Map
    if name == "PreparedMap":
        from pymizu._map import PreparedMap

        return PreparedMap
    # pymizu._dimarray imports numpy outright — the same deferral, so a
    # numpy-less `import pymizu` stays clean.
    if name == "DimArray":
        from pymizu._dimarray import DimArray

        return DimArray
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    # the lazy re-exports never enter the module dict (PEP 562's
    # __getattr__ returns them without binding), so dir() lists __all__
    return sorted(set(globals()) | set(__all__))


__all__ = [
    "CLOSED",
    "FULL",
    "PEER_GONE",
    "TIMEOUT",
    "CancelledError",
    "Channel",
    "ConcurrentUseError",
    "DeclinedError",
    "DimArray",
    "ForeignPayloadError",
    "Frame",
    "Map",
    "Pool",
    "PoolExecutor",
    "PreparedMap",
    "MizuError",
    "Sentinel",
    "ShmError",
    "SlotsExhaustedError",
    "StartupError",
    "StoppedError",
    "SubmitTimeoutError",
    "Task",
    "TaskError",
    "TaskLostError",
    "WorkerDiedError",
    "__version__",
    "call",
    "current_pool",
    "current_rng",
    "default_pool",
    "is_remote_error",
    "is_sentinel",
    "launcher",
    "prune",
    "r_launcher",
    "set_default_pool",
    "using_pool",
]
