"""pymizu — lock-free shared-memory IPC: SPSC channels and work-stealing
task pools (the Python binding for libmizu)."""

from __future__ import annotations

import re as _re
import subprocess as _subprocess
import sys as _sys
import threading as _threading
import warnings as _warnings
from collections.abc import Callable as _Callable
from collections.abc import Iterable as _Iterable
from typing import Any as _Any

from pymizu import _pymizu
from pymizu._pymizu import (
    CLOSED,
    FULL,
    PEER_GONE,
    TIMEOUT,
    CancelledError,
    DeclinedError,
    Frame,
    MizuError,
    ShmError,
    SlotsExhaustedError,
    StartupError,
    StoppedError,
    SubmitTimeoutError,
    TaskError,
    WorkerDiedError,
    __core_version__,
    _call_frame,
    _channel_attach,
    _channel_new,
    _pool_attach,
    _pool_new,
    _task_frame,
    abi_version,
    is_sentinel,
)
from pymizu._pymizu import (
    _Task as Task,
)
from pymizu._pymizu import (
    prune as _prune,
)
from pymizu._r import r_launcher, r_pool_launcher

__version__ = "0.1.0.dev0"

_DROP_SOURCE = 0x53  # 'S': UTF-8 source text in the peer's language
_TOKEN_RE = _re.compile(r"[0-9a-f]+_[0-9a-f]+\Z")


def _default_launcher() -> _Callable[[str], _subprocess.Popen]:
    """Spawn the channel peer as ``python -m pymizu.child <token>``."""

    def launch(token: str) -> _subprocess.Popen:
        return _subprocess.Popen(
            [_sys.executable, "-m", "pymizu.child", token]
        )

    return launch


class Channel:
    """A shared-memory SPSC channel handle (process-private).

    Create the host side with :meth:`create` (which spawns the peer);
    the peer side attaches with :meth:`attach` — ``python -m pymizu.child``
    does this. Handles do not survive ``fork()``.
    """

    _h: _pymizu._Channel
    _proc: _subprocess.Popen | None
    drop: bytes  # set by attach()

    def __init__(self):
        raise TypeError("use Channel.create() or Channel.attach()")

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

        ``peer`` is a Python source string, evaluated in the peer process
        with ``ch`` bound to the peer-side handle. ``capacity`` and
        ``slot_size`` are powers of two; the inline payload budget is
        ``slot_size - 16``. ``launcher`` is a ``callable(token)`` arranging
        for a Python process to run ``python -m pymizu.child <token>``; the
        default spawns ``sys.executable`` directly. ``pymizu.r_launcher()``
        returns one spawning an R peer (the R package ``mizu``).
        """
        if not isinstance(peer, str) or not peer:
            raise TypeError("pymizu: peer must be a non-empty source string")
        drop = bytes([_DROP_SOURCE]) + peer.encode("utf-8")
        h = _channel_new(capacity, slot_size, arena_size, spin, drop)
        token = h.token
        proc = (launcher or _default_launcher())(token)
        if not h.ready_wait(startup_timeout):
            h.destroy()
            raise StartupError(
                "pymizu: child failed to attach within "
                f"{startup_timeout} seconds"
            )
        ch = cls._wrap(h)
        ch._proc = proc if isinstance(proc, _subprocess.Popen) else None
        return ch

    @classmethod
    def attach(cls, token: str) -> Channel:
        """Attach to the channel named by a join token (the peer side).

        Low-level: the caller consumes ``ch.drop`` before signalling
        ``ch.ready_set()``. ``python -m pymizu.child`` is the reference
        peer entry.
        """
        if not _TOKEN_RE.fullmatch(token):
            raise ValueError("pymizu: malformed join token")
        h, drop = _channel_attach(token)
        ch = cls._wrap(h)
        ch.drop = drop
        return ch

    @property
    def token(self) -> str:
        """The join token for the peer's attach."""
        return self._h.token

    def send(self, x: _Any) -> _pymizu._Sentinel | None:
        """Send one payload; return None, or the FULL / CLOSED /
        PEER_GONE sentinel (identity-tested). ``None`` itself is a
        valid payload; bytes and numpy arrays travel raw, everything
        else rides pickle protocol 4."""
        return self._h.send(x)

    def _send_error(self, exc: BaseException) -> bool:
        """The peer shim's uncaught-error send: frame the exception as an
        'I' err stream and publish it, whatever the peer's language.
        Bounded (never blocks for ring space); False when the ring was
        full or the channel already closed. ``python -m pymizu.child``
        runs this before exit; user code should not need to."""
        return self._h._send_error(exc)

    def send_batch(self, xs: _Iterable[_Any]) -> int:
        """Send several payloads in one crossing; return the number
        accepted (short on ring-full or a terminal state)."""
        return self._h.send_batch(xs)

    def recv(self, timeout: float | None = None) -> _Any:
        """Receive one payload, waiting up to ``timeout`` seconds
        (None waits indefinitely); the TIMEOUT / CLOSED / PEER_GONE
        sentinel on the non-payload outcomes."""
        return self._h.recv(timeout)

    def recv_batch(
        self, n: int = 256, timeout: float | None = None
    ) -> list[_Any] | _pymizu._Sentinel:
        """Receive up to ``n`` payloads in one crossing; a list
        (possibly short or empty), or a terminal sentinel."""
        return self._h.recv_batch(n, timeout)

    def close(self, timeout: float = 5.0) -> bool:
        """Orderly close: signal the peer and wait up to ``timeout``
        seconds for it to observe the close. True on a clean handshake;
        False (with a warning) on timeout, in which case resources
        release when the handle is garbage collected."""
        ok = self._h.close(timeout)
        if not ok:
            _warnings.warn(
                "pymizu: close timed out waiting for the peer; resources "
                "release when the handle is garbage collected",
                stacklevel=2,
            )
        return ok

    def close_signal(self) -> None:
        """Signal close without waiting (the peer's recv side sees
        CLOSED once the ring drains)."""
        return self._h.close_signal()

    def destroy(self) -> None:
        """Tear down the handle immediately, without the close
        handshake. The peer sees PEER_GONE."""
        return self._h.destroy()

    def alive(self) -> bool:
        """True while the peer process is alive."""
        return self._h.alive()

    def info(self) -> dict:
        """A read-only wire-state snapshot of the channel (dict)."""
        return self._h.info()

    def __enter__(self) -> Channel:
        return self

    def __exit__(self, *exc: _Any) -> bool:
        self.close()
        return False


def _default_worker_launcher() -> _Callable[[str, int], _subprocess.Popen]:
    """Spawn one pool worker as ``python -m pymizu.worker <token> <slot>``."""

    def launch(token: str, slot: int) -> _subprocess.Popen:
        return _subprocess.Popen(
            [_sys.executable, "-m", "pymizu.worker", token, str(slot)]
        )

    return launch


_LANG_R = 2
_LANG_PYTHON = 3

_R_NAME_RE = _re.compile(r"[A-Za-z0-9.]+:{2,3}[A-Za-z.][A-Za-z0-9._]*\Z")
_PY_NAME_RE = _re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+\Z"
)


class call:
    """A task specification for a pool of another language's workers.

    Describes a call for :meth:`Pool.submit`: a qualified name
    (``"mod.fn"`` for Python workers, ``"pkg::fn"`` for R workers) or a
    ``source=`` string in the workers' language, plus the constant
    arguments. The spec describes a call; it is not a value. The
    language never appears at the call site — Pool.submit resolves the
    workers' language from the pool itself — and a bare (unqualified)
    name errors at submit, not here.

    Unnamed arguments map to the positional list and keyword arguments
    to the named dict, matching R's mixed-call convention. Arguments
    must be portable values (the interchange subset documented under
    :meth:`Channel.send`): a non-portable argument raises
    :class:`DeclinedError` at submit, never a fallback.

    A ``source=`` task evaluates in a fresh namespace with the keyword
    arguments bound as names and positional arguments bound as ``_1``,
    ``_2``, ... The result is the trailing expression's value, or None
    when the source ends with a statement.
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


def _exec_source(source: str, ns: dict) -> _Any:
    """Run a source-kind task: the arguments are bound in ``ns``; the
    result is the trailing expression's value, else None (the ast split:
    exec the prefix, eval the tail). Called by the worker's exec hook."""
    import ast

    tree = ast.parse(source)
    body = tree.body
    if body and isinstance(body[-1], ast.Expr):
        if len(body) > 1:
            prefix = ast.fix_missing_locations(
                ast.Module(body=body[:-1], type_ignores=[])
            )
            exec(compile(prefix, "<task>", "exec"), ns)
        tail = ast.fix_missing_locations(ast.Expression(body[-1].value))
        return eval(compile(tail, "<task>", "eval"), ns)
    exec(compile(tree, "<task>", "exec"), ns)
    return None


class Pool:
    """A shared-memory work-stealing task pool handle (process-private).

    Create the controller side with :meth:`create` (which spawns the
    workers, ``python -m pymizu.worker``); other processes join as
    submitters with :meth:`attach`. The pool's lifetime is bound to the
    creating process: dropping the handle shuts the pool down as
    :meth:`stop` does, but without the wait. Handles do not survive
    ``fork()``.

    Task callables ride pickle: under stock pickle a submitted callable
    must be an importable reference (the multiprocessing constraint);
    installing cloudpickle lifts that transparently.
    """

    _h: _pymizu._Pool

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
        workers: int = 1,
        *,
        max_workers: int | None = None,
        max_submitters: int = 8,
        injection_cap: int = 1024,
        per_worker_cap: int = 1024,
        result_slots: int = 4096,
        slot_size: int = 512,
        launcher: _Callable[[str, int], _Any] | None = None,
        startup_timeout: float = 30.0,
    ) -> Pool:
        """Create a pool and spawn its worker processes.

        ``workers`` worker processes join the pool's registry (capacity
        ``max_workers``). ``result_slots`` bounds each submitter's
        outstanding (uncollected) tasks; ``slot_size`` is the bytes per
        queue entry and result slot — a payload past the inline budget
        travels in a fresh region per payload. ``launcher`` is a
        ``callable(token, slot)`` arranging for a Python process to run
        ``python -m pymizu.worker <token> <slot>``; the default spawns
        ``sys.executable`` directly.
        """
        if workers < 1:
            raise ValueError("pymizu: workers must be at least 1")
        if max_workers is None:
            max_workers = workers
        if workers > max_workers:
            raise ValueError("pymizu: workers exceeds max_workers")
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
                "pymizu: workers failed to attach within "
                f"{startup_timeout} seconds"
            )
        return cls._wrap(h)

    @classmethod
    def attach(cls, token: str) -> Pool:
        """Attach to a live pool as a submitter, by its join token.

        The token travels out of band: it is ``pool.token`` on the
        creator.
        """
        if not _TOKEN_RE.fullmatch(token):
            raise ValueError("pymizu: malformed join token")
        return cls._wrap(_pool_attach(token))

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

        Blocks only for injection-ring space, up to ``timeout`` seconds
        (None waits indefinitely): SubmitTimeoutError on expiry,
        SlotsExhaustedError / StoppedError on the fatal outcomes.

        ``timeout`` belongs to the submission, not to ``fn``: a callable
        taking its own ``timeout=`` keyword argument cannot receive it
        through ``**kwargs`` here — bind it first with
        ``functools.partial(fn, timeout=...)``.

        A buffer-protocol argument (e.g. a numpy array) past the
        zero-copy floor crosses as a read-only view over shared pages,
        not a writable copy.

        With a :class:`pymizu.call` spec as ``fn`` (no ``*args`` /
        ``**kwargs`` — the spec carries them), the task stream crosses
        in the neutral interchange format: this is how a pool of another
        language's workers is driven (spawn them with
        :func:`pymizu.r_pool_launcher`). On a foreign pool a plain
        callable errors locally, naming the spec verb.
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
        """Submit one task per zero-arg callable in ``fns`` in one crossing.

        Ring-full past ``timeout`` ends the batch short — the returned
        handles stay valid and collectible. Use functools.partial to bind
        arguments.
        """
        _check_native(self._h._worker_ident(), "Pool.submit_batch")
        payloads = []
        for fn in fns:
            if not callable(fn):
                raise TypeError("pymizu: batch items must be callable")
            payloads.append(_task_frame(fn, (), {}))
        return self._h.submit_batch(payloads, timeout)

    def collect_any(
        self, tasks: _Iterable[Task], timeout: float | None = None
    ) -> tuple[int, _Any] | _pymizu._Sentinel:
        """Wait on several tasks; return ``(index, value)`` of the first
        terminal one, or the TIMEOUT sentinel. A non-OK outcome raises
        with an ``index`` attribute (0-based)."""
        return self._h.collect_any(tasks, timeout)

    def collect_all(
        self, tasks: _Iterable[Task], timeout: float | None = None
    ) -> list[_Any] | _pymizu._Sentinel:
        """Wait until every task is terminal; return all values in input
        order. On the first non-OK outcome by position, raise with an
        ``index`` attribute (0-based) — handles up to it inclusive are
        consumed, the rest stay collectible. The TIMEOUT sentinel
        consumes nothing."""
        return self._h.collect_all(tasks, timeout)

    def map(
        self,
        fn: _Callable[..., _Any],
        x: _Iterable[_Any],
        *,
        args: _Iterable[_Any] = (),
        kwargs: dict[str, _Any] | None = None,
        chunks: int | None = None,
        seed: int
        | bytes
        | bytearray
        | tuple[int | bytes | bytearray, int]
        | None = None,
        timeout: float | None = None,
        template: _Any = None,
        collect: str | None = None,
    ) -> list[_Any] | _pymizu._Sentinel | _Any:
        """Map ``fn`` over the elements of ``x`` on the pool; return the
        results as a list in input order.

        One call stages ``fn``, the constant ``args``/``kwargs``, and
        ``x`` exactly once (a shared region, or inline in chunk tasks when
        small), then submits one *runner* task per live worker; runners
        self-schedule adaptively sized element batches off a shared
        cursor. A C-contiguous buffer of a supported dtype
        (float64/int32/int64/complex128/uint8) travels as bare bytes — workers
        wrap it once and index per element. ``chunks`` overrides the
        morsel count (the scheduling granularity). ``seed`` (an int or
        bytes) derives deterministic per-element streams of the stdlib
        ``random`` module: element ``i`` runs under
        ``random.seed(SHA-256(seed_bytes + i.to_bytes(8, "little")))``,
        identical for any chunking, worker count, or steal order. Pass
        ``seed=(seed, offset)`` to shift every element's stream by
        ``offset`` positions, for maps split across runs or processes.

        ``fn`` may be a :class:`pymizu.call` specification instead of a
        callable — the way to map over a foreign pool (one spawned with
        :func:`r_pool_launcher`). A spec always stages a shared region:
        the descriptor crosses in the interchange format and each runner
        task carries a region reference any worker language reads. The
        element fills the spec's first positional slot (name kind) or
        binds as ``x`` (source kind), and the spec's own constant
        arguments ride with it — so ``args`` and ``kwargs`` must be empty
        with a spec. Constants and elements must be portable values; a
        non-portable one raises :class:`DeclinedError` at stage time.
        ``seed=`` carries as a language-neutral pair and each worker
        language derives its own streams, so a spec map takes int seeds
        only (32-bit-ranged on R workers); invariance holds within a
        worker language, never identical draws across languages.

        ``template`` is an exemplar buffer (e.g. ``numpy.empty(m,
        dtype=...)``) declaring that every ``fn`` result is ``m`` values
        of that dtype: results are written in place into a shared
        ``n x m`` output area and never serialized. Each result must be a
        matching buffer — or, for ``m == 1``, a plain Python scalar. With
        a template, ``collect="copy"`` (the default) returns the area as
        one gathered numpy array (a memoryview without numpy) of shape
        ``(n, m)`` — ``(n,)`` for ``m == 1``; ``collect="view"`` returns
        it zero-copy, with the map region's teardown deferred to the
        view's.

        A task error re-raises as TaskError carrying the failing element's
        0-based ``index``; failure is fail-fast (peers stop within about
        one batch). Worker death raises WorkerDiedError carrying the lost
        element ranges as ``lost`` (0-based half-open ``(lo, hi)`` pairs,
        conservative). On ``timeout`` expiry the outstanding work is
        cancelled and the TIMEOUT sentinel is returned, never raised.
        """
        from pymizu import _map

        return _map.pool_map(
            self, fn, x, args, kwargs, chunks, seed, timeout, template,
            collect,
        )

    def map_prepare(
        self,
        fn: _Callable[..., _Any],
        x: _Iterable[_Any],
        *,
        args: _Iterable[_Any] = (),
        kwargs: dict[str, _Any] | None = None,
        chunks: int | None = None,
        seed: int
        | bytes
        | bytearray
        | tuple[int | bytes | bytearray, int]
        | None = None,
        template: _Any = None,
        collect: str | None = None,
    ) -> _Any:
        """Stage a map once for repeated runs; return a map handle.

        Takes the same arguments as ``map`` (minus ``timeout``, which is
        per-run). The descriptor pickle, the region create, and the
        worker-side attach are paid once here; each ``map_run`` re-arms in
        O(1) and reuses the workers' cached contexts. A run collected with
        ``collect="view"`` hands its region to the view, so the next run
        restages into a fresh one. Close the handle (or use it as a
        context manager) to unlink the region.
        """
        from pymizu import _map

        return _map.PreparedMap(
            self, fn, x, args, kwargs, chunks, seed, template, collect
        )

    def map_run(
        self,
        prepared: _Any,
        x: _Any = None,
        timeout: float | None = None,
    ) -> _Any:
        """Run a map handle from ``map_prepare`` once; return its results
        (the same shapes and outcome taxonomy as ``map``).

        ``x`` replaces the staged data for this and later runs: a
        raw-buffer replacement of the same dtype and length swaps in
        place (a memcpy over the region, no restage); anything else
        restages transparently."""
        from pymizu import _map

        if not isinstance(prepared, _map.PreparedMap):
            raise TypeError("pymizu: not a prepared map handle")
        if prepared._pool is not self:
            raise ValueError("pymizu: map handle belongs to another pool")
        return prepared.run(x, timeout)

    def retire(self, slot: int) -> None:
        """Ask the worker in ``slot`` to exit cleanly (non-blocking)."""
        return self._h.retire(slot)

    def spawn_workers(
        self,
        n: int = 1,
        *,
        launcher: _Callable[[str, int], _Any] | None = None,
        startup_timeout: float = 30.0,
    ) -> list[int]:
        """Spawn ``n`` additional workers into free registry slots and wait
        for them to join. Returns the slot indices spawned into."""
        if n < 1:
            raise ValueError("pymizu: n must be at least 1")
        free = [
            i for i, s in enumerate(self.status()["workers"]) if s == "free"
        ]
        if len(free) < n:
            raise MizuError(
                f"pymizu: not enough free worker slots ({len(free)} free)"
            )
        slots = free[:n]
        token = self._h.token
        launch = launcher or _default_worker_launcher()
        for slot in slots:
            launch(token, slot)
        if not self._h.ready_wait(slots, startup_timeout):
            raise StartupError(
                "pymizu: workers failed to attach within "
                f"{startup_timeout} seconds"
            )
        return slots

    def stop(self, timeout: float = 5.0) -> bool:
        """Orderly shutdown (controller only): broadcast shutdown, cancel
        pending tasks, wait up to ``timeout`` seconds for clean worker
        exits, and unlink. Idempotent."""
        ok = self._h.stop(timeout)
        if not ok:
            _warnings.warn(
                "pymizu: pool stop timed out waiting for workers; they exit "
                "on their own once they observe shutdown",
                stacklevel=2,
            )
        return ok

    def destroy(self) -> None:
        """Tear down the pool handle immediately, without the shutdown
        broadcast or the wait. Workers exit once they observe the
        controller gone."""
        return self._h.destroy()

    def status(self) -> dict:
        """A read-only wire-state snapshot of the pool (dict)."""
        return self._h.status()

    def dump(self) -> dict:
        """A read-only debugging snapshot of the whole pool region (dict)."""
        return self._h.dump()

    def stats(self) -> dict:
        """Cumulative per-worker and per-submitter counters since each
        participant joined (dict with ``workers`` and ``submitters``
        lists)."""
        return self._h.stats()

    def trace(self, fn: _Callable[[str, str], None] | None) -> None:
        """Register a hook called as ``fn(event, id)`` at each task
        lifecycle event this process observes: ``"submit"`` on the
        submitting thread; ``"start"``, ``"done"``, ``"error"``,
        ``"drop"``, ``"rehome"`` on worker handles. ``id`` is
        ``"<submitter slot>:<counter>"``, stable across processes.
        Registration is per-handle and per-process; ``None`` removes the
        hook. A hook exception is written as unraisable, never propagated
        into the pool."""
        self._h.set_trace(fn)

    def __enter__(self) -> Pool:
        return self

    def __exit__(self, *exc: _Any) -> bool:
        self.stop()
        return False


_worker_local = _threading.local()


def prune() -> list[str]:
    """Remove orphaned shared memory regions.

    Removes the pymizu shared memory regions that dead processes leave
    behind. Needed only in exceptional circumstances. Cleanup after a
    crash is automatic as long as any participant of the pool or channel
    survives it. The survivor detects the death through the liveness lock
    and reclaims the regions of the dead process itself. Orphans arise
    only when every attached process dies at once (for example, the whole
    process group is killed), and no survivor remains to run that cleanup.
    They persist until pruned or until the machine reboots.

    Regions of running processes are never touched. If another pymizu
    session on the machine is itself recovering from a crash, do not run
    this. Payloads sent by a now-dead peer stay deliverable to its
    survivor until drained, and this function reaps them.

    A crashed process cannot clean up after itself, and a new process
    that happens to reuse its PID cannot reap its orphans either (it
    reads its own PID as alive). Run ``prune()`` while the PID is free,
    before reuse.

    Returns the region names removed, or an empty list if none were. On
    platforms whose shared memory namespace cannot be enumerated
    (Windows, where orphans cannot exist), always an empty list.
    """
    return _prune()


def is_remote_error(x: _Any) -> bool:
    """Test whether a received channel value is a remote error.

    An uncaught error in a channel peer crosses as a value, not a raised
    exception (transport states are values, payloads are values — user
    code decides to raise). The value is a :class:`TaskError` carrying
    the original exception's class name as ``remote_type`` and its
    traceback text as ``remote_traceback``; raise it to propagate.

    Returns True for a received remote error, False otherwise.
    """
    return isinstance(x, TaskError)


def current_pool() -> Pool | None:
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
    "DeclinedError",
    "Frame",
    "Pool",
    "MizuError",
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
    "call",
    "current_pool",
    "is_remote_error",
    "is_sentinel",
    "prune",
    "r_launcher",
    "r_pool_launcher",
]
