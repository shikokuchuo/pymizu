"""Pool.map orchestration (mirrors the R package's map.R).

One call stages fn / args / kwargs / x once — a single pickled descriptor
stream in one fresh map region — or entirely inline in chunk tasks when it
fits the entry inline budget (the region-less blob path). It then submits
one *runner* task per live worker: runners self-schedule element ranges
off the shared cursor in the map region (morsel-driven scheduling), one
adaptive-sized batch per ``_map_next`` transition, and publish their batch
histories and values as their single ordinary result. Workers materialize
the map context at most once each (a small cache keyed by region name); a
raw-buffer x is wrapped once per worker and indexed per element, never
deserialized.

Seeding: ``seed`` derives deterministic per-element streams of the stdlib
``random`` module — element ``i`` runs under
``random.seed(SHA-256(seed_bytes + i.to_bytes(8, "little")))``, with the
worker's prior RNG state saved and restored around each batch. Because the
streams are per-element, results are identical for any chunking, worker
count, or steal order.
"""

from __future__ import annotations

import hashlib as _hashlib
import random as _random
import time as _time
from collections.abc import Callable as _Callable
from typing import TYPE_CHECKING
from typing import Any as _Any

from pyrei import _pyrei

if TYPE_CHECKING:
    import pyrei

try:
    import cloudpickle as _pickle
except ImportError:  # pragma: no cover - environment-dependent
    import pickle as _pickle

try:
    import numpy as _np
except ImportError:  # pragma: no cover - environment-dependent
    _np = None

# The region header's x_kind values (src/map.c).
_X_DESC = 0
_X_RAWBUF = 1

# The wire tags of the raw-x gate (rei.h's REI_TYPE_*), and their numpy /
# memoryview spellings. Complex needs numpy (a memoryview cannot cast to
# it); the gate falls back to the descriptor path when numpy is absent.
_TAG_NP = {
    24: "uint8",
    14: "float64",
    13: "int32",
    10: "int32",
    15: "complex128",
}
_TAG_MV = {24: "B", 14: "d", 13: "i", 10: "i"}

# Morsel geometry (frozen by the R package's gate sweep): target ~256
# morsels per runner, clamped to a constant grain.
_MORSEL_CAP = 256
_MORSELS_PER_RUNNER = 256

# The worker-local map-context cache, keyed by region name. Plain bounded
# cache: past 8 resident contexts, clear everything — eviction only drops
# the cache references (an in-flight runner's own ctx keeps its mapping
# alive), and more than a handful of maps interleaving on one worker is
# pathological and costs one re-attach. The x view owns no reference to
# the region, so it rides the same entry as the capsule that pins it.
_CTX_CACHE_MAX = 8


class _Ctx:
    __slots__ = ("capsule", "fn", "args", "kwargs", "get")

    def __init__(
        self,
        capsule: _Any,
        fn: _Callable[..., _Any],
        args: tuple,
        kwargs: dict,
        get: _Callable[[int], _Any],
    ) -> None:
        self.capsule = capsule
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.get = get


_ctx_cache: dict[str, _Ctx] = {}


def _raw_accessor(view: _Any, tag: int) -> _Callable[[int], _Any]:
    """Wrap the region's raw x section once: a numpy array over the mapping
    when numpy is present, else a cast memoryview. Indexing yields one
    element; no worker ever copies more than the elements it reads."""
    if _np is not None:
        return _np.frombuffer(view, dtype=_TAG_NP[tag]).__getitem__
    return view.cast(_TAG_MV[tag]).__getitem__


def _map_ctx(name: str) -> _Ctx:
    ctx = _ctx_cache.get(name)
    if ctx is None:
        if len(_ctx_cache) >= _CTX_CACHE_MAX:
            _ctx_cache.clear()
        capsule = _pyrei._map_open(name)
        hdr = _pyrei._map_header(capsule)
        desc = _pickle.loads(_pyrei._map_desc(capsule))
        if hdr["x_kind"] == _X_RAWBUF:
            fn, args, kwargs = desc
            get = _raw_accessor(_pyrei._map_x_view(capsule), hdr["x_tag"])
        else:
            fn, args, kwargs, x = desc
            get = x.__getitem__
        ctx = _Ctx(capsule, fn, args, kwargs, get)
        _ctx_cache[name] = ctx
    return ctx


def _run_batch(
    fn: _Callable[..., _Any],
    args: tuple,
    kwargs: dict,
    get: _Callable[[int], _Any],
    lo: int,
    hi: int,
    seed_bytes: bytes | None,
) -> list:
    """One batch's element loop: fn(elt, *args, **kwargs) over [lo, hi).
    An escaping error is annotated with the in-flight element index (the
    "first by element index" contract — the worker's error envelope
    carries it as the fourth tuple element). Seeded: install element i's
    stream before its call; the worker's own RNG state is restored around
    the batch either way."""
    out = []
    if seed_bytes is None:
        for i in range(lo, hi):
            try:
                out.append(fn(get(i), *args, **kwargs))
            except Exception as e:
                e._pyrei_map_index = i  # pyrefly: ignore [missing-attribute]
                raise
        return out
    state = _random.getstate()
    try:
        for i in range(lo, hi):
            _random.seed(
                _hashlib.sha256(seed_bytes + i.to_bytes(8, "little")).digest()
            )
            try:
                out.append(fn(get(i), *args, **kwargs))
            except Exception as e:
                e._pyrei_map_index = i  # pyrefly: ignore [missing-attribute]
                raise
    finally:
        _random.setstate(state)
    return out


def _chunk(blob: bytes, lo: int, hi: int, seed_bytes: bytes | None) -> list:
    """Worker-side blob-path chunk task: the inline descriptor blob (the
    sizes this path admits make a per-chunk unpickle negligible, so there
    is no cache), the 0-based half-open element range, and the seed."""
    fn, args, kwargs, x = _pickle.loads(blob)
    return _run_batch(fn, args, kwargs, x.__getitem__, lo, hi, seed_bytes)


def _runner(
    region_name: str, ordinal: int, seed_bytes: bytes | None
) -> tuple[list, list]:
    """Worker-side morsel runner, riding each runner task. Opens (or
    reuses) the map context, obtains the pool signals worker-locally —
    never from the submitter — and loops batch transitions: claim the next
    adaptively sized batch off the shared cursor, answer the doorbell at
    the boundary when flagged, evaluate, and record the batch in the local
    history only after it completes (a batch interrupted by an error is
    never recorded — correctly lost). The history and per-batch value
    lists ride the runner's single ordinary result publish."""
    import pyrei

    pool = pyrei.current_pool()
    assert pool is not None  # a runner always executes inside a task
    ctx = _map_ctx(region_name)
    sig = pool._h._signals()
    hist = []
    vals = []
    try:
        while True:
            nxt = _pyrei._map_next(ctx.capsule, sig, ordinal)
            if nxt is None:
                break
            lo, hi, help_flag = nxt
            # doorbell: one foreign injection task between batches hands
            # concurrent submitters their chunk-boundary interleave back
            if help_flag:
                pool._h._help_once()
            vals.append(
                _run_batch(
                    ctx.fn, ctx.args, ctx.kwargs, ctx.get, lo, hi, seed_bytes
                )
            )
            hist.append((lo, hi))
    except Exception:
        # the fail-fast store, ahead of the ERR publish: peers observe it
        # within ~a batch instead of draining the cursor first
        _pyrei._map_cancel_set(ctx.capsule)
        raise
    return (hist, vals)


def _seed_bytes(seed: int | bytes | bytearray | None) -> bytes | None:
    if seed is None:
        return None
    if isinstance(seed, bool) or not isinstance(seed, (int, bytes, bytearray)):
        raise TypeError("pyrei: seed must be an int or bytes")
    if isinstance(seed, int):
        return str(seed).encode("ascii")
    return bytes(seed)


def pool_map(
    pool: pyrei.Pool,
    fn: _Callable[..., _Any],
    x: _Any,
    args: _Any,
    kwargs: dict | None,
    chunks: int | None,
    seed: int | bytes | bytearray | None,
    timeout: float | None,
) -> list | _pyrei._Sentinel:
    """The one map path: stage, submit the runners (or blob chunks),
    collect against the single deadline, splice into input order. The
    try/finally backstop cancels outstanding work on KeyboardInterrupt or
    error and unlinks the region explicitly (the capsule destructor is
    only the GC backstop)."""
    import pyrei

    if not callable(fn):
        raise TypeError("pyrei: fn must be callable")
    args = tuple(args)
    kwargs = {} if kwargs is None else dict(kwargs)
    seed_bytes = _seed_bytes(seed)

    # the raw-x gate: a C-contiguous buffer of a supported dtype rides the
    # region as bare bytes (complex needs numpy's frombuffer); anything
    # else pickles into the descriptor
    probe = _pyrei._map_probe_x(x)
    if probe is not None and probe[0] == 15 and _np is None:
        probe = None
    if probe is None:
        x = list(x)
        n = len(x)
    else:
        n = probe[1]
    if n == 0:
        return []
    if chunks is not None:
        chunks = int(chunks)
        if chunks < 1:
            raise ValueError("pyrei: chunks must be a positive number")

    live, free_rs, inj_cap, inline_entry = pool._h._map_caps()
    if free_rs == 0:
        raise pyrei.SlotsExhaustedError(
            "pyrei: result slots exhausted — collect or cancel outstanding "
            "tasks first"
        )

    deadline = None if timeout is None else _time.monotonic() + timeout

    def remaining() -> float | None:
        if deadline is None:
            return None
        return max(0.0, deadline - _time.monotonic())

    def expired() -> bool:
        return deadline is not None and _time.monotonic() >= deadline

    # Region-less probe: does the full chunk payload — the wrapper plus
    # the descriptor blob as an ordinary argument — fit the entry inline
    # budget? Skipped when a raw x alone already exceeds the budget, so a
    # huge x is never pickled just to learn it does not fit.
    blob = None
    if probe is None or probe[2] <= inline_entry:
        cand = _pickle.dumps((fn, args, kwargs, x), 4)
        worst = _pickle.dumps((_chunk, (cand, n, n, seed_bytes), {}), 4)
        if len(worst) <= inline_entry:
            blob = cand

    handles = []
    box = {"capsule": None}  # _map_region hands its region back through
    try:  # here for the finally's cancel + unlink
        if blob is not None:
            return _map_blob(
                pool,
                pyrei,
                blob,
                n,
                seed_bytes,
                chunks,
                live,
                free_rs,
                inj_cap,
                remaining,
                expired,
                handles,
            )
        return _map_region(
            pool,
            pyrei,
            fn,
            args,
            kwargs,
            x,
            probe,
            n,
            seed_bytes,
            chunks,
            live,
            free_rs,
            inj_cap,
            remaining,
            expired,
            handles,
            box,
        )
    finally:
        # the interrupt/error/timeout backstop (a clean collect consumed
        # every handle: this is then a no-op), then the explicit unlink
        capsule = box["capsule"]
        if capsule is not None:
            _pyrei._map_cancel_set(capsule)
        for h in handles:
            if h is not None:
                h.cancel()
        if capsule is not None:
            _pyrei._map_close(capsule)


def _map_blob(
    pool: pyrei.Pool,
    pyrei,
    blob: bytes,
    n: int,
    seed_bytes: bytes | None,
    chunks: int | None,
    live: int,
    free_rs: int,
    inj_cap: int,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
    handles: list,
) -> list | _pyrei._Sentinel:
    """The region-less path: fixed chunks ride the task payloads inline.
    Worker death reports the dead chunk's fixed [lo, hi) range directly
    (there is no cursor)."""
    c = min(
        n, chunks if chunks is not None else 8 * max(1, live), free_rs, inj_cap
    )
    c = max(1, c)
    size, extra = divmod(n, c)
    sizes = [size + 1] * extra + [size] * (c - extra)
    lo = 0
    timed_out = False
    for k in range(c):
        # pre-check, not just the verb's: a nested (worker-side) submit
        # never waits on ring space, so an expired deadline must be caught
        # here, before the payload is built
        if expired():
            timed_out = True
            break
        hi = lo + sizes[k]
        h = pool._h.submit(
            (_chunk, (blob, lo, hi, seed_bytes), {}), remaining()
        )
        handles.append(h)
        lo = hi
    if timed_out:
        return pyrei.TIMEOUT
    out = []
    lo = 0
    for k in range(len(handles)):
        hi = lo + sizes[k]
        try:
            v = handles[k].collect(timeout=remaining())
        except pyrei.WorkerDiedError as e:
            e.lost = [(lo, hi)]
            raise
        if v is pyrei.TIMEOUT:
            return pyrei.TIMEOUT
        handles[k] = None
        out.extend(v)
        lo = hi
    return out


def _map_region(
    pool: pyrei.Pool,
    pyrei,
    fn: _Callable[..., _Any],
    args: tuple,
    kwargs: dict,
    x: _Any,
    probe: tuple | None,
    n: int,
    seed_bytes: bytes | None,
    chunks: int | None,
    live: int,
    free_rs: int,
    inj_cap: int,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
    handles: list,
    box: dict,
) -> list | _pyrei._Sentinel:
    """The region path: stage, submit one runner per live worker (clamped
    by the morsel count, the free result slots, and the injection ring),
    collect under the exhausted-runner trim, splice by element position."""
    runners = min(max(1, live), free_rs, inj_cap)
    if chunks is None:
        morsel = max(1, min(n // (runners * _MORSELS_PER_RUNNER), _MORSEL_CAP))
    else:
        morsel = -(-n // min(n, chunks))
    n_morsels = -(-n // morsel)
    desc = _pickle.dumps(
        (fn, args, kwargs) if probe is not None else (fn, args, kwargs, x), 4
    )
    name, capsule = _pyrei._map_stage(
        desc, x if probe is not None else None, n, morsel
    )
    # hand the region to pool_map's finally backstop (cancel + unlink)
    box["capsule"] = capsule
    r = min(n_morsels, runners)
    timed_out = False
    for k in range(r):
        if expired():
            timed_out = True
            break
        h = pool._h._submit_runner(
            (_runner, (name, k, seed_bytes), {}), remaining()
        )
        handles.append(h)
    if timed_out:
        return pyrei.TIMEOUT
    return _collect_region(pyrei, capsule, handles, n, remaining, expired)


def _collect_region(
    pyrei,
    capsule: _Any,
    handles: list,
    n: int,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
) -> list | _pyrei._Sentinel:
    """Collect the runner handles under the exhausted-runner trim, in a
    deferred collection order. A runner carries no work of its own, so
    once the cursor exhausts a still-queued runner is dead weight — but
    parking unboundedly on its handle would wait for a busy peer to claim
    and no-op it. So: each pass abandons what the armed trigger allows,
    collects handles whose CLAIM word reads RUNNING, and defers IDLE ones.
    Only when every uncollected handle defers does collect park, in
    bounded slices, re-scanning on each return."""
    errs = []
    died: pyrei.WorkerDiedError | None = None
    runs = []

    def consume(k: int, t: float | None) -> bool:
        nonlocal died
        h = handles[k]
        try:
            v = h.collect(timeout=t)
        except pyrei.WorkerDiedError as e:
            handles[k] = None
            # fail fast: peers stop within ~a batch (idempotent — an
            # erroring runner already stored this before its ERR publish)
            _pyrei._map_cancel_set(capsule)
            if died is None:
                died = e
            return True
        except pyrei.TaskError as e:
            handles[k] = None
            _pyrei._map_cancel_set(capsule)
            if hasattr(e, "index"):
                errs.append(e)
                return True
            raise  # not fn's (a transition or help failure): fatal
        if v is pyrei.TIMEOUT:
            return False
        handles[k] = None
        # one list carries every published runner result: the death
        # lost-set scan reads the batch histories, the clean path splices
        # the batch values
        runs.append(v)
        return True

    pending = list(range(len(handles)))
    while pending:
        progress = False
        still = []
        for k in pending:
            if expired():
                return pyrei.TIMEOUT
            # the verdict is the morsel-state code: 2 abandoned, 1
            # running, 0 idle
            verdict = _pyrei._map_abandon(capsule, k)
            if verdict == 2:
                # abandoned: never started and never will — cancel and
                # drop; a claim that lands anyway loses its first-call
                # CAS and publishes empty
                h = handles[k]
                if h is not None:
                    h.cancel()
                handles[k] = None
                done = True
            elif verdict == 1:
                done = consume(k, remaining())
            else:
                done = False  # idle: defer, the trim trigger unarmed
            if done:
                progress = True
            else:
                still.append(k)
        pending = still
        if pending and not progress:
            # every uncollected handle defers (nothing claimed yet, or
            # every worker pinned): park on one in bounded slices, so a
            # claim landing on a different handle — or exhaustion reached
            # while parked — is picked up within a slice
            if expired():
                return pyrei.TIMEOUT
            t = remaining()
            if t is None or t > 0.05:
                t = 0.05
            if consume(pending[0], t):
                pending.pop(0)
    if died is not None:
        # the lost set is arithmetic over the collected histories:
        # issued = [0, cursor), lost = issued minus their union
        lost = _pyrei._map_lost(capsule, [hist for hist, _ in runs])
        died.lost = lost
        raise died
    if errs:
        # first by element index among the runners that ran — the set
        # that ran already depended on steal order; the fail-fast store
        # only shrinks it sooner
        raise min(errs, key=lambda e: e.index)
    out = [None] * n
    for hist, vals in runs:
        for (lo, hi), batch in zip(hist, vals, strict=True):
            out[lo:hi] = batch
    return out
