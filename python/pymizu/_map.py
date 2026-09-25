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
``random.seed(SHA-256(seed_bytes + (i + offset).to_bytes(8, "little")))``,
with the worker's prior RNG state saved and restored around each batch.
``seed`` may be a ``(seed, offset)`` pair to shift every element's stream
by ``offset`` positions (maps split across runs or processes). Because the
streams are per-element, results are identical for any chunking, worker
count, or steal order. The spec rides the task payloads as a
``(seed_bytes, offset)`` tuple.
"""

from __future__ import annotations

import hashlib as _hashlib
import random as _random
import time as _time
from collections.abc import Callable as _Callable
from typing import TYPE_CHECKING
from typing import Any as _Any
from typing import Literal as _Literal

from pymizu import _pymizu

if TYPE_CHECKING:
    import pymizu

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

# The wire tags of the raw-x gate (mizu.h's MIZU_TYPE_*), and their numpy /
# memoryview spellings. Complex needs numpy (a memoryview cannot cast to
# it); the gate falls back to the descriptor path when numpy is absent.
_TAG_NP = {
    24: "uint8",
    14: "float64",
    13: "int32",
    10: "int32",
    15: "complex128",
    32: "int64",
}
_TAG_MV: dict[int, _Literal["B", "d", "i", "q"]] = {
    24: "B", 14: "d", 13: "i", 10: "i", 32: "q"
}

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
    __slots__ = ("capsule", "fn", "args", "kwargs", "get", "tmpl")

    def __init__(
        self,
        capsule: _Any,
        fn: _Callable[..., _Any],
        args: tuple,
        kwargs: dict,
        get: _Callable[[int], _Any],
        tmpl: bool,
    ) -> None:
        self.capsule = capsule
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.get = get
        self.tmpl = tmpl


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
        capsule = _pymizu._map_open(name)
        hdr = _pymizu._map_header(capsule)
        desc = _pickle.loads(_pymizu._map_desc(capsule))
        if hdr["x_kind"] == _X_RAWBUF:
            fn, args, kwargs = desc
            get = _raw_accessor(_pymizu._map_x_view(capsule), hdr["x_tag"])
        else:
            fn, args, kwargs, x = desc
            get = x.__getitem__
        ctx = _Ctx(capsule, fn, args, kwargs, get, hdr["out_tag"] != 0)
        _ctx_cache[name] = ctx
    return ctx


def _run_batch(
    fn: _Callable[..., _Any],
    args: tuple,
    kwargs: dict,
    get: _Callable[[int], _Any],
    lo: int,
    hi: int,
    seed_spec: tuple[bytes, int] | None,
) -> list:
    """One batch's element loop: fn(elt, *args, **kwargs) over [lo, hi).
    An escaping error is annotated with the in-flight element index (the
    "first by element index" contract — the worker's error envelope
    carries it as the fourth tuple element). Seeded: install element i's
    stream before its call; the worker's own RNG state is restored around
    the batch either way."""
    out = []
    if seed_spec is None:
        for i in range(lo, hi):
            try:
                out.append(fn(get(i), *args, **kwargs))
            except Exception as e:
                e._pymizu_map_index = i  # pyrefly: ignore [missing-attribute]
                raise
        return out
    seed_bytes, offset = seed_spec
    state = _random.getstate()
    try:
        for i in range(lo, hi):
            _random.seed(
                _hashlib.sha256(
                    seed_bytes + (i + offset).to_bytes(8, "little")
                ).digest()
            )
            try:
                out.append(fn(get(i), *args, **kwargs))
            except Exception as e:
                e._pymizu_map_index = i  # pyrefly: ignore [missing-attribute]
                raise
    finally:
        _random.setstate(state)
    return out


def _chunk(
    blob: bytes, lo: int, hi: int, seed_spec: tuple[bytes, int] | None
) -> list:
    """Worker-side blob-path chunk task: the inline descriptor blob (the
    sizes this path admits make a per-chunk unpickle negligible, so there
    is no cache), the 0-based half-open element range, and the seed."""
    fn, args, kwargs, x = _pickle.loads(blob)
    return _run_batch(fn, args, kwargs, x.__getitem__, lo, hi, seed_spec)


def _runner(
    region_name: str,
    ordinal: int,
    gen: int,
    seed_spec: tuple[bytes, int] | None,
) -> tuple[list, list | None]:
    """Worker-side morsel runner, riding each runner task. Opens (or
    reuses) the map context, obtains the pool signals worker-locally —
    never from the submitter — and loops batch transitions: claim the next
    adaptively sized batch off the shared cursor, answer the doorbell at
    the boundary when flagged, evaluate, and record the batch in the local
    history only after it completes (a batch interrupted by an error is
    never recorded — correctly lost). The history and per-batch value
    lists ride the runner's single ordinary result publish; on the
    template path values land in the region's output area instead and the
    publish carries the history alone."""
    import pymizu

    pool = pymizu.current_pool()
    assert pool is not None  # a runner always executes inside a task
    ctx = _map_ctx(region_name)
    sig = pool._h._signals()
    hist = []
    vals = []
    try:
        while True:
            nxt = _pymizu._map_next(ctx.capsule, sig, ordinal, gen)
            if nxt is None:
                break
            lo, hi, help_flag = nxt
            # doorbell: one foreign injection task between batches hands
            # concurrent submitters their chunk-boundary interleave back
            if help_flag:
                pool._h._help_once()
            batch = _run_batch(
                ctx.fn, ctx.args, ctx.kwargs, ctx.get, lo, hi, seed_spec
            )
            if ctx.tmpl:
                _pymizu._map_write(ctx.capsule, lo, batch)
            else:
                vals.append(batch)
            hist.append((lo, hi))
    except Exception:
        # the fail-fast store, ahead of the ERR publish: peers observe it
        # within ~a batch instead of draining the cursor first
        _pymizu._map_cancel_set(ctx.capsule)
        raise
    return (hist, None if ctx.tmpl else vals)


def _seed_spec(
    seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None,
) -> tuple[bytes, int] | None:
    """Normalize the public ``seed`` argument to the carried spec: a
    ``(seed_bytes, offset)`` tuple, element ``i`` drawing stream
    ``i + offset`` (the ``.seed = c(seed, offset)`` mirror)."""
    if seed is None:
        return None
    offset = 0
    if isinstance(seed, tuple):
        if len(seed) != 2:
            raise TypeError(
                "pymizu: seed must be an int, bytes, or a (seed, offset) pair"
            )
        seed, offset = seed
        if (
            isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
        ):
            raise TypeError("pymizu: seed offset must be a non-negative int")
    if isinstance(seed, bool) or not isinstance(seed, (int, bytes, bytearray)):
        raise TypeError("pymizu: seed must be an int or bytes")
    if isinstance(seed, int):
        return str(seed).encode("ascii"), offset
    return bytes(seed), offset


def _deadline(
    timeout: float | None,
) -> tuple[_Callable[[], float | None], _Callable[[], bool]]:
    deadline = None if timeout is None else _time.monotonic() + timeout

    def remaining() -> float | None:
        if deadline is None:
            return None
        return max(0.0, deadline - _time.monotonic())

    def expired() -> bool:
        return deadline is not None and _time.monotonic() >= deadline

    return remaining, expired


def _template_probe(template: _Any) -> tuple:
    """The template gate: an exemplar buffer of a supported dtype carries
    the output area's wire type and per-element length m together (the
    mirror of mizu's `.template` exemplar)."""
    probe = _pymizu._map_probe_x(template)
    if probe is None or probe[1] < 1:
        raise TypeError(
            "pymizu: template must be a C-contiguous buffer of a supported "
            "dtype (float64/int32/int64/complex128/uint8)"
        )
    if probe[0] == 15 and _np is None:
        raise TypeError("pymizu: a complex128 template needs numpy")
    return probe


def _wrap_out(raw: _Any, tag: int, n: int, m: int) -> _Any:
    """Assemble the gathered output area: a numpy array (n, m) — (n,) for
    m == 1 — when numpy is present, else a (cast) memoryview. `raw` is the
    gather bytes (copy) or the _MapOutView exporter (view)."""
    if _np is not None:
        a = _np.frombuffer(raw, dtype=_TAG_NP[tag])
        return a.reshape(n, m) if m > 1 else a
    fmt = _TAG_MV[tag]
    mv = memoryview(raw).cast(fmt)
    return mv.cast(fmt, [n, m]) if m > 1 else mv


def pool_map(
    pool: pymizu.Pool,
    fn: _Callable[..., _Any],
    x: _Any,
    args: _Any,
    kwargs: dict | None,
    chunks: int | None,
    seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None,
    timeout: float | None,
    template: _Any = None,
    collect: str | None = None,
) -> list | _pymizu._Sentinel | _Any:
    """The one map path: stage, submit the runners (or blob chunks),
    collect against the single deadline, splice into input order. The
    try/finally backstop cancels outstanding work on KeyboardInterrupt or
    error and unlinks the region explicitly (the capsule destructor is
    only the GC backstop)."""
    import pymizu

    if not callable(fn):
        raise TypeError("pymizu: fn must be callable")
    args = tuple(args)
    kwargs = {} if kwargs is None else dict(kwargs)
    seed_spec = _seed_spec(seed)

    tprobe = None if template is None else _template_probe(template)
    if tprobe is None:
        if collect not in (None, "list"):
            raise ValueError("pymizu: collect must be 'list' without template")
        collect = "list"
    else:
        if collect is None:
            collect = "copy"
        if collect not in ("copy", "view"):
            raise ValueError("pymizu: collect must be 'copy' or 'view'")

    # the raw-x gate: a C-contiguous buffer of a supported dtype rides the
    # region as bare bytes (complex needs numpy's frombuffer); anything
    # else pickles into the descriptor
    probe = _pymizu._map_probe_x(x)
    if probe is not None and probe[0] == 15 and _np is None:
        probe = None
    if probe is None:
        x = list(x)
        n = len(x)
    else:
        n = probe[1]
    if n == 0:
        if tprobe is None:
            return []
        return _wrap_out(b"", tprobe[0], 0, tprobe[1])
    if chunks is not None:
        if chunks < 1:
            raise ValueError("pymizu: chunks must be a positive number")

    live, free_rs, inj_cap, inline_entry = pool._h._map_caps()
    if free_rs == 0:
        raise pymizu.SlotsExhaustedError(
            "pymizu: result slots exhausted — collect or cancel outstanding "
            "tasks first"
        )

    remaining, expired = _deadline(timeout)

    # Region-less probe: does the full chunk payload — the wrapper plus
    # the descriptor blob as an ordinary argument — fit the entry inline
    # budget? Skipped when a raw x alone already exceeds the budget, so a
    # huge x is never pickled just to learn it does not fit. The template
    # path always needs the region (its output area lives there).
    blob = None
    if tprobe is None and (probe is None or probe[2] <= inline_entry):
        cand = _pickle.dumps((fn, args, kwargs, x), 4)
        worst = _pickle.dumps((_chunk, (cand, n, n, seed_spec), {}), 4)
        if len(worst) <= inline_entry:
            blob = cand

    handles = []
    box = {"capsule": None}  # _map_region hands its region back through
    try:  # here for the finally's cancel + unlink
        if blob is not None:
            return _map_blob(
                pool,
                pymizu,
                blob,
                n,
                seed_spec,
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
            pymizu,
            fn,
            args,
            kwargs,
            x,
            probe,
            n,
            seed_spec,
            chunks,
            live,
            free_rs,
            inj_cap,
            remaining,
            expired,
            handles,
            box,
            template,
            tprobe,
            collect,
        )
    finally:
        # the interrupt/error/timeout backstop (a clean collect consumed
        # every handle: this is then a no-op), then the explicit unlink
        capsule = box["capsule"]
        if capsule is not None:
            _pymizu._map_cancel_set(capsule)
        for h in handles:
            if h is not None:
                h.cancel()
        if capsule is not None:
            _pymizu._map_close(capsule)


def _map_blob(
    pool: pymizu.Pool,
    pymizu,
    blob: bytes,
    n: int,
    seed_spec: tuple[bytes, int] | None,
    chunks: int | None,
    live: int,
    free_rs: int,
    inj_cap: int,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
    handles: list,
) -> list | _pymizu._Sentinel:
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
            (_chunk, (blob, lo, hi, seed_spec), {}), remaining()
        )
        handles.append(h)
        lo = hi
    if timed_out:
        return pymizu.TIMEOUT
    out = []
    lo = 0
    for k in range(len(handles)):
        hi = lo + sizes[k]
        try:
            v = handles[k].collect(timeout=remaining())
        except pymizu.WorkerDiedError as e:
            e.lost = [(lo, hi)]
            raise
        if v is pymizu.TIMEOUT:
            return pymizu.TIMEOUT
        handles[k] = None
        out.extend(v)
        lo = hi
    return out


def _map_region(
    pool: pymizu.Pool,
    pymizu,
    fn: _Callable[..., _Any],
    args: tuple,
    kwargs: dict,
    x: _Any,
    probe: tuple | None,
    n: int,
    seed_spec: tuple[bytes, int] | None,
    chunks: int | None,
    live: int,
    free_rs: int,
    inj_cap: int,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
    handles: list,
    box: dict,
    template: _Any,
    tprobe: tuple | None,
    collect: str,
) -> list | _pymizu._Sentinel | _Any:
    """The region path: stage, submit one runner per live worker (clamped
    by the morsel count, the free result slots, and the injection ring),
    collect under the exhausted-runner trim, splice by element position —
    or, on the template path, gather the output area the runners wrote in
    place (one copy, or none for a view)."""
    runners = min(max(1, live), free_rs, inj_cap)
    if chunks is None:
        morsel = max(1, min(n // (runners * _MORSELS_PER_RUNNER), _MORSEL_CAP))
    else:
        morsel = -(-n // min(n, chunks))
    n_morsels = -(-n // morsel)
    desc = _pickle.dumps(
        (fn, args, kwargs) if probe is not None else (fn, args, kwargs, x), 4
    )
    name, capsule = _pymizu._map_stage(
        desc, x if probe is not None else None, n, morsel, template
    )
    # hand the region to pool_map's finally backstop (cancel + unlink)
    box["capsule"] = capsule
    r = min(n_morsels, runners)
    if _submit_runners(
        pool, name, r, 0, seed_spec, remaining, expired, handles
    ):
        return pymizu.TIMEOUT
    out = _collect_region(
        pymizu, capsule, handles, n, remaining, expired, tprobe is not None
    )
    if out is pymizu.TIMEOUT or tprobe is None:
        return out
    if collect == "view":
        # ownership of the region transfers to the view: the finally
        # backstop must not unlink it
        view = _wrap_out(_pymizu._map_gather_view(capsule), tprobe[0], n,
                         tprobe[1])
        box["capsule"] = None
        return view
    return _wrap_out(_pymizu._map_gather(capsule), tprobe[0], n, tprobe[1])


def _submit_runners(
    pool: pymizu.Pool,
    name: str,
    r: int,
    gen: int,
    seed_spec: tuple[bytes, int] | None,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
    handles: list,
) -> bool:
    """Submit the r runner tasks of one run; True when the deadline
    expired mid-submit (the caller's backstop cancels what landed). The
    payload carries the run's generation: a stale straggler from a prior
    run of a prepared map fails its first-call CAS against the re-armed
    CLAIM word."""
    for k in range(r):
        # pre-check, not just the verb's: a nested (worker-side) submit
        # never waits on ring space, so an expired deadline must be caught
        # here, before the payload is built
        if expired():
            return True
        h = pool._h._submit_runner(
            (_runner, (name, k, gen, seed_spec), {}), remaining()
        )
        handles.append(h)
    return False


def _collect_region(
    pymizu,
    capsule: _Any,
    handles: list,
    n: int,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
    tmpl: bool = False,
    gen: int = 0,
) -> list | _pymizu._Sentinel | None:
    """Collect the runner handles under the exhausted-runner trim, in a
    deferred collection order. A runner carries no work of its own, so
    once the cursor exhausts a still-queued runner is dead weight — but
    parking unboundedly on its handle would wait for a busy peer to claim
    and no-op it. So: each pass abandons what the armed trigger allows,
    collects handles whose CLAIM word reads RUNNING, and defers IDLE ones.
    Only when every uncollected handle defers does collect park, in
    bounded slices, re-scanning on each return."""
    errs = []
    died: pymizu.WorkerDiedError | None = None
    runs = []

    def consume(k: int, t: float | None) -> bool:
        nonlocal died
        h = handles[k]
        try:
            v = h.collect(timeout=t)
        except pymizu.WorkerDiedError as e:
            handles[k] = None
            # fail fast: peers stop within ~a batch (idempotent — an
            # erroring runner already stored this before its ERR publish)
            _pymizu._map_cancel_set(capsule)
            if died is None:
                died = e
            return True
        except pymizu.TaskError as e:
            handles[k] = None
            _pymizu._map_cancel_set(capsule)
            if hasattr(e, "index"):
                errs.append(e)
                return True
            raise  # not fn's (a transition or help failure): fatal
        if v is pymizu.TIMEOUT:
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
                return pymizu.TIMEOUT
            # the verdict is the morsel-state code: 2 abandoned, 1
            # running, 0 idle
            verdict = _pymizu._map_abandon(capsule, k, gen)
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
                return pymizu.TIMEOUT
            t = remaining()
            if t is None or t > 0.05:
                t = 0.05
            if consume(pending[0], t):
                pending.pop(0)
    if died is not None:
        # the lost set is arithmetic over the collected histories:
        # issued = [0, cursor), lost = issued minus their union
        lost = _pymizu._map_lost(capsule, [hist for hist, _ in runs])
        died.lost = lost
        raise died
    if errs:
        # first by element index among the runners that ran — the set
        # that ran already depended on steal order; the fail-fast store
        # only shrinks it sooner
        raise min(errs, key=lambda e: e.index)
    if tmpl:
        # results sit in the output area; the caller gathers
        return None
    out = [None] * n
    for hist, vals in runs:
        for (lo, hi), batch in zip(hist, vals, strict=True):
            out[lo:hi] = batch
    return out


class PreparedMap:
    """A map staged once into a persistent region, run many times
    (``Pool.map_prepare`` / ``Pool.map_run``). Re-arming is O(1) — a
    generation bump, a cursor reset, a cancel-word clear — and workers
    reuse their cached contexts, so a re-run pays neither the descriptor
    pickle nor the region create nor the worker-side re-attach. A
    view-collected run hands its region to the view; the next run
    restages into a fresh one (the mirror of mizu)."""

    def __init__(
        self,
        pool: pymizu.Pool,
        fn: _Callable[..., _Any],
        x: _Any,
        args: _Any,
        kwargs: dict | None,
        chunks: int | None,
        seed: int
        | bytes
        | bytearray
        | tuple[int | bytes | bytearray, int]
        | None,
        template: _Any,
        collect: str | None,
    ) -> None:
        import pymizu

        if not callable(fn):
            raise TypeError("pymizu: fn must be callable")
        self._pymizu = pymizu
        self._pool = pool
        args = tuple(args)
        kwargs = {} if kwargs is None else dict(kwargs)
        self._seed_spec = _seed_spec(seed)
        self._tprobe = None if template is None else _template_probe(template)
        if self._tprobe is None:
            if collect not in (None, "list"):
                raise ValueError(
                    "pymizu: collect must be 'list' without template"
                )
            self._collect_mode = "list"
        else:
            if collect is None:
                collect = "copy"
            if collect not in ("copy", "view"):
                raise ValueError("pymizu: collect must be 'copy' or 'view'")
            self._collect_mode = collect
        if chunks is not None:
            if chunks < 1:
                raise ValueError("pymizu: chunks must be a positive number")
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._chunks = chunks
        self._template = template
        self._set_x(x)
        self._capsule = None
        self._name = None
        self._gen = 0
        self._ran = False
        self._closed = False
        if self._n:
            self._stage()

    def _set_x(self, x: _Any) -> None:
        """(Re)target the map at x: probe for the raw section, rebuild the
        descriptor, and recompute the morsel geometry from the pool's
        current caps."""
        probe = _pymizu._map_probe_x(x)
        if probe is not None and probe[0] == 15 and _np is None:
            probe = None
        if probe is None:
            x = list(x)
            n = len(x)
        else:
            n = probe[1]
        self._probe = probe
        self._x = x
        self._n = n
        self._desc = _pickle.dumps(
            (self._fn, self._args, self._kwargs)
            if probe is not None
            else (self._fn, self._args, self._kwargs, x),
            4,
        )
        live, free_rs, inj_cap, _ = self._pool._h._map_caps()
        runners = max(1, min(max(1, live), free_rs, inj_cap))
        chunks = self._chunks
        if n == 0:
            morsel = 1
        elif chunks is None:
            morsel = max(
                1, min(n // (runners * _MORSELS_PER_RUNNER), _MORSEL_CAP)
            )
        else:
            morsel = -(-n // min(n, chunks))
        self._morsel = morsel
        self._n_morsels = -(-n // morsel) if n else 0

    def _swap_x(self, x: _Any) -> None:
        """Replace the staged x: an in-place memcpy over the region's x
        section when both the staged and the replacement x are raw-buffer
        eligible with the same wire type and element count (a raw x is
        sliced from the mapping per batch, never cached worker-side, so
        the swap is invisible to the workers) — anything else drops the
        staged state and the next run restages (a descriptor-carried x IS
        cached worker-side, so a shape or kind change must re-key the
        region)."""
        probe = _pymizu._map_probe_x(x)
        if probe is not None and probe[0] == 15 and _np is None:
            probe = None
        if (
            self._capsule is not None
            and self._probe is not None
            and probe is not None
            and probe[0] == self._probe[0]
            and probe[1] == self._n
        ):
            _pymizu._map_swap_x(self._capsule, x)
            self._x = x
            return
        self._set_x(x)
        # drop the staged region: the capsule destructor unlinks it, and a
        # straggler against it dies at its stale-generation claim word
        self._capsule = None
        self._name = None

    def _stage(self) -> None:
        name, capsule = _pymizu._map_stage(
            self._desc,
            self._x if self._probe is not None else None,
            self._n,
            self._morsel,
            self._template,
        )
        self._name, self._capsule, self._gen = name, capsule, 0

    def run(self, x: _Any = None, timeout: float | None = None) -> list | _Any:
        """Run the prepared map once; return its results (the same shapes
        and outcome taxonomy as ``Pool.map``).

        ``x`` replaces the staged data for this and later runs. When both
        the staged and the replacement x are raw-buffer eligible with the
        same dtype and length, the swap is an in-place memcpy over the
        region's x section — the iterate-over-same-shape loop (optimizer
        steps, simulation sweeps) runs at memcpy cost, skipping the region
        create and the worker-side re-attach. Any other x restages
        transparently on this run."""
        pymizu = self._pymizu
        if self._closed:
            raise pymizu.MizuError("pymizu: map handle is closed")
        if x is not None:
            self._swap_x(x)
        if self._n == 0:
            if self._tprobe is None:
                return []
            return _wrap_out(b"", self._tprobe[0], 0, self._tprobe[1])
        if self._capsule is None:
            # a view-collected run transferred the region: restage fresh
            self._stage()
        elif self._ran:
            self._gen = _pymizu._map_reset(self._capsule)
        self._ran = True
        pool = self._pool
        live, free_rs, inj_cap, _ = pool._h._map_caps()
        if free_rs == 0:
            raise pymizu.SlotsExhaustedError(
                "pymizu: result slots exhausted — collect or cancel "
                "outstanding tasks first"
            )
        remaining, expired = _deadline(timeout)
        r = min(self._n_morsels, max(1, live), free_rs, inj_cap)
        handles = []
        name = self._name
        assert name is not None  # n > 0 stages at prepare / restage above
        try:
            if _submit_runners(
                pool, name, r, self._gen, self._seed_spec,
                remaining, expired, handles,
            ):
                return pymizu.TIMEOUT
            out = _collect_region(
                pymizu, self._capsule, handles, self._n, remaining, expired,
                self._tprobe is not None, self._gen,
            )
            if out is pymizu.TIMEOUT or self._tprobe is None:
                return out
            if self._collect_mode == "view":
                view = _wrap_out(
                    _pymizu._map_gather_view(self._capsule),
                    self._tprobe[0], self._n, self._tprobe[1],
                )
                # ownership transferred to the view; the next run restages
                self._capsule = None
                return view
            return _wrap_out(
                _pymizu._map_gather(self._capsule),
                self._tprobe[0], self._n, self._tprobe[1],
            )
        finally:
            # the interrupt/error/timeout backstop (a clean collect
            # consumed every handle: a no-op then); the region survives —
            # the next run's re-arm clears the cancel word
            pending = [h for h in handles if h is not None]
            if pending:
                if self._capsule is not None:
                    _pymizu._map_cancel_set(self._capsule)
                for h in pending:
                    h.cancel()

    def close(self) -> None:
        """Unlink the staged region (idempotent; the GC backstop is the
        capsule destructor)."""
        self._closed = True
        if self._capsule is not None:
            _pymizu._map_close(self._capsule)
            self._capsule = None

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> PreparedMap:
        return self

    def __exit__(self, *exc: _Any) -> None:
        self.close()
