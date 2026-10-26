"""Pool.map orchestration (mirrors the R package's map.R).

One call stages fn / args / kwargs / x once — a single pickled descriptor
stream in one fresh map region — or entirely inline in chunk tasks when it
fits the entry inline budget (the region-less blob path). It then submits
one *runner* task per live worker: runners self-schedule element ranges
off the shared cursor in the map region (morsel-driven scheduling), one
adaptive-sized batch per `_map_next` transition, and publish their batch
histories and values as their single ordinary result. Workers materialize
the map context at most once each (a small cache keyed by region name); a
raw-buffer x is wrapped once per worker and indexed per element, never
deserialized.

Seeding: `seed` derives deterministic per-element streams of the stdlib
`random` module — element `i` runs under
`random.seed(SHA-256(seed_bytes + (i + offset).to_bytes(8, "little")))`,
with the worker's prior RNG state saved and restored around each batch.
`seed` may be a `(seed, offset)` pair to shift every element's stream
by `offset` positions (maps split across runs or processes). Because the
streams are per-element, results are identical for any chunking, worker
count, or steal order. The spec rides the task payloads as a
`(seed_bytes, offset)` tuple.

`seed` covers the stdlib `random` module only: a task drawing from
numpy calls [`current_rng()`](`pymizu.current_rng`) — the running
element's memoized `numpy.random.Generator`, derived lazily from the
same seed material (the entropy is domain-separated, so the stdlib
streams above are unchanged). The legacy `np.random.*` module functions
draw from the worker's shared global RandomState and stay
order-dependent; other RNG universes are out of scope. A seeded element
must not nested-submit and collect: the helped batch's stdlib
save/restore nests inside this batch's, but its stash clear wipes this
element's — a later `current_rng()` call here rebuilds from the digest,
restarting the stream instead of continuing it.
"""

from __future__ import annotations

import hashlib as _hashlib
import math as _math
import random as _random
import time as _time
from collections.abc import Callable as _Callable
from typing import TYPE_CHECKING
from typing import Any as _Any
from typing import Literal as _Literal
from typing import NoReturn as _NoReturn

from pymizu import _LANG_PYTHON, _LANG_R, _pymizu, _split_source

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

# The runner stream's gen_field packing: (ordinal << 32) | generation.
_GEN_SHIFT = 32

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
    24: "B",
    14: "d",
    13: "i",
    10: "i",
    32: "q",
}
_TAG_COMPLEX = 15

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
    __slots__ = ("capsule", "fn", "args", "kwargs", "get", "tmpl", "claim_n")

    def __init__(
        self,
        capsule: _Any,
        fn: _Callable[..., _Any],
        args: tuple,
        kwargs: dict,
        get: _Callable[[int], _Any] | None,
        tmpl: bool,
        claim_n: int,
    ) -> None:
        self.capsule = capsule
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.get = get
        self.tmpl = tmpl
        self.claim_n = claim_n


_ctx_cache: dict[str, _Ctx] = {}


def _sweep_ctx_cache() -> None:
    """Drop every cached map context: the worker's idle sweep hook (the C
    binding's ``sweep`` callback), so a parked worker holds no map-region
    mappings or view-x zc loans."""
    _ctx_cache.clear()


def _map_check_native(pool: _Any, fn: _Any) -> tuple[bool, int]:
    """The map guard for a foreign pool: a native fn fails fast at the
    entry point (its runner tasks are same-language private frames that
    would otherwise each fail remotely, one error per runner). A spec fn
    takes the cross-language path on any pool (the 'I' descriptor and
    kind-2 runner tasks), and needs the pool word already set: the
    descriptor's target byte stages once, at stage time.

    Returns
    -------
        The `(is_spec, worker_language)` pair.
    """
    import pymizu

    ident = pool._h._worker_ident()
    if isinstance(fn, pymizu.call):
        if ident is None:
            raise pymizu.MizuError("pymizu: no worker has joined this pool")
        return True, ident[0]
    if ident is None or ident[0] == _LANG_PYTHON:
        return False, _LANG_PYTHON
    raise TypeError(
        "pymizu: this pool's workers are not Python — Pool.map() needs a "
        "pymizu.call() spec as 'fn' on a foreign pool"
    )


def _seed_pair(
    seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None,
) -> tuple[int | bytes | bytearray, int] | None:
    """The seed shape gate shared by both carried forms: a `(seed,
    offset)` pair unpacks, a bare seed takes offset 0; a bool is neither
    an int seed nor bytes."""
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
    return seed, offset


def _seed_wire(
    seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None,
    lang: int,
) -> tuple[int, int] | None:
    """The spec-map seed gate: the kind-2 runner fields carry the
    language-neutral `(seed, offset)` i64 pair, so a spec map takes int
    seeds only — a bytes seed has no i64 form. On R workers the int must
    fit R's 32-bit derivation range and the offset must not exceed 2^53
    (R derives per-element streams in doubles, exact only to 2^53); the
    local error beats one per runner."""
    pair = _seed_pair(seed)
    if pair is None:
        return None
    seed, offset = pair
    if isinstance(seed, (bytes, bytearray)):
        raise TypeError(
            "pymizu: a bytes seed has no i64 form on a spec map — pass an "
            "int seed"
        )
    if not -(2**63) <= seed < 2**63 or offset >= 2**63:
        raise TypeError("pymizu: seed and offset must fit an int64")
    if lang == _LANG_R and abs(seed) > 2**31 - 1:
        raise TypeError(
            "pymizu: an int seed on R workers must fit R's 32-bit "
            "derivation range"
        )
    if lang == _LANG_R and offset > 2**53:
        raise TypeError(
            "pymizu: a seed offset on R workers must not exceed 2^53 — "
            "doubles cannot represent larger integers exactly"
        )
    return (seed, offset)


def _resolve_name(code: str) -> _Callable[..., _Any]:
    """Resolve a name-kind spec's qualified name through the worker's own
    module machinery (there is no name registry): split at the last dot,
    import the module, take the attribute."""
    import importlib

    modname, dot, attr = code.rpartition(".")
    if not dot or not modname or not attr:
        import pymizu

        raise pymizu.MizuError(
            "pymizu: malformed map descriptor: the task name is not qualified"
        )
    return getattr(importlib.import_module(modname), attr)


def _source_fn(code: str, positional: list, named: dict) -> _Callable:
    """A source-kind spec as the per-element function: the ast split once
    per runner (exec the prefix, eval the trailing expression — the
    source convention), each element bound as x in a fresh namespace over
    the constant arguments, matching a task's own fresh namespace."""
    prefix, tail = _split_source(code)
    prefix_code = (
        compile(prefix, "<map>", "exec") if prefix is not None else None
    )
    tail_code = compile(tail, "<map>", "eval") if tail is not None else None
    base_ns: dict[str, _Any] = {"__builtins__": __builtins__}
    for i, v in enumerate(positional):
        base_ns[f"_{i + 1}"] = v
    base_ns.update(named)

    def fn(x: _Any) -> _Any:
        ns = dict(base_ns)
        ns["x"] = x
        if prefix_code is not None:
            exec(prefix_code, ns)
        if tail_code is None:
            return None
        return eval(tail_code, ns)

    return fn


def _runner_ix(
    region_name: str, gen_field: int, seed_pair: tuple[int, int] | None
) -> tuple[list, list | None]:
    """Worker-side kind-2 runner entry (the cross-language map): the exec
    hook hands the decoded runner stream's fields here — the ordinal and
    generation packed in one i64 (the ordinal the high 32 bits), the
    (seed, offset) pair when seeded. The runner is always same-language
    as the worker: unpack and run the native loop, rebuilding this
    language's own seed spec from the neutral pair (an int seed's bytes
    are its ascii form, exactly as `_seed_spec` builds them)."""
    import pymizu

    k = gen_field >> _GEN_SHIFT
    gen = gen_field % (1 << _GEN_SHIFT)
    ctx = _map_ctx(region_name)
    if k >= ctx.claim_n:
        raise pymizu.MizuError(
            "pymizu: malformed runner stream: the runner ordinal is out "
            "of range"
        )
    seed_spec = (
        None
        if seed_pair is None
        else (str(seed_pair[0]).encode("ascii"), seed_pair[1])
    )
    return _runner(region_name, k, gen, seed_spec)


def _view_accessor(x: _Any) -> _Callable[[int], _Any]:
    """The element accessor for a descriptor x: a resolved view reads
    off the shared pages (an ndarray with numpy, a memoryview without), a
    string view's one explicit copy, a plain list its own indexing."""
    if _pymizu._view_check(x):
        if hasattr(x, "to_list"):
            return x.to_list().__getitem__
        if _np is None:
            return memoryview(x).__getitem__
    return x.__getitem__


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
        raw = _pymizu._map_desc(capsule)
        raw_x = hdr["x_kind"] == _X_RAWBUF
        if raw[0] == 0x49:  # 'I': the interchange descriptor (a spec map)
            kind, code, positional, kwargs, x = _pymizu._map_desc_read(raw)
            if kind == 0:
                fn = _resolve_name(code)
                args = tuple(positional)
            else:
                fn = _source_fn(code, positional, kwargs)
                args, kwargs = (), {}
            get = (
                _raw_accessor(_pymizu._map_x_view(capsule), hdr["x_tag"])
                if raw_x
                else _view_accessor(x)
            )
        elif raw_x:
            fn, args, kwargs = _pickle.loads(raw)
            get = _raw_accessor(_pymizu._map_x_view(capsule), hdr["x_tag"])
        else:
            d = _pickle.loads(raw)
            if len(d) == 3:
                # a streaming map's descriptor: x never staged, the chunk
                # tasks bring their own slices
                fn, args, kwargs = d
                get = None
            else:
                fn, args, kwargs, x = d
                get = x.__getitem__
        ctx = _Ctx(
            capsule,
            fn,
            args,
            kwargs,
            get,
            hdr["out_tag"] != 0,
            hdr["claim_n"],
        )
        _ctx_cache[name] = ctx
    return ctx


# The worker-local seeded-element stash and current_rng()'s one-slot
# memo. _run_batch's seeded loop stashes the running element's
# (seed_bytes, i + offset) and clears it, with the memo key, around the
# batch; workers are single-threaded per the GIL policy, so module state
# is safe. The memo dies with the stash: a key change rebuilds, so a key
# recurring on the worker (a re-armed prepared run's element) restarts
# its stream instead of continuing a stale one — a keyed dict would only
# grow unboundedly.
_elt_key: tuple[bytes, int] | None = None
_rng_key: tuple[bytes, int] | None = None
_rng_gen: _Any = None


def _current_rng() -> _Any:
    """[`current_rng()`](`pymizu.current_rng`)'s worker side: the stashed
    element's memoized numpy Generator, built lazily on first call in
    the element — the digest is computed only on call, so a seeded map
    that never calls pays the stash's two attribute writes per element,
    never a SHA-256. The numpy entropy is domain-separated (the "np\\0"
    prefix) from the stdlib digest, which stays byte-identical."""
    global _rng_key, _rng_gen
    key = _elt_key
    if key is None:
        return None
    if _rng_key != key:
        if _np is None:
            raise TypeError("pymizu: current_rng() needs numpy (install it)")
        seed_bytes, elt = key
        entropy = int.from_bytes(
            _hashlib.sha256(
                b"np\x00" + seed_bytes + elt.to_bytes(8, "little")
            ).digest(),
            "little",
        )
        _rng_gen = _np.random.default_rng(_np.random.SeedSequence(entropy))
        _rng_key = key
    return _rng_gen


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
    stream before its call and stash its (seed_bytes, i + offset) key
    for [`current_rng()`](`pymizu.current_rng`); the worker's own RNG
    state is restored, and the stash and memo cleared, around the batch
    either way."""
    global _elt_key, _rng_key
    out = []

    def call_one(i: int) -> None:
        try:
            out.append(fn(get(i), *args, **kwargs))
        except Exception as e:
            e._pymizu_map_index = i  # pyrefly: ignore [missing-attribute]
            raise

    if seed_spec is None:
        for i in range(lo, hi):
            call_one(i)
        return out
    seed_bytes, offset = seed_spec
    state = _random.getstate()
    try:
        for i in range(lo, hi):
            _elt_key = (seed_bytes, i + offset)
            _random.seed(
                _hashlib.sha256(
                    seed_bytes + (i + offset).to_bytes(8, "little")
                ).digest()
            )
            call_one(i)
    finally:
        _elt_key = _rng_key = None
        _random.setstate(state)
    return out


class _Star:
    """[`Pool.starmap()`](`pymizu.Pool.starmap`)'s fn wrapper: unpacks
    the element into the call's positional slots
    (`multiprocessing.Pool.starmap` semantics). A module-level class so
    the descriptor pickle crosses by reference."""

    __slots__ = ("fn",)

    def __init__(self, fn: _Callable[..., _Any]) -> None:
        self.fn = fn

    def __call__(self, elt: _Any, *args: _Any, **kwargs: _Any) -> _Any:
        return self.fn(*elt, *args, **kwargs)


def _chunk(
    blob: bytes, lo: int, hi: int, seed_spec: tuple[bytes, int] | None
) -> list:
    """Worker-side blob-path chunk task: the inline descriptor blob (the
    sizes this path admits make a per-chunk unpickle negligible, so there
    is no cache), the 0-based half-open element range, and the seed."""
    fn, args, kwargs, x = _pickle.loads(blob)
    return _run_batch(fn, args, kwargs, x.__getitem__, lo, hi, seed_spec)


def _stream_chunk(
    name: str,
    lo: int,
    hi: int,
    sl: _Any,
    seed_spec: tuple[bytes, int] | None,
) -> list | None:
    """Worker-side streaming chunk task, riding an importable reference:
    the map context comes from the worker's name-keyed cache (one attach
    per worker per map), then the existing `_run_batch` runs over the
    slice with the global [lo, hi) — seeding needs no seek machinery,
    the per-element SHA-256 key being the global index i + offset.
    Template results write into the region's output area (the publish is
    None); generic results publish as the chunk's ordinary result."""
    ctx = _map_ctx(name)
    batch = _run_batch(
        ctx.fn,
        ctx.args,
        ctx.kwargs,
        lambda i: sl[i - lo],
        lo,
        hi,
        seed_spec,
    )
    if ctx.tmpl:
        _pymizu._map_write(ctx.capsule, lo, batch)
        return None
    return batch


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
    assert ctx.get is not None  # a runner region always stages x
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
    """Normalize the public `seed` argument to the carried spec: a
    `(seed_bytes, offset)` tuple, element `i` drawing stream
    `i + offset` (the `.seed = c(seed, offset)` mirror)."""
    pair = _seed_pair(seed)
    if pair is None:
        return None
    seed, offset = pair
    if isinstance(seed, int):
        return str(seed).encode("ascii"), offset
    return bytes(seed), offset


def _deadline(
    timeout: float | None,
) -> tuple[_Callable[[], float | None], _Callable[[], bool]]:
    # as the verbs' timeout_ms_of: a NaN must not reach the deadline math
    # (it never expires, and remaining() degrades to a non-blocking poll)
    if timeout is not None and _math.isnan(timeout):
        raise ValueError(
            "pymizu: timeout must be a number of seconds, not NaN"
        )
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
    probe = _pymizu._map_probe_x(template, False)
    if probe is None or probe[1] < 1:
        raise TypeError(
            "pymizu: template must be a C-contiguous buffer of a supported "
            "dtype (float64/int32/int64/complex128/uint8)"
        )
    if probe[0] == _TAG_COMPLEX and _np is None:
        raise TypeError("pymizu: a complex128 template needs numpy")
    return probe


def _wrap_out(raw: _Any, tag: int, n: int, m: int) -> _Any:
    """Assemble the gathered output area: a numpy array (n, m) — (n,) for
    m == 1 — when numpy is present, else a (cast) memoryview. `raw` is
    the gather bytes (copy) or the `_MapOutView` exporter (view)."""
    if _np is not None:
        a = _np.frombuffer(raw, dtype=_TAG_NP[tag])
        return a.reshape(n, m) if m > 1 else a
    fmt = _TAG_MV[tag]
    mv = memoryview(raw).cast(fmt)
    return mv.cast(fmt, [n, m]) if m > 1 else mv


def _probe_x(
    spec: bool, x: _Any, convert: bool = False, stream: bool = False
) -> tuple[tuple | None, _Any, int]:
    """The raw-x gate, shared by the one-shot entry and the prepared
    handle: a received view x on a spec map crosses as one ref leaf (F1's
    D6 — the workers read the resolved view off the shared pages), or
    slices as the buffer it is when streaming; otherwise a C-contiguous
    buffer of a supported dtype rides the region as bare bytes (complex
    needs numpy's frombuffer) — with `convert` (a spec map on R workers)
    a 1-D non-identity buffer after one conversion; anything else
    pickles into the descriptor as a list.

    Returns
    -------
        The `(probe, x, n)` triple.
    """
    view_x = spec and not stream and _pymizu._view_check(x)
    probe = None if view_x else _pymizu._map_probe_x(x, convert)
    if probe is not None and probe[0] == _TAG_COMPLEX and _np is None:
        probe = None
    if probe is None:
        if not view_x:
            x = list(x)
        n = len(x)
    else:
        n = probe[1]
    return probe, x, n


def _write_desc(
    spec: bool,
    fn: _Any,
    args: tuple,
    kwargs: dict,
    x: _Any,
    probe: tuple | None,
    lang: int,
) -> bytes:
    """The staged descriptor bytes: a spec map's 'I' interchange form
    (the fn spec nested as a task tag, the list-x bare or nil — its target
    byte stages once, here), else the pickled (fn, args, kwargs[, x])
    tuple, x omitted when it rides the region's raw section."""
    if spec:
        from pymizu import call as _Call

        assert isinstance(fn, _Call)
        return _pymizu._map_desc_write(
            fn.code,
            fn.kind,
            fn.args,
            fn.kwargs,
            None if probe is not None else x,
            lang,
        )
    return _pickle.dumps(
        (fn, args, kwargs) if probe is not None else (fn, args, kwargs, x),
        4,
    )


class _Plan:
    """The validated map entry, shared by `pool_map` and
    [`PreparedMap`](`pymizu.PreparedMap`): the spec/language verdict,
    normalized args/kwargs, the carried seed form (the wire pair on a
    spec map, `(seed_bytes, offset)` otherwise), the template probe, the
    resolved collect mode, n_chunks, the streaming verdict, and the
    conversion gate (a spec map on R workers converts non-identity
    buffers at stage time, the channel's policy)."""

    __slots__ = (
        "spec",
        "lang",
        "args",
        "kwargs",
        "seed",
        "tprobe",
        "collect",
        "n_chunks",
        "stream",
        "convert",
    )

    def __init__(
        self,
        pool: _Any,
        fn: _Any,
        args: _Any,
        kwargs: dict | None,
        n_chunks: int | None,
        seed: _Any,
        template: _Any,
        collect: str | None,
        stream: bool = False,
    ) -> None:
        spec, lang = _map_check_native(pool, fn)
        if not spec and not callable(fn):
            raise TypeError("pymizu: fn must be callable")
        args = tuple(args)
        kwargs = {} if kwargs is None else dict(kwargs)
        if spec and (args or kwargs):
            raise TypeError(
                "pymizu: constant arguments ride the pymizu.call() spec — "
                "'args' and 'kwargs' must be empty with a spec 'fn'"
            )
        self.spec = spec
        self.lang = lang
        self.args = args
        self.kwargs = kwargs
        # the convert gate is the worker language, never 'spec' alone
        self.convert = spec and lang == _LANG_R
        self.seed = _seed_wire(seed, lang) if spec else _seed_spec(seed)
        self.tprobe = None if template is None else _template_probe(template)
        if self.tprobe is None:
            if collect not in (None, "list"):
                raise ValueError(
                    "pymizu: collect must be 'list' without template"
                )
            collect = "list"
        else:
            if collect is None:
                collect = "copy"
            if collect not in ("copy", "view"):
                raise ValueError("pymizu: collect must be 'copy' or 'view'")
        if spec and stream and collect == "view":
            raise TypeError(
                "pymizu: collect='view' cannot compose a streaming spec "
                "map — per-chunk output areas cannot compose into one view"
            )
        self.collect = collect
        if n_chunks is not None and n_chunks < 1:
            raise ValueError("pymizu: n_chunks must be a positive number")
        self.n_chunks = n_chunks
        self.stream = stream


def pool_map(
    pool: pymizu.Pool,
    fn: _Callable[..., _Any],
    x: _Any,
    args: _Any,
    kwargs: dict | None,
    n_chunks: int | None,
    seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None,
    timeout: float | None,
    template: _Any = None,
    collect: str | None = None,
    stream: bool = False,
) -> _Map | _pymizu.Sentinel:
    """The one map entry: stage, submit the runners (or blob chunks, or
    the streaming window's first fill), and return the `_Map` handle —
    collection is the handle's (`collect()` blocking, `collect_async()`
    event-driven). `timeout` bounds submission only: on its expiry the
    tasks already in are cancelled and the TIMEOUT sentinel returns (no
    handle); the collection deadline is `collect()`'s. An error raises at
    the call site with the same backstop."""
    import pymizu

    plan = _Plan(
        pool, fn, args, kwargs, n_chunks, seed, template, collect, stream
    )
    probe, x, n = _probe_x(plan.spec, x, plan.convert, plan.stream)
    if n == 0:
        return _Map._empty(pool, plan)

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
    # path always needs the region (its output area lives there), and a
    # spec fn always stages one (the inline chunk tasks are same-language
    # private frames a foreign worker cannot run).
    blob = None
    if (
        not plan.spec
        and not plan.stream
        and plan.tprobe is None
        and (probe is None or probe[2] <= inline_entry)
    ):
        cand = _pickle.dumps((fn, plan.args, plan.kwargs, x), 4)
        worst = _pickle.dumps((_chunk, (cand, n, n, plan.seed), {}), 4)
        if len(worst) <= inline_entry:
            blob = cand

    m = _Map(pool, plan)
    m.n = n
    m.live = live
    try:
        if plan.stream:
            timed_out = m._open_stream(
                pymizu, fn, x, probe, template, free_rs, remaining, expired
            )
        elif blob is not None:
            timed_out = m._open_blob(
                pymizu, blob, live, free_rs, inj_cap, remaining, expired
            )
        else:
            timed_out = m._open_region(
                pymizu,
                fn,
                x,
                probe,
                template,
                free_rs,
                inj_cap,
                remaining,
                expired,
            )
    except BaseException:
        m._abort()
        raise
    if timed_out:
        m._abort()
        return pymizu.TIMEOUT
    return m


def _stream_geometry(n: int, c: int) -> list:
    """The fixed chunk ranges of a streaming map: c balanced 0-based
    half-open [lo, hi) spans over n (the blob path's arithmetic)."""
    size, extra = divmod(n, c)
    sizes = [size + 1] * extra + [size] * (c - extra)
    ranges = []
    lo = 0
    for s in sizes:
        ranges.append((lo, lo + s))
        lo += s
    return ranges


def _stream_slicer(
    x: _Any, probe: tuple | None
) -> _Callable[[int, int], _Any]:
    """The submitter-side slicer: a buffer x slices as a view (a numpy
    slice is a view; a memoryview slice keeps bytes/bytearray copy-free),
    anything else slices shallow — bounded either way. No ALTREP trap
    exists here: Python views never copy until staged."""
    if probe is None:
        return lambda lo, hi: x[lo:hi]
    if _np is not None and isinstance(x, _np.ndarray):
        return lambda lo, hi: x[lo:hi]
    mv = memoryview(x)
    return lambda lo, hi: mv[lo:hi]


def _stream_fail(
    pymizu,
    handles: list,
    oi: list,
    ranges: list,
    first: _Any,
    capsules: list | None = None,
) -> _NoReturn:
    """The fail path of the streaming window, NORET: cancel the
    outstanding chunks, then drain them non-blockingly. A blind collect
    of a just-cancelled task races the worker's cancel consume (its
    publish or claim skip frees the slot, which a collect reads as
    already collected), so each sibling is probed first and only a
    stable terminal outcome (ok / err / died) is collected — pending and
    cancelled siblings carry no outcome and are ignored. A sibling death
    takes precedence, its lost range read off the drained position;
    otherwise the minimum element index among the observed errors raises
    (the erroring chunk's own `index` is the element index —
    [`Pool.collect_any()`](`pymizu.Pool.collect_any`)'s position stamp
    defers to it). An error carrying no element index is not fn's
    (infrastructure) and stays fatal, as the runner path's.

    The spec window passes `capsules`: each outstanding chunk's region
    cancel word is set ahead of `cancel()`, and the chunk-local element
    indices — `first`'s (its chunk read off `position`, collect_any's
    unconditional stamp) and the drained siblings' — are offset to the
    global element index before the minimum raises."""
    if capsules is not None:
        first.index += ranges[oi[first.position]][0]
    errs = [first]
    died = None
    lost = []
    for k in oi:
        if capsules is not None:
            cap = capsules[k]
            if cap is not None:
                _pymizu._map_cancel_set(cap)
        h = handles[k]
        if h is not None:
            h.cancel()
    for k in oi:
        h = handles[k]
        if h is None:
            continue
        handles[k] = None
        if h.state not in ("ok", "err", "died"):
            continue
        try:
            h.collect(timeout=0)
        except pymizu.WorkerDiedError as e:
            if died is None:
                died = e
            lost.append(ranges[k])
        except pymizu.TaskError as e:
            if not hasattr(e, "index"):
                raise
            if capsules is not None:
                e.index += ranges[k][0]
            errs.append(e)
    if died is not None:
        died.lost = lost
        raise died
    raise min(errs, key=lambda e: e.index)


class _StreamWindow:
    """The same-language sliding submit/collect window over one staged
    streaming map, split at the handle-first seam: `refill` submits
    outstanding chunk tasks up to the window width (unflagged, like blob
    chunks) — the prime at map() time, the top-up per completion at
    collect() time — and `drive` loops on
    [`Pool.collect_any()`](`pymizu.Pool.collect_any`) over the
    outstanding set, splicing generic results into place (template
    results are already in the region's output area; the chunk's None
    result is drained and dropped). The collect deadline threads its
    submits and collects; expiry returns the sentinel."""

    __slots__ = (
        "spec",
        "pool",
        "pymizu",
        "name",
        "ranges",
        "w",
        "seed_spec",
        "slicer",
        "tmpl",
        "handles",
        "oi",
        "next_k",
    )

    def __init__(
        self,
        pool: pymizu.Pool,
        pymizu,
        name: str,
        ranges: list,
        w: int,
        seed_spec: tuple[bytes, int] | None,
        slicer: _Callable[[int, int], _Any],
        tmpl: bool,
    ) -> None:
        self.spec = False
        self.pool = pool
        self.pymizu = pymizu
        self.name = name
        self.ranges = ranges
        self.w = w
        self.seed_spec = seed_spec
        self.slicer = slicer
        self.tmpl = tmpl
        self.handles: list = [None] * len(ranges)
        self.oi: list[int] = []
        self.next_k = 0

    def refill(
        self,
        remaining: _Callable[[], float | None],
        expired: _Callable[[], bool],
    ) -> bool:
        c = len(self.ranges)
        while self.next_k < c and len(self.oi) < self.w:
            # pre-check, not just the verb's: a nested (worker-side)
            # submit never waits on ring space, so an expired deadline
            # must be caught here, before the slice is built
            if expired():
                return False
            lo, hi = self.ranges[self.next_k]
            try:
                h = self.pool._h.submit(
                    (
                        _stream_chunk,
                        (
                            self.name,
                            lo,
                            hi,
                            self.slicer(lo, hi),
                            self.seed_spec,
                        ),
                        {},
                    ),
                    remaining(),
                )
            except self.pymizu.SubmitTimeoutError:
                # ring-full past the deadline is the map's timeout
                return False
            self.handles[self.next_k] = h
            self.oi.append(self.next_k)
            self.next_k += 1
        return True

    def drive(
        self,
        remaining: _Callable[[], float | None],
        expired: _Callable[[], bool],
    ) -> list | _pymizu.Sentinel | None:
        pymizu = self.pymizu
        n = self.ranges[-1][1]
        out = None if self.tmpl else [None] * n
        while self.oi:
            if expired():
                return pymizu.TIMEOUT
            try:
                got = self.pool._h.collect_any(
                    [self.handles[k] for k in self.oi], remaining()
                )
            except pymizu.WorkerDiedError as e:
                # the position stamp names the dead chunk (a death carries
                # no element index); the lost set is its fixed range —
                # the blob-path precedent
                e.lost = [self.ranges[self.oi[e.index]]]
                raise
            except pymizu.TaskError as e:
                _stream_fail(pymizu, self.handles, self.oi, self.ranges, e)
            if got is pymizu.TIMEOUT:
                return pymizu.TIMEOUT
            idx, v = got
            k = self.oi.pop(idx)
            self.handles[k] = None
            if not self.tmpl:
                assert out is not None  # allocated on the generic path
                lo, hi = self.ranges[k]
                out[lo:hi] = v
            if not self.refill(remaining, expired):
                return pymizu.TIMEOUT
        return out


class _StreamWindowSpec:
    """The spec streaming window: a foreign worker cannot run a
    same-language chunk task, so each in-flight chunk stages its own
    small map region ('I' descriptor, one-morsel geometry) with exactly
    one kind-2 runner (ordinal 0, generation 0). Parallelism is
    unchanged: W chunks in flight, one worker each. Each chunk's region
    unlinks at its own turn, so submitter residency stays at W regions.
    The runner result arrives in the worker language's shape: R workers'
    triples normalize through `_runs_to_spans` (chunk-local ms = n_k),
    Python workers' (ranges, values) pairs splice as they are — spans
    offset by the chunk base either way. Template chunks gather their
    per-chunk output area at the turn (gathering at the end would break
    the residency bound). Split like `_StreamWindow`: `refill` primes at
    map() time and tops up per completion, `drive` collects."""

    __slots__ = (
        "spec",
        "pool",
        "pymizu",
        "ranges",
        "w",
        "plan",
        "fn",
        "x",
        "probe",
        "template",
        "capsules",
        "desc0",
        "slicer",
        "handles",
        "oi",
        "next_k",
    )

    def __init__(
        self,
        pool: pymizu.Pool,
        pymizu,
        ranges: list,
        w: int,
        plan: _Plan,
        fn: _Any,
        x: _Any,
        probe: tuple | None,
        template: _Any,
    ) -> None:
        self.spec = True
        self.pool = pool
        self.pymizu = pymizu
        self.ranges = ranges
        self.w = w
        self.plan = plan
        self.fn = fn
        self.x = x
        self.probe = probe
        self.template = template
        self.capsules: list = [None] * len(ranges)
        # x-free when the slices ride the raw section: build the 'I'
        # descriptor once; a descriptor-riding x rebuilds it per chunk
        self.desc0 = (
            None
            if probe is None
            else _write_desc(
                True, fn, plan.args, plan.kwargs, None, probe, plan.lang
            )
        )
        self.slicer = _stream_slicer(x, probe)
        self.handles: list = [None] * len(ranges)
        self.oi: list[int] = []
        self.next_k = 0

    def refill(
        self,
        remaining: _Callable[[], float | None],
        expired: _Callable[[], bool],
    ) -> bool:
        plan = self.plan
        c = len(self.ranges)
        while self.next_k < c and len(self.oi) < self.w:
            # the deadline pre-check, as the same-language refill's
            if expired():
                return False
            lo, hi = self.ranges[self.next_k]
            n_k = hi - lo
            if self.probe is not None:
                desc = self.desc0
                x_sec = self.slicer(lo, hi)
            else:
                desc = _write_desc(
                    True,
                    self.fn,
                    plan.args,
                    plan.kwargs,
                    self.x[lo:hi],
                    None,
                    plan.lang,
                )
                x_sec = None
            name, capsule = _pymizu._map_stage(
                desc, x_sec, n_k, n_k, self.template, plan.convert, plan.lang
            )
            # the chunk's element streams offset by its base, exactly
            # the same-language slice's global-index seeding
            seed_pair = (
                None
                if plan.seed is None
                else (plan.seed[0], plan.seed[1] + lo)
            )
            scratch: list = []
            if _submit_runners(
                self.pool,
                name,
                1,
                0,
                seed_pair,
                remaining,
                expired,
                scratch,
                True,
            ):
                _pymizu._map_cancel_set(capsule)
                _pymizu._map_close(capsule)
                return False
            self.capsules[self.next_k] = capsule
            self.handles[self.next_k] = scratch[0]
            self.oi.append(self.next_k)
            self.next_k += 1
        return True

    def drive(
        self,
        remaining: _Callable[[], float | None],
        expired: _Callable[[], bool],
    ) -> list | _pymizu.Sentinel | _Any:
        pymizu = self.pymizu
        plan = self.plan
        tprobe = plan.tprobe
        n = self.ranges[-1][1]
        if tprobe is not None:
            # the final accumulator, filled a chunk's rows at its turn
            out: _Any = _wrap_out(
                bytearray(n * tprobe[2]), tprobe[0], n, tprobe[1]
            )
        else:
            out = [None] * n
        while self.oi:
            if expired():
                return pymizu.TIMEOUT
            try:
                got = self.pool._h.collect_any(
                    [self.handles[k] for k in self.oi], remaining()
                )
            except pymizu.WorkerDiedError as e:
                # the position stamp names the dead chunk; its lost set is
                # the chunk's fixed range — the blob-path precedent
                e.lost = [self.ranges[self.oi[e.index]]]
                raise
            except pymizu.TaskError as e:
                _stream_fail(
                    pymizu,
                    self.handles,
                    self.oi,
                    self.ranges,
                    e,
                    self.capsules,
                )
            if got is pymizu.TIMEOUT:
                return pymizu.TIMEOUT
            idx, v = got
            k = self.oi.pop(idx)
            self.handles[k] = None
            capsule, self.capsules[k] = self.capsules[k], None
            lo, hi = self.ranges[k]
            if tprobe is not None:
                chunk, _ = _gather_out(capsule, tprobe, hi - lo, "copy")
                out[lo:hi] = chunk
            else:
                run = v
                if plan.lang == _LANG_R:
                    run = _runs_to_spans([v], hi - lo, hi - lo)[0]
                hist, vals = run
                for (blo, bhi), batch in zip(hist, vals, strict=True):
                    out[lo + blo : lo + bhi] = batch
            _pymizu._map_close(capsule)
            if not self.refill(remaining, expired):
                return pymizu.TIMEOUT
        return out


def _morsel_geometry(
    n: int, n_chunks: int | None, live: int, free_rs: int, inj_cap: int
) -> tuple[int, int, int]:
    """(runners, morsel, n_morsels) for n elements: ~256 morsels per
    runner, clamped to the grain constants, runners clamped by the free
    result slots and the injection ring; `n_chunks` overrides the morsel
    count directly. n == 0 takes the unit geometry (a prepared handle
    stages before it knows a run's n)."""
    runners = max(1, min(max(1, live), free_rs, inj_cap))
    if n == 0:
        return runners, 1, 0
    if n_chunks is None:
        morsel = max(1, min(n // (runners * _MORSELS_PER_RUNNER), _MORSEL_CAP))
    else:
        morsel = -(-n // min(n, n_chunks))
    return runners, morsel, -(-n // morsel)


def _gather_out(
    capsule: _Any, tprobe: tuple, n: int, collect: str
) -> tuple[_Any, bool]:
    """The template-path gather: the output area assembled as one copy,
    or zero-copy as a view — which takes over the region's ownership
    (True), so the caller must drop its own reference without unlinking."""
    if collect == "view":
        view = _wrap_out(
            _pymizu._map_gather_view(capsule), tprobe[0], n, tprobe[1]
        )
        return view, True
    copy = _wrap_out(_pymizu._map_gather(capsule), tprobe[0], n, tprobe[1])
    return copy, False


def _submit_runners(
    pool: pymizu.Pool,
    name: str,
    r: int,
    gen: int,
    seed_spec: tuple | None,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
    handles: list,
    spec: bool = False,
) -> bool:
    """Submit the r runner tasks of one run; True when the deadline
    expired mid-submit (the caller's backstop cancels what landed). The
    payload carries the run's generation: a stale straggler from a prior
    run of a prepared map fails its first-call CAS against the re-armed
    CLAIM word. A spec map's runner is the kind-2 task stream — region
    name, ordinal and generation packed in one i64, the (seed, offset)
    pair — framed off a `_RunnerFrame`."""
    for k in range(r):
        # pre-check, not just the verb's: a nested (worker-side) submit
        # never waits on ring space, so an expired deadline must be caught
        # here, before the payload is built
        if expired():
            return True
        payload = (
            _pymizu._runner_frame(name, (k << _GEN_SHIFT) + gen, seed_spec)
            if spec
            else (_runner, (name, k, gen, seed_spec), {})
        )
        h = pool._h._submit_runner(payload, remaining())
        handles.append(h)
    return False


def _runs_to_spans(runs: list, ms: int, n: int) -> list:
    """Normalize R workers' runner results to this binding's shape: their
    runners publish (morsel starts, morsel counts, values) triples, this
    binding's publish (element ranges, values) pairs. The lost-set scan
    and the splice are written over the element-range shape; a batch's
    span is [m*ms, min((m+k)*ms, n)). A one-batch history crosses as
    scalars (the interchange writer's length-1 atomic rule): re-list
    them."""
    out = []
    for run in runs:
        hm, hk, vals = run
        if isinstance(hm, (int, float)):
            hm, hk = [hm], [hk]
        hist = [
            (int(m) * ms, min((int(m) + int(k)) * ms, n))
            for m, k in zip(hm, hk, strict=True)
        ]
        out.append((hist, vals))
    return out


def _map_workers_gone(pool: pymizu.Pool) -> bool:
    """The pending loop's registry-read death verdict: the workers are
    all gone from the registry and no shutdown was broadcast (a clean
    stop surfaces through the handles). "claiming" counts as live — a
    joining worker is about to claim."""
    st = pool.status()
    return not st["shutdown"] and not any(
        w in ("live", "claiming") for w in st["workers"]
    )


def _collect_region(
    pymizu,
    pool: pymizu.Pool,
    capsule: _Any,
    handles: list,
    n: int,
    remaining: _Callable[[], float | None],
    expired: _Callable[[], bool],
    tmpl: bool = False,
    gen: int = 0,
    lang: int = 3,
    morsel: int = 1,
    live0: int = 0,
) -> list | _pymizu.Sentinel | None:
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
    # the death exit's state: a verdict that never surfaces on a handle
    # (the workers all died with the pending runners unclaimed — a queued
    # runner's slot has no reaper) is read off the registry instead,
    # polled at most once a second while the loop makes no progress
    doomed = False
    last_live_scan = 0.0
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
            # with the workers live at submit all gone (and no shutdown
            # broadcast, which surfaces through the handles), nothing
            # will ever claim the pending runners: arm the trim so they
            # abandon, and let the drain raise the documented
            # WorkerDiedError below. A map submitted with no live worker
            # keeps waiting — one may join
            now = _time.monotonic()
            if live0 > 0 and now - last_live_scan >= 1:
                last_live_scan = now
                if _map_workers_gone(pool):
                    _pymizu._map_cancel_set(capsule)
                    doomed = True
            t = remaining()
            if t is None or t > 0.05:
                t = 0.05
            if consume(pending[0], t):
                pending.pop(0)
    if lang == _LANG_R:  # R workers publish morsel-pair histories: normalize
        runs = _runs_to_spans(runs, morsel, n)
    if died is None and doomed:
        # the registry-read verdict carries no claimant record: None
        # slot/pid, as a no-claim reap reports
        died = pymizu.WorkerDiedError(
            "pymizu: worker died while executing this task"
        )
        died.slot = None
        died.pid = None
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


class _Map:
    """A staged, submitted map — the handle-first return of `pool_map`
    and `PreparedMap.run`: collection happens exactly once, through the
    handle. `collect()` blocks; `collect_async()` / `await` resolves one
    asyncio Future off the runner tasks' notify announces (never on a
    streaming map — its sliding window needs the blocking turn loop);
    `cancel()` sets the morsel fail-fast word and cancels the
    outstanding tasks. A dropped uncollected handle cancels at GC, so no
    run of fn is wasted. One collection per handle, as for tasks."""

    __slots__ = (
        "pool",
        "plan",
        "kind",
        "n",
        "live",
        "gen",
        "morsel",
        "handles",
        "ranges",
        "capsule",
        "name",
        "run",
        "pm",
        "_done",
        "_promised",
        "_cancelled",
        "_transferred",
    )

    def __init__(self, pool: pymizu.Pool, plan: _Plan) -> None:
        self.pool = pool
        self.plan = plan
        self.kind = ""
        self.n = 0
        self.live = 0
        self.gen = 0
        self.morsel = 1
        self.handles: list = []
        self.ranges: list = []
        self.capsule = None
        self.name = None
        self.run = None  # the streaming window state
        self.pm = None  # the PreparedMap back-reference, or None
        self._done = False
        self._promised = False
        self._cancelled = False
        self._transferred = False  # a view collect owns the region

    @classmethod
    def _empty(cls, pool: pymizu.Pool, plan: _Plan) -> _Map:
        """The length-0 map: a handle like any other (the return type
        never varies), whose collect yields the empty result. No region,
        no tasks — nothing to cancel."""
        m = cls(pool, plan)
        m.kind = "empty"
        return m

    # The submit halves (map() time) -------------------------------------

    def _open_blob(
        self,
        pymizu,
        blob: bytes,
        live: int,
        free_rs: int,
        inj_cap: int,
        remaining: _Callable[[], float | None],
        expired: _Callable[[], bool],
    ) -> bool:
        """The region-less path: fixed chunks ride the task payloads
        inline. True when the deadline expired mid-submit (the caller's
        backstop cancels what landed)."""
        n = self.n
        c = min(
            n,
            self.plan.n_chunks
            if self.plan.n_chunks is not None
            else 8 * max(1, live),
            free_rs,
            inj_cap,
        )
        c = max(1, c)
        size, extra = divmod(n, c)
        sizes = [size + 1] * extra + [size] * (c - extra)
        lo = 0
        for k in range(c):
            # pre-check, not just the verb's: a nested (worker-side)
            # submit never waits on ring space, so an expired deadline
            # must be caught here, before the payload is built
            if expired():
                return True
            hi = lo + sizes[k]
            h = self.pool._h.submit(
                (_chunk, (blob, lo, hi, self.plan.seed), {}), remaining()
            )
            self.handles.append(h)
            self.ranges.append((lo, hi))
            lo = hi
        self.kind = "blob"
        return False

    def _open_region(
        self,
        pymizu,
        fn: _Callable[..., _Any],
        x: _Any,
        probe: tuple | None,
        template: _Any,
        free_rs: int,
        inj_cap: int,
        remaining: _Callable[[], float | None],
        expired: _Callable[[], bool],
    ) -> bool:
        """The region path: stage, submit one runner per live worker
        (clamped by the morsel count, the free result slots, and the
        injection ring). A spec fn stages the 'I' descriptor (the f spec
        nested as a task tag, the list-x bare or nil) and submits kind-2
        runner tasks. True on a mid-submit deadline expiry."""
        plan = self.plan
        n = self.n
        runners, morsel, n_morsels = _morsel_geometry(
            n, plan.n_chunks, self.live, free_rs, inj_cap
        )
        desc = _write_desc(
            plan.spec, fn, plan.args, plan.kwargs, x, probe, plan.lang
        )
        name, capsule = _pymizu._map_stage(
            desc,
            x if probe is not None else None,
            n,
            morsel,
            template,
            plan.convert,
            plan.lang,
        )
        self.kind = "region"
        self.capsule = capsule
        self.name = name
        self.morsel = morsel
        r = min(n_morsels, runners)
        return _submit_runners(
            self.pool,
            name,
            r,
            0,
            plan.seed,
            remaining,
            expired,
            self.handles,
            plan.spec,
        )

    def _open_stream(
        self,
        pymizu,
        fn: _Callable[..., _Any],
        x: _Any,
        probe: tuple | None,
        template: _Any,
        free_rs: int,
        remaining: _Callable[[], float | None],
        expired: _Callable[[], bool],
    ) -> bool:
        """The streaming path: stage the descriptor region (a spec map
        stages per-chunk regions at refill time instead) and prime the
        sliding window's first fill. True on a mid-prime deadline
        expiry."""
        plan = self.plan
        n = self.n
        live = max(1, self.live)
        c = min(n, plan.n_chunks if plan.n_chunks is not None else 32 * live)
        c = max(1, c)
        w = max(1, min(c, 2 * live, free_rs))
        ranges = _stream_geometry(n, c)
        self.kind = "stream"
        self.ranges = ranges
        if plan.spec:
            self.run = _StreamWindowSpec(
                self.pool, pymizu, ranges, w, plan, fn, x, probe, template
            )
            return not self.run.refill(remaining, expired)
        desc = _pickle.dumps((fn, plan.args, plan.kwargs), 4)
        name, capsule = _pymizu._map_stage(
            desc, None, n, 1, template, plan.convert, plan.lang
        )
        self.capsule = capsule
        self.name = name
        self.run = _StreamWindow(
            self.pool,
            pymizu,
            name,
            ranges,
            w,
            plan.seed,  # pyrefly: ignore [bad-argument-type]
            _stream_slicer(x, probe),
            plan.tprobe is not None,
        )
        return not self.run.refill(remaining, expired)

    def _tasks(self) -> list:
        """The run's task handles — a streaming window's live on it."""
        run = self.run
        return run.handles if run is not None else self.handles

    def _abort(self) -> None:
        """The submit-time backstop: cancel what landed; a one-shot map
        unlinks its region explicitly, a prepared run drops the stage
        (the next run restages into a fresh one)."""
        if self.pm is None:
            if self.capsule is not None:
                _pymizu._map_cancel_set(self.capsule)
                _pymizu._map_close(self.capsule)
                self.capsule = None
        elif self.pm._capsule is not None:
            _pymizu._map_cancel_set(self.pm._capsule)
            self.pm._capsule = None
            self.pm._name = None
        run = self.run
        if run is not None and run.spec:
            for cap in run.capsules:
                if cap is not None:
                    _pymizu._map_cancel_set(cap)
                    _pymizu._map_close(cap)
        for h in self._tasks():
            if h is not None:
                h.cancel()
        self._done = True

    # The collect halves ---------------------------------------------------

    def _collect(self, pymizu, remaining, expired) -> _Any:
        plan = self.plan
        if self.kind == "empty":
            if plan.tprobe is None:
                return []
            return _wrap_out(b"", plan.tprobe[0], 0, plan.tprobe[1])
        if self.kind == "blob":
            out = []
            for k in range(len(self.handles)):
                lo, hi = self.ranges[k]
                try:
                    v = self.handles[k].collect(timeout=remaining())
                except pymizu.WorkerDiedError as e:
                    # a death carries no element index; the lost set is
                    # the dead chunk's fixed range
                    e.lost = [(lo, hi)]
                    raise
                if v is pymizu.TIMEOUT:
                    return pymizu.TIMEOUT
                self.handles[k] = None
                out.extend(v)
            return out
        if self.kind == "stream":
            assert self.run is not None  # set by _open_stream / run()
            out = self.run.drive(remaining, expired)
            if out is pymizu.TIMEOUT or plan.spec or plan.tprobe is None:
                return out
            got, transferred = _gather_out(
                self.capsule, plan.tprobe, self.n, plan.collect
            )
            if transferred:
                self._transferred = True
                if self.pm is not None:
                    self.pm._capsule = None
                    self.pm._name = None
            return got
        # the region path: collect under the exhausted-runner trim,
        # splice by element position — or gather the output area
        out = _collect_region(
            pymizu,
            self.pool,
            self.capsule,
            self.handles,
            self.n,
            remaining,
            expired,
            plan.tprobe is not None,
            self.gen,
            plan.lang,
            self.morsel,
            self.live,
        )
        if out is pymizu.TIMEOUT or plan.tprobe is None:
            return out
        out, transferred = _gather_out(
            self.capsule, plan.tprobe, self.n, plan.collect
        )
        if transferred:
            self._transferred = True
            if self.pm is not None:
                self.pm._capsule = None
                self.pm._name = None
        return out

    def _cleanup(self, unclean: bool) -> None:
        """The collect-end backstop: cancel the outstanding tasks (a
        clean collect consumed them all — a no-op then). A one-shot map
        unlinks its region explicitly; a prepared run keeps a clean
        stage for the O(1) re-arm and drops an unclean one (a straggler
        of an unclean run faces no generation fence, so the next run
        restages into a fresh region)."""
        pm = self.pm
        if pm is None:
            if self.capsule is not None:
                _pymizu._map_cancel_set(self.capsule)
                if not self._transferred:
                    _pymizu._map_close(self.capsule)
                self.capsule = None
            run = self.run
            if run is not None and run.spec:
                for i, cap in enumerate(run.capsules):
                    if cap is not None:
                        _pymizu._map_cancel_set(cap)
                        _pymizu._map_close(cap)
                        run.capsules[i] = None
            for h in self._tasks():
                if h is not None:
                    h.cancel()
            return
        pending = [h for h in self._tasks() if h is not None]
        if pending and pm._capsule is not None:
            _pymizu._map_cancel_set(pm._capsule)
        if unclean and not self._transferred:
            pm._capsule = None
            pm._name = None
        for h in pending:
            h.cancel()

    def _resolve(self, pymizu, remaining, expired) -> _Any:
        """The collect half shared by collect() and the async _finish():
        run the kind's collect, classify clean/unclean for the stage's
        fate, clean up."""
        try:
            out = self._collect(pymizu, remaining, expired)
        except BaseException:
            self._cleanup(True)
            if self._cancelled:
                # the cancel owns the outcome: a claimed runner's collect
                # raises CancelledError, a skipped queued one's frees to
                # "already collected" — both fold to the documented one
                raise pymizu.CancelledError("pymizu: map cancelled") from None
            raise
        if out is pymizu.TIMEOUT:
            self._cleanup(True)
            return out
        if self._cancelled:
            # cancelled mid-collect: the runners' partial histories must
            # not splice into a result
            self._cleanup(True)
            raise pymizu.CancelledError("pymizu: map cancelled")
        self._cleanup(False)
        return out

    def collect(self, timeout: float | None = None) -> _Any:
        """Block for the map's results; collect exactly once.

        Parameters
        ----------
        timeout
            Seconds to wait for completion; None waits indefinitely. On
            expiry the outstanding work is cancelled and the `TIMEOUT`
            [`Sentinel`](`pymizu.Sentinel`) is returned, never raised.

        Returns
        -------
            The results — the same shapes and outcome taxonomy as
            [`PreparedMap.run()`](`pymizu.PreparedMap`) documents.
        """
        import pymizu

        if self._done:
            raise pymizu.MizuError("pymizu: map handle is already collected")
        if self._promised:
            raise pymizu.MizuError(
                "pymizu: map handle has a pending async consumer — one "
                "consumer per result"
            )
        if self._cancelled:
            raise pymizu.CancelledError("pymizu: map cancelled")
        self._done = True
        remaining, expired = _deadline(timeout)
        return self._resolve(pymizu, remaining, expired)

    def collect_async(self) -> _Any:
        """The event-driven collect: an asyncio.Future resolving with the
        same gather `collect()` returns (an element error rejecting with
        the in-flight index), driven by the runner tasks' notify
        announces — never blocks, never polls. Cancelling the Future
        detaches the waiter (the handle collects blocking again); it
        never cancels the map — `cancel()` stays the explicit verb. A
        streaming map refuses: its sliding window needs the blocking
        turn loop."""
        import pymizu

        if self._done:
            raise pymizu.MizuError("pymizu: map handle is already collected")
        if self._promised:
            raise pymizu.MizuError(
                "pymizu: map handle has a pending async consumer — one "
                "consumer per result"
            )
        if self._cancelled:
            raise pymizu.CancelledError("pymizu: map cancelled")
        if self.kind == "stream":
            raise pymizu.MizuError(
                "pymizu: a streaming map handle collects blocking only — "
                "the sliding window needs the blocking turn loop"
            )
        from pymizu import _aio

        self._promised = True
        try:
            return _aio.map_collect_async(self)
        except BaseException:
            self._promised = False
            raise

    def __await__(self) -> _Any:
        return self.collect_async().__await__()

    def _finish(self) -> _Any:
        """The async settle's gather: every enrolled task terminal, so
        the blocking machinery never parks (a deadline of None parks on
        nothing)."""
        import pymizu

        self._done = True
        remaining, expired = _deadline(None)
        return self._resolve(pymizu, remaining, expired)

    def _pre_scan(self, term: dict) -> None:
        """The morsel trim per wake, as the sync collect's passes: a
        still-queued runner is dead weight once the cursor exhausts —
        cancel it into a CANCEL publish (its wake) instead of parking on
        a busy peer's no-op claim."""
        for k, h in enumerate(self.handles):
            if h is not None and not term.get(h, True):
                if _pymizu._map_abandon(self.capsule, k, self.gen) == 2:
                    h.cancel()

    def _async_detached(self) -> None:
        """The Future.cancel() restore: the announces are withdrawn and
        the handle is blocking-collectible again (the tasks are never
        cancelled)."""
        self._promised = False

    def cancel(self) -> None:
        """Stop the run: set the morsel fail-fast word (peers stop within
        about one batch) and cancel the outstanding tasks. Advisory and
        discard-only, as [`Task.cancel()`](`pymizu.Task.cancel`): a
        queued task is skipped, an executing batch completes and its
        result drops. A pending collect — blocking or async — settles
        with [`CancelledError`](`pymizu.CancelledError`). Idempotent."""
        if self._cancelled:
            return
        self._cancelled = True
        if self.capsule is not None:
            _pymizu._map_cancel_set(self.capsule)
        run = self.run
        if run is not None and run.spec:
            for cap in run.capsules:
                if cap is not None:
                    _pymizu._map_cancel_set(cap)
        for h in self._tasks():
            if h is not None:
                h.cancel()

    def __del__(self) -> None:
        # the GC backstop mirrors the sync one for a dropped UNCOLLECTED
        # handle: the cancel word stops executing runners within ~a
        # batch, and the region's unlink rides the capsule's own
        # destructor. A collected handle is inert — a prepared map's
        # stage lives on for the next run.
        if getattr(self, "_done", True):
            return
        try:
            self.cancel()
            if self.pm is not None:
                # an uncollected run is unclean: drop the stage, so the
                # next run restages into a fresh region
                self.pm._capsule = None
                self.pm._name = None
        except Exception:
            pass


class PreparedMap:
    """A map staged once into a persistent region, run many times.

    Create with [`Pool.map_prepare()`](`pymizu.Pool.map_prepare`); run
    with `run()`, which returns the run's map handle (the collect
    happens on it, as for [`Pool.map()`](`pymizu.Pool.map`)). Re-arming
    is O(1) — a generation bump, a cursor reset, a cancel-word clear —
    and workers reuse their cached contexts, so a re-run pays neither
    the descriptor pickle nor the region create nor the worker-side
    re-attach. A view-collected run hands its region to the view; the
    next run restages into a fresh one (the mirror of mizu). Close with
    `close()` (or a with block) to unlink the region.

    Parameters
    ----------
    pool
        The [`Pool`](`pymizu.Pool`) to run on.
    fn
        The function to map, or a [`call`](`pymizu.call`) spec — as for
        [`Pool.map()`](`pymizu.Pool.map`).
    x
        The data to map over.
    args
        Constant positional arguments appended to every call.
    kwargs
        Constant keyword arguments for every call.
    n_chunks
        Overrides the morsel count (the scheduling granularity).
    seed
        Deterministic per-element seeding — as for
        [`Pool.map()`](`pymizu.Pool.map`).
    template
        An exemplar buffer declaring each result's dtype and length —
        as for [`Pool.map()`](`pymizu.Pool.map`).
    collect
        'copy' or 'view' with `template`; 'list' without.
    stream
        Stream slices of `x` instead of staging it whole — as for
        [`Pool.map()`](`pymizu.Pool.map`).

    Examples
    --------
    Stage the map once, then re-run it over replacement data at memcpy
    cost (same dtype and length swap in place):

    ```{python}
    import numpy as np
    import pymizu

    with pymizu.Pool.create(2) as pool:
        with pool.map_prepare(np.square, np.arange(4.0)) as pm:
            first = pm.run().collect(timeout=10)
            second = pm.run(np.array([4.0, -5.0, 6.0, 7.0])).collect(
                timeout=10
            )
    second
    ```
    """

    def __init__(
        self,
        pool: pymizu.Pool,
        fn: _Callable[..., _Any],
        x: _Any,
        *,
        args: _Any = (),
        kwargs: dict | None = None,
        n_chunks: int | None = None,
        seed: int
        | bytes
        | bytearray
        | tuple[int | bytes | bytearray, int]
        | None = None,
        template: _Any = None,
        collect: str | None = None,
        stream: bool = False,
    ) -> None:
        import pymizu

        if not isinstance(pool, pymizu.Pool):
            raise TypeError("pymizu: pool must be a pymizu.Pool")
        plan = _Plan(
            pool, fn, args, kwargs, n_chunks, seed, template, collect, stream
        )
        if plan.spec and plan.stream:
            raise TypeError(
                "pymizu: a prepared map with a pymizu.call() spec as 'fn' "
                "cannot stream — a spec stream stages its chunk regions at "
                "run time, so there is nothing to prepare"
            )
        self._pymizu = pymizu
        self._pool = pool
        self._plan = plan
        self._spec = plan.spec
        self._lang = plan.lang
        self._convert = plan.convert
        self._seed_spec = plan.seed
        self._tprobe = plan.tprobe
        self._collect_mode = plan.collect
        self._stream = plan.stream
        self._fn = fn
        self._args = plan.args
        self._kwargs = plan.kwargs
        self._n_chunks = plan.n_chunks
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
        """(Re)target the map at x: probe for the raw section, rebuild
        the descriptor, and recompute the geometry from the pool's
        current caps (the morsel geometry, or a streaming map's fixed
        chunk ranges — whose descriptor is x-independent: fn, args,
        kwargs)."""
        probe, x, n = _probe_x(self._spec, x, self._convert, self._stream)
        self._probe = probe
        self._x = x
        self._n = n
        live, free_rs, inj_cap, _ = self._pool._h._map_caps()
        if self._stream:
            self._desc = _pickle.dumps((self._fn, self._args, self._kwargs), 4)
            c = min(
                n,
                self._n_chunks
                if self._n_chunks is not None
                else 32 * max(1, live),
            )
            self._ranges = _stream_geometry(n, max(1, c))
            return
        self._desc = _write_desc(
            self._spec,
            self._fn,
            self._args,
            self._kwargs,
            x,
            probe,
            self._lang,
        )
        _, self._morsel, self._n_morsels = _morsel_geometry(
            n, self._n_chunks, live, free_rs, inj_cap
        )

    def _swap_x(self, x: _Any) -> None:
        """Replace the staged x: an in-place memcpy over the region's x
        section when both the staged and the replacement x are raw-buffer
        eligible with the same wire type and element count (a raw x is
        sliced from the mapping per batch, never cached worker-side, so
        the swap is invisible to the workers) — anything else drops the
        staged state and the next run restages (a descriptor-carried x IS
        cached worker-side, so a shape or kind change must re-key the
        region). A streaming map's region is x-independent, so any shape
        re-slices — only a length change under a template restages (the
        output area sizes off the staged n)."""
        if self._stream:
            n = self._n
            self._set_x(x)
            if (
                self._capsule is not None
                and self._tprobe is not None
                and self._n != n
            ):
                self._capsule = None
                self._name = None
            return
        probe = _pymizu._map_probe_x(x, self._convert)
        if probe is not None and probe[0] == _TAG_COMPLEX and _np is None:
            probe = None
        if (
            self._capsule is not None
            and self._probe is not None
            and probe is not None
            and probe[0] == self._probe[0]
            and probe[1] == self._n
        ):
            try:
                _pymizu._map_swap_x(
                    self._capsule, x, self._convert, self._lang
                )
            except Exception:
                # a conversion warning escalated after the write completed:
                # the section holds the new x but the staged state is
                # distrusted — drop it so the next run restages
                self._capsule = None
                self._name = None
                self._x = x
                raise
            self._x = x
            return
        self._set_x(x)
        # drop the staged region: the capsule destructor unlinks it, and a
        # straggler against it dies at its stale-generation claim word
        self._capsule = None
        self._name = None

    def _stage(self) -> None:
        if self._stream:
            name, capsule = _pymizu._map_stage(
                self._desc,
                None,
                self._n,
                1,
                self._template,
                self._convert,
                self._lang,
            )
        else:
            name, capsule = _pymizu._map_stage(
                self._desc,
                self._x if self._probe is not None else None,
                self._n,
                self._morsel,
                self._template,
                self._convert,
                self._lang,
            )
        self._name, self._capsule, self._gen = name, capsule, 0

    def run(
        self, x: _Any = None, timeout: float | None = None
    ) -> _Map | _pymizu.Sentinel:
        """Run the prepared map once; return the run's map handle.

        Parameters
        ----------
        x
            Replaces the staged data for this and later runs. When both
            the staged and the replacement x are raw-buffer eligible
            with the same dtype and length, the swap is an in-place
            memcpy over the region's x section — the
            iterate-over-same-shape loop (optimizer steps, simulation
            sweeps) runs at memcpy cost, skipping the region create and
            the worker-side re-attach. Any other x restages
            transparently on this run.
        timeout
            Seconds before the run's submission gives up; on expiry the
            tasks already in are cancelled and the TIMEOUT
            [`Sentinel`](`pymizu.Sentinel`) is returned (no handle). The
            collection deadline is the handle's `collect()`.

        Returns
        -------
            The run's map handle — collected exactly once, by
            `collect()` (blocking) or `collect_async()` (event-driven).
            A clean value collect keeps the stage for the O(1) re-arm;
            a view collect hands the region to the view, and an unclean
            run (timeout, an error, a worker death) drops it — the next
            run restages into a fresh one either way.
        """
        pymizu = self._pymizu
        if self._closed:
            raise pymizu.MizuError("pymizu: map handle is closed")
        if x is not None:
            self._swap_x(x)
        if self._n == 0:
            return _Map._empty(self._pool, self._plan)
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
        name = self._name
        assert name is not None  # n > 0 stages at prepare / restage above
        m = _Map(pool, self._plan)
        m.pm = self
        m.n = self._n
        m.live = live
        if self._stream:
            c = len(self._ranges)
            w = max(1, min(c, 2 * max(1, live), free_rs))
            m.kind = "stream"
            m.ranges = self._ranges
            m.capsule = self._capsule
            m.name = name
            m.run = _StreamWindow(
                pool,
                pymizu,
                name,
                self._ranges,
                w,
                # never a spec map here: the (seed_bytes, offset) form
                self._seed_spec,  # pyrefly: ignore [bad-argument-type]
                _stream_slicer(self._x, self._probe),
                self._tprobe is not None,
            )
            timed_out = not m.run.refill(remaining, expired)
        else:
            m.kind = "region"
            m.capsule = self._capsule
            m.name = name
            m.gen = self._gen
            m.morsel = self._morsel
            r = min(self._n_morsels, max(1, live), free_rs, inj_cap)
            timed_out = _submit_runners(
                pool,
                name,
                r,
                self._gen,
                self._seed_spec,
                remaining,
                expired,
                m.handles,
                self._spec,
            )
        if timed_out:
            m._abort()
            return pymizu.TIMEOUT
        return m

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
