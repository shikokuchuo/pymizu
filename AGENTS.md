# pymizu — project memory

Python binding for libmizu: lock-free shared-memory IPC (SPSC channels and
work-stealing task pools) via the raw CPython C API. Pre-release
(0.1.0.dev0; `CHANGELOG.md` is the release-facing feature summary, Keep a
Changelog format).
Sibling repos: `libmizu` (the C core, upstream authority) and `mizu` (the R
package). The governing design document is the ipc plan in the libmizu repo
(Phase 2 covers pymizu).

## Requirements

- Python >= 3.10, 64-bit platform. Linux: kernel >= 5.3. Windows: the
  build requires clang-cl (see Build).
- Raw CPython C API only — no pybind11/Cython/cffi. No numpy at build
  time (buffer protocol only; numpy is imported at runtime when present,
  the `pymizu[numpy]` extra). cloudpickle is the `pymizu[cloudpickle]`
  extra.
- Free-threaded CPython (3.13t) is out of scope for v1 — the GIL policy
  assumes a GIL. (A local `.venv-ft/` 3.14 freethreaded venv exists for
  exploratory checks only.)

## Layout

- `src/_pymizu.c` — the extension module (`pymizu._pymizu`): `_Channel` /
  `_Pool` / `_Task` handles, stage/read/check callbacks, verb wrappers,
  sentinel singletons, the exception hierarchy (`TaskError` with
  remote_type/remote_traceback, `WorkerDiedError` with slot/pid), the
  `_Caught` outcome box the collect veneer unwraps and raises.
- `src/map.c` / `src/pymap.h` — the `Pool.map` region layer (see
  Conventions); `pymap.h` is its interface to `_pymizu.c` (capsule names,
  the wire-type gate, registration).
- `src/vendor/libmizu/` — vendored libmizu core (generated; see Vendoring).
- `python/pymizu/` — the Python package. `child.py` / `worker.py` are the
  spawned-process entries (`python -m pymizu.child <token>`,
  `python -m pymizu.worker <suffix> <slot>`). `_r.py` holds the R-peer
  launcher (`pymizu.r_launcher()`; see Conventions). Ships `py.typed` and
  the `_pymizu.pyi` stub as package data.
- `tests/` — pytest; see Testing.
- `benchmarks/` — `mizu-bench.py` (report-only, asserts nothing; records
  appended to `notes.md`) and `mizu-stdlib-bench.py` (stdlib comparison).
  Task callables live in `benchmarks/tasks.py` (same by-reference pickle
  reason as `tests/helpers.py`).

## Build and test

```sh
pip install .          # builds the extension (setuptools backend)
pip install -e .       # editable
python -m pytest tests/
ruff check python tests benchmarks   # lint (config in pyproject.toml)
pyrefly check                        # typecheck
```

`setup.py` holds the explicit `ext_modules` source list (`_pymizu.c` + the
vendored core) and a `build_ext` override forcing clang-cl on Windows
(setuptools' msvc backend resolves cl.exe itself and ignores CC).

The `.venv` install is non-editable: after editing `python/`, reinstall
(`pip install .`) or tests import the stale copy.

`.pre-commit-config.yaml` runs the standard hygiene hooks plus
`ruff-check --fix` and `ruff-format`.

## Vendoring

`tools/vendor-libmizu.sh` (pin in the script) flattens libmizu's `mizu.h`,
`mizu_ext.h`, `internal.h`, and all core TUs into `src/vendor/libmizu/`,
with a VENDOR shasum record and a self-grep gate for stray sora/mori
remnants. The pin tracks the R package's pin. The package's own TUs
(`_pymizu.c`, `map.c`) compile against `mizu_ext.h` (the binding-author
tier: version-pinned per libmizu minor release, may change without
deprecation), never `internal.h` — only vendored core TUs include it.
Vendored files are never edited by hand: changes go upstream to libmizu
and are pulled by re-running the script (`LIBMIZU_SRC=~/r/libmizu
tools/vendor-libmizu.sh` for a local checkout).

## Conventions

- Staging tier order (`stage_impl` in `src/_pymizu.c`), first match wins:
  1. `None` -> NIL.
  2. Buffer-protocol objects -> the core's raw-tier reservation
     (`mizu_stage_raw`, vendored `stage_raw.c`): RAWVEC inline, then past
     max(inline budget, `MIZU_ZC_FLOOR`) SHM_VEC (one MIZH layout write
     into a spill region, `mizu_stage_retain_zc` storing the producer-loan
     refcount). The channel keeps the arena copy below `MIZU_ZC_FLOOR_RAW`
     and under churn; the pool frames RAWSPILL as a named region. A NULL
     reservation falls through to pickle. Ahead of the reservation, a
     read-only buffer is checked for a received view re-sent whole
     (`stage_ref`: a `_ShmView`, or a numpy array whose `.base` chain
     ends in one over the view's exact bytes at its wire type — an LGL
     view read as int32 included): it crosses as REF (the region name,
     zero payload bytes) after `MIZU_ZC_FLAG_REFHELD` is OR'd into the
     region's flags word. A slice, reshape or dtype view goes by value;
     writable buffers never pay the `.base` walk.
  3. Exact-type `str` within the inline budget -> STR1 (UTF-8 payload,
     `MIZU_CE_UTF8` aux; lone surrogates fall through).
  4. `_TaskFrame` (the `Pool.submit` payload marker, a tuple subclass
     built only through the `_task_frame` factory) -> the structured
     frame codec (`PYMIZU_TAG_TASK`): fn by module+qualname reference
     (exact function/builtin, no `<` in the qualname, module not
     `__main__`) or its own protocol-4 pickle; args/kwargs as codec
     scalars, None, one flat container level, or buffer leaves — inline
     bytes, or past the zc floor a SHM_VEC region named by a BUFREF leaf
     (at most one per frame, the stream then inline-only: the core's
     staging seam holds a single spill checkout). A BUFREF argument
     arrives as a read-only view.
  5. The compact binary codec (`MIZU_PYMIZU_CODEC_MAGIC` 0x50 'P',
     defined in the vendored `mizu_ext.h` next to mizu's own
     `MIZU_CODEC_MAGIC` 0x52 'R' — one registry for both bindings): bool,
     int64-bounded int, float, str, bytes, None, and one flat
     list/tuple/dict level of those, capped at 64 elements. Exact-type
     checks throughout, so subclasses keep their pickle semantics;
     anything else falls back.
  6. Pickle protocol 4 over INLINE/ARENA/SHM_RAW.

  `stage_bytes` frames the pickle/codec/task-frame streams over the
  INLINE/ARENA/SHM_RAW tiers; the INLINE frame stamps the keeperless
  claim (`MIZU_AUX_F_KEEPERLESS`) — these streams commit no retain-table
  entry of their own, and a BUFREF leaf's zc loan rides the claim-side
  release machinery, not the keeper-drop reap.

  The buffer gate requests `PyBUF_ND | PyBUF_FORMAT` and verifies
  C-contiguity as `strides == NULL` — never `PyBUF_C_CONTIGUOUS`, which
  implies WRITABLE and would reject a read-only view echoing back. Its
  dtype map (`wire_type_of`) is width-exact:
  uint8/float64/int32/int64/complex128 -> RAW/REAL/INT/INT64/CPLX; int64
  is a native wire tag (`MIZU_TYPE_INT64` 32, `INT64_MIN` the missing
  sentinel, no per-element scan), not a conversion. The conversion pass
  (`cvt_for_buffer`/`cvt_for_arrow`) is channel-scoped and serves only
  the non-identity dtypes (uint64 past 2^53 warns to `NA_real_`; narrow
  ints widen; bool byte-expands). Reads dispatch on the first byte
  across the three magics; R streams get an informative "R payload"
  error. `_read_stream` exposes the stream parser for tests.
- Zero-copy views (SHM_VEC/REF reads): `_ShmView`, one exporter per
  view, holds the region's `_ShmOwner` (shared mapping owner) and subs
  the refcount in `tp_dealloc` (a fork guard skips a child's sub). It
  exports a read-only 1-D buffer, so the user object (a numpy array via
  `np.frombuffer`, or a memoryview) pins the mapping through the buffer
  protocol. `.refcount` / `.flags` read the region's zc words
  (introspection; flags bit 0 is REFHELD). MIZS/MIZL layouts, REF paths into list trees, and R
  attributes (names/dim/class) are informative errors, never silent
  drops. A per-handle view cache (`MizuViewCache`, LRU,
  `MIZU_OPEN_CACHE_MAX` entries, hung off `binding.ctx`, mirrored on the
  handle for teardown after destroy) maps region name to owner: a hit
  does the counted add (`mizu_zc_ref`) without a fresh open/mmap. Only
  the mapping is cached — MIZH header validation runs per read. An
  evicted owner's mapping closes when its last view is gone.
- Arrow interop: `Channel.send` accepts any `__arrow_c_array__` producer
  (pyarrow, polars, duckdb; the `cvt_for_arrow` pass). Arrow nulls become
  R missing values. A received zero-copy view exports to an Arrow consumer
  through its own `__arrow_c_array__` (`pa.array(view)`,
  `pl.from_arrow(view)`); the export release callback is pure C, callable
  from any thread. Without numpy, a received view is the view object
  itself — `memoryview(view)` and `pa.array(view)` both work.
- Pickle protocol pinned to 4 (homogeneous pools can mix Python point
  versions). Task callables must be importable references under stock
  pickle; cloudpickle lifts that when installed. The channel peer
  program is always a UTF-8 source string (MIZU_DROP_SOURCE drop).
- R interop: `pymizu.r_launcher()` (`python/pymizu/_r.py`) mirrors the R
  package's exported `mizu_launcher()` — a `Channel.create` launcher
  spawning the peer through mizu's static `mizu-child.R` runner with the
  entry expression (`mizu:::peer_main("<token>")`) and the probed
  `.libPaths()` hex-encoded in argv; never `Rscript -e` (it writes a
  per-spawn command file). The probe (Rscript + an installed mizu with
  the source-drop path) runs at factory call, cached per Rscript, and
  fails fast with `MizuError` before any channel region exists.
  `pymizu.r_pool_launcher()` is the pool-side mirror
  (`mizu:::worker_main(<token>, <slot>)`).
- Tier-A mixed-language pools (Phase 4): `pymizu.call()` builds a task
  spec (qualified `"mod.fn"` name or `source=`, args/kwargs) and
  `Pool.submit(spec)` stages it as the `'I'` task stream (tag 0x12)
  via the submit-private `_CallFrame` (exact-type match in
  `stage_impl`, the `_TaskFrame` pattern; the override ident rides the
  frame's second item). The exec hook dispatches on the tag ahead of
  the private read (`pool_entry_bytes` resolves INLINE/RAWVEC/STR1/
  SHM_RAW first; byte-3 misroute guard; an R-magic private frame gets
  the neutral err stream), and `pymizu_ix_task_run` (interop.c)
  decodes straight off the cursor: importlib resolution + vectorcall,
  or the ast split (`pymizu._exec_source`) with args bound in a fresh
  namespace (`_1`, `_2`, ... for positional). The task stream's
  submitter identity keys the publish formats: foreign → the err
  stream (`publish_exc`'s foreign branch) / the foreign result policy
  (the handle's `peer_lang`/`peer_caps` set around the publish, a
  decline recovered as the task's err stream); same-language → the
  private envelope. The pool word (`Pool._h._worker_ident()`, cached
  re-read while 0) gates the private verbs: a callable submit, batch
  or native-fn map on a foreign pool errors locally naming the spec.
- A task error crosses as the worker's constructed, bounded (type,
  message, traceback) envelope — never a pickled exception instance; an
  unpicklable result recovers as the task's ERR.
- `pymizu.current_pool()` binds the worker's own handle inside a task
  (nested submit/collect).
- Pool surface beyond submit/collect: `submit_batch`,
  `collect_any`/`collect_all`, `retire`/`spawn_workers`, submitter
  `attach(token)`, and a trace hook (`mizu_pool_set_trace`: submit-side
  events on the calling thread, removed with None, hook errors
  unraisable). Module-level `pymizu.prune()` runs the vendored reaper,
  reclaiming `/mizu_` regions orphaned by dead creators (a hard-killed
  process runs no finalizers).
- Join tokens: `<pid hex>_<counter hex>`, validated against
  `^[0-9a-f]+_[0-9a-f]+$`; children prepend their compiled-in `/mizu_`
  prefix.
- GIL policy (per handle kind): submitter handles release the GIL around
  every park/wait and reacquire it in the `check` hook for
  `PyErr_CheckSignals()`; worker (exec-capable) handles hold the GIL
  through collect and register the core's around-park `park` hook, which
  drops the GIL for each bounded sleep.
- Sentinels map to Python singletons; `MIZU_ERR` maps to an exception
  hierarchy mirroring the R package's classed errors. The mapping is
  per-verb, not global.
- Map conventions (`src/map.c`, `python/pymizu/_map.py`):
  - One fresh region per map (own `MIZU_PYMAP_MAGIC` "PYMM",
    `MIZU_ABI_VERSION`-keyed).
    The protocol half — header layout, CLAIM-word claim CAS, AIMD sizing,
    reset/trim, cancel, lost-set scan — is the core's morsel module
    (`mizu_morsel_*`, vendored `morsel.c`; one layout for both bindings). Runners are ordinary tasks submitted with
    `MIZU_ENTRY_RUNNER` via `_Pool._submit_runner`.
  - The pool-signal capsule is worker-local only (raw addresses — a
    runner calls `_Pool._signals()` on its own handle, never one from
    the submitter). The cancel word is the fail-fast store, set by an
    erroring runner before its ERR publish and by the submitter on
    timeout/interrupt/death.
  - The stage-side capsule destructor unlinks (GC backstop);
    `_map_close` is the explicit unlink after collect.
  - A runner's error envelope carries the in-flight element index as a
    fourth tuple element (ordinary `submit` errors stay 3-tuples).
    Element ranges are 0-based half-open `[lo, hi)` on the Python side
    (R is 1-based inclusive).
  - `Pool.map(template=...)` stages an n x m output area in the region
    (header `out_*` fields carved from the pad — the struct stays 128
    bytes). Runners write results in place (`pymap_write_value`: a
    matching buffer memcpys, an m == 1 Python scalar converts). Collect
    is one `_map_gather` memcpy or, with `collect="view"`, a read-only
    `_MapOutView` exporter that owns the region capsule (unlink defers
    to view teardown; the stage side drops its reference without
    `_map_close`).
  - Prepared maps: `Pool.map_prepare` stages once; `Pool.map_run`
    re-arms in O(1) (`_map_reset` bumps the generation and re-stamps the
    CLAIM array; the runner payload carries the run's generation, so a
    prior run's straggler fails its first-call CAS). A view-collected
    run restages into a fresh region.
  - The element loop is Python (`_run_batch`, `_map_write` on the
    template path) — a C batch loop was tried and reverted 2026-08-24:
    the winsum scaling gap is memory bandwidth, not interpreter
    overhead.
  - `Pool.map(seed=...)`: deterministic per-element streams of the stdlib
    `random` module — `random.seed(SHA-256(seed_bytes + (i +
    offset).to_bytes(8, "little")))`; `seed=(seed, offset)` shifts every
    element's stream by `offset`.
- Commit messages are a single line (subject only, no body).
- Never push without explicit approval — every push must be approved by
  the user first.

## Testing

- Task callables used in pool tests live in `tests/helpers.py`, not the
  test modules: pickle sends them by reference, so spawned workers must
  import them. The root `conftest.py` puts the repo root on `sys.path`,
  making `tests.helpers` importable to the test process and the workers
  (whose `sys.path[0]` is the repo root).
- Coverage: `coverage run -m pytest tests/`; `conftest.py` arms the
  spawned child/worker interpreters via `COVERAGE_PROCESS_START` so they
  measure themselves too.
- Cross-language cases (`tests/test_crosslang.py` +
  `tests/r_host_roundtrip.R`, real user programs as MIZU_DROP_SOURCE
  drops both directions) skip unless `Rscript` and an installed R `mizu`
  with the source-drop path are present. The `r_mizu` fixture dogfoods
  the shipped `pymizu.r_launcher()` — it catches the `MizuError` as the
  skip, so the probe logic has exactly one home (`_r.py`);
  `test_r_launcher_missing_rscript` runs without R.
- `tests/test_no_numpy.py` exercises the numpy-less paths in a subprocess
  whose PYTHONPATH shim makes `import numpy` raise ImportError (the
  `_TAG_MV` memoryview rows standing in for `_TAG_NP`), in both the
  driver and its spawned workers. `tests/test_prune.py` hard-kills a
  creator to test `prune()`; `tests/test_trace.py` covers the trace hook;
  `tests/test_arrow.py` covers the Arrow exchange.

## CI

`.github/workflows/ci.yml`: a lint leg (ruff + pyrefly, with
numpy/cloudpickle/pyarrow/polars installed so the optional paths
typecheck), an sdist leg (the sdist must carry the vendored C core and
build standalone), and a build matrix — ubuntu 3.10/3.13/3.14, macos
3.11/3.14, windows 3.12/3.14 — that installs, runs an import smoke, and
measures coverage (codecov upload from ubuntu only). Windows legs add
the preinstalled LLVM to PATH for clang-cl. Linux legs run the
cross-language R tests only when the `CROSSLANG_PAT` secret is set (mizu
is a private repo; without it the steps skip and the tests probe-skip).

License: MIT.
