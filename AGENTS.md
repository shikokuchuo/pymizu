# pyrei — project memory

Python binding for librei: lock-free shared-memory IPC (SPSC channels and
work-stealing task pools) via the raw CPython C API. Pre-release (v0.1.0).
Sibling repos: `librei` (the C core, upstream authority) and `rei` (the R
package). The governing design document is the ipc plan in the librei repo
(Phase 2 covers pyrei).

## Requirements

- Python >= 3.10, 64-bit platform. Linux: kernel >= 5.3.
- Raw CPython C API only — no pybind11/Cython/cffi. No numpy at build time
  (buffer protocol only; numpy is imported at runtime when present, the
  `pyrei[numpy]` extra). cloudpickle is the `pyrei[cloudpickle]` extra.
- Free-threaded CPython (3.13t) is out of scope for v1 — the GIL policy
  assumes a GIL.

## Layout

- `src/_pyrei.c` — the extension module (`pyrei._pyrei`): the `_Channel` /
  `_Pool` / `_Task` handles, stage/read/check callbacks, verb wrappers,
  sentinel singletons, the exception hierarchy (incl. `TaskError` with
  remote_type/remote_traceback, `WorkerDiedError` with slot/pid), and the
  `_Caught` outcome box the collect veneer unwraps and raises.
- `src/vendor/librei/` — vendored librei core (generated; see below).
- `python/pyrei/` — the Python package. `child.py` / `worker.py` are the
  spawned-process entries: `python -m pyrei.child <token>` and
  `python -m pyrei.worker <suffix> <slot>`.
- `tests/` — pytest. Cross-language cases skip unless `Rscript` and the
  installed R `rei` package are present (the `skip_if_no_child_rei()`
  mirror).
- `benchmarks/rei-bench.py` — report-only suite (asserts nothing); records
  are appended to `benchmarks/notes.md`.

## Build and test

```sh
pip install .          # builds the extension (setuptools backend)
pip install -e .       # editable
python -m pytest tests/
ruff check python tests benchmarks   # lint (config in pyproject.toml)
pyrefly check                        # typecheck
```

`setup.py` holds the explicit `ext_modules` source list (`_pyrei.c` + the
vendored core) and a `build_ext` override forcing clang-cl on Windows —
setuptools' msvc backend resolves cl.exe itself and ignores CC.

## Vendoring

`tools/vendor-librei.sh` (pin in the script) flattens librei's `rei.h`,
`rei_ext.h`, `internal.h`, and all core TUs into `src/vendor/librei/`,
with a VENDOR shasum record and a self-grep gate for stray sora/mori
remnants. The pin tracks the R package's pin. The package's own TUs
(`_pyrei.c`, `map.c`) compile against `rei_ext.h` — the binding-author
tier, version-pinned per librei minor release, may change without
deprecation — never `internal.h` (only the vendored core TUs include
it). Vendored files are never edited by hand —
changes go upstream to librei and are pulled by re-running the script
(`LIBREI_SRC=~/r/librei tools/vendor-librei.sh` for a local checkout).

## Conventions

- Staging tier order (`stage_impl` in `src/_pyrei.c`): `None` -> NIL;
  buffer-protocol objects -> RAWVEC/RAWSPILL/SHM_VEC behind the O(1) gate
  (past max(inline budget, `REI_ZC_FLOOR`) a buffer stages as SHM_VEC — one
  REIH layout write into a spill region, `rei_stage_retain_zc` storing the
  producer-loan refcount; the channel keeps the arena copy below
  `REI_ZC_FLOOR_RAW` and under churn, the pool frames RAWSPILL as a named
  region, the mirror of R's pool framing); an exact-type `str` within the
  inline budget -> STR1 (UTF-8 payload, `REI_CE_UTF8` aux; lone surrogates
  fall through); then a `_TaskFrame`
  (the `Pool.submit` payload marker — a tuple subclass built only through
  the `_task_frame` factory) -> the structured frame codec
  (`PYREI_TAG_TASK` in the codec stream: fn by module+qualname reference —
  exact function/builtin, no `<` in the qualname, module not `__main__` —
  or its own protocol-4 pickle; args/kwargs as codec scalars, None, one
  flat container level, or buffer leaves — inline bytes, or past the zc
  floor a SHM_VEC region named by a BUFREF leaf, at most one per frame and
  the stream then inline-only, because the core's staging seam holds a
  single spill checkout; a BUFREF argument arrives as a read-only view);
  then the
  compact binary codec (`PYREI_CODEC_MAGIC` 0x50 streams: bool,
  int64-bounded int, float, str, bytes, None, and one flat
  list/tuple/dict level
  of those, capped at 64 elements — exact-type checks throughout so
  subclasses keep their pickle semantics, anything else falls back); then
  pickle protocol 4 over INLINE/ARENA/SHM_RAW. Reads dispatch on the first
  byte across the three magics; R streams get an informative "R payload"
  error. `_read_stream` exposes the stream parser for tests.
- Zero-copy views (SHM_VEC/REF reads): `_ShmView`, one exporter per view,
  holds the region's `_ShmOwner` (the shared mapping owner) and subs the
  refcount in `tp_dealloc` (a fork guard skips a child's sub); it exports a
  read-only 1-D buffer, so the user object — a numpy array via
  `np.frombuffer` or a memoryview — pins the mapping through the buffer
  protocol. REIS/REIL layouts, REF paths into list trees, and R attributes
  (names/dim/class) on a layout are informative errors, never silent drops.
  A per-handle consumer view cache (`ReiViewCache`, LRU,
  `REI_OPEN_CACHE_MAX` entries, hung off `binding.ctx` and mirrored on the
  handle object for teardown after destroy) maps region name to owner: a
  hit does the counted add (`rei_zc_ref`) without a fresh open/mmap; only
  the mapping is cached — the REIH header validation runs per read. An
  evicted owner's mapping closes when its last view is gone.
- Pickle protocol pinned to 4 (homogeneous pools can mix Python point
  versions). Task callables must be importable references under stock
  pickle; cloudpickle lifts that when installed. The channel peer program
  is always a UTF-8 source string (REI_DROP_SOURCE drop).
- A task error crosses as the worker's constructed, bounded (type,
  message, traceback) envelope — never a pickled exception instance; an
  unpicklable result recovers as the task's ERR.
- `pyrei.current_pool()` binds the worker's own handle inside a task
  (nested submit/collect).
- Join tokens: `<pid hex>_<counter hex>`, validated against
  `^[0-9a-f]+_[0-9a-f]+$`; children prepend their compiled-in `/rei_`
  prefix.
- GIL policy (per handle kind): submitter handles release the GIL around
  every park/wait and reacquire it in the `check` hook for
  `PyErr_CheckSignals()`; worker (exec-capable) handles hold the GIL
  through collect and register the core's around-park `park` hook, which
  drops the GIL for each bounded sleep.
- Sentinels map to Python singletons; `REI_ERR` maps to an exception
  hierarchy mirroring the R package's classed errors. The mapping is
  per-verb, not global.
- Map conventions (`src/map.c`, `python/pyrei/_map.py`): one fresh region
  per map (own "PYRM" magic, `REI_ABI_VERSION`-keyed, the R morsel
  layout); runners are ordinary tasks submitted with `REI_ENTRY_RUNNER`
  via `_Pool._submit_runner`; the pool-signal capsule is worker-local only
  (raw addresses — a runner calls `_Pool._signals()` on its own handle,
  never on one from the submitter); the cancel word is the fail-fast
  store, set by an erroring runner before its ERR publish and by the
  submitter on timeout/interrupt/death; the stage-side capsule destructor
  unlinks (GC backstop), with `_map_close` the explicit unlink after
  collect; a runner's error envelope carries the in-flight element index
  as a fourth tuple element (ordinary `submit` errors stay 3-tuples);
  element ranges are 0-based half-open `[lo, hi)` throughout the Python
  side (R is 1-based inclusive). `Pool.map(template=...)` stages an n x m
  output area in the region (header `out_*` fields, carved from the pad —
  the struct stays 128 bytes); runners write results in place
  (`pymap_write_value`: a matching buffer memcpys, an m == 1 Python
  scalar converts), and collect is one `_map_gather` memcpy or, with
  `collect="view"`, a read-only `_MapOutView` exporter that owns the
  region capsule (unlink defers to view teardown; the stage side drops
  its reference without `_map_close`). Prepared maps:
  `Pool.map_prepare` stages once, `Pool.map_run` re-arms in O(1)
  (`_map_reset` bumps the generation and re-stamps the CLAIM array; the
  runner payload carries the run's generation so a prior run's
  straggler fails its first-call CAS), and a view-collected run
  restages into a fresh region. The element loop is Python
  (`_run_batch`) — a C batch loop was tried and reverted
  (2026-08-24): it missed its target (the winsum scaling gap is memory
  bandwidth, not interpreter overhead) — with `_map_write` on the
  template path.
- Commit messages are a single line (subject only, no body).
- Never push without explicit approval — every push must be approved by
  the user first.

## Testing

- Task callables used in pool tests live in `tests/helpers.py`, not in the
  test modules: pickle sends them by reference, so the spawned workers
  must import them. The root `conftest.py` puts the repo root on
  `sys.path`, making `tests.helpers` importable to both the test process
  and the workers (whose `sys.path[0]` is the repo root).
- Coverage runs as `coverage run -m pytest tests/`; `conftest.py` arms the
  spawned child/worker interpreters via `COVERAGE_PROCESS_START` so they
  measure themselves too.

## CI

`.github/workflows/ci.yml`: a lint leg (ruff + pyrefly), an sdist leg (the
sdist must carry the vendored C core and build standalone), and a build
matrix over ubuntu/macos/windows x Python 3.10/3.14 that installs, runs an
import smoke, and measures coverage (uploaded from ubuntu only).

License: MIT.
