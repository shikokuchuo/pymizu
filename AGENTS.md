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
`internal.h`, and all core TUs into `src/vendor/librei/`, with a VENDOR
shasum record and a self-grep gate for stray sora/mori remnants. The pin
tracks the R package's pin. Vendored files are never edited by hand —
changes go upstream to librei and are pulled by re-running the script
(`LIBREI_SRC=~/r/librei tools/vendor-librei.sh` for a local checkout).

## Conventions

- Staging tier order (`stage_impl` in `src/_pyrei.c`): `None` -> NIL;
  buffer-protocol objects -> RAWVEC/RAWSPILL behind the O(1) gate (pool
  handles frame RAWSPILL as a named region, the mirror of R's pool
  framing); an exact-type `str` within the inline budget -> STR1 (UTF-8
  payload, `REI_CE_UTF8` aux; lone surrogates fall through); then the
  compact binary codec (`PYREI_CODEC_MAGIC` 0x50 streams: bool,
  int64-bounded int, float, str, bytes, and one flat list/tuple/dict level
  of those, capped at 64 elements — exact-type checks throughout so
  subclasses keep their pickle semantics, anything else falls back); then
  pickle protocol 4 over INLINE/ARENA/SHM_RAW. Reads dispatch on the first
  byte across the three magics; R streams get an informative "R payload"
  error.
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
  side (R is 1-based inclusive).
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
