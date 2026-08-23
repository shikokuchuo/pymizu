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

- `src/_pyrei.c` — the extension module (`pyrei._pyrei`): handle objects,
  stage/read callbacks, verb wrappers.
- `src/vendor/librei/` — vendored librei core (generated; see below).
- `python/pyrei/` — the Python package. `child.py` / `worker.py` are the
  spawned-process entries: `python -m pyrei.child <token>` and
  `python -m pyrei.worker <suffix> <slot>`.
- `tests/` — pytest. Cross-language cases skip unless `Rscript` and the
  installed R `rei` package are present (the `skip_if_no_child_rei()`
  mirror).

## Build and test

```sh
pip install .          # builds the extension (setuptools backend)
pip install -e .       # editable
python -m pytest tests/
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

- Pickle protocol pinned to 4 (homogeneous pools can mix Python point
  versions). Task callables must be importable references under stock
  pickle; cloudpickle lifts that when installed. The channel peer program
  is always a UTF-8 source string (REI_DROP_SOURCE drop).
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

## Status

The channel, the pool, and the parallel map (`Pool.map`: region + blob
paths, morsel protocol, adaptive batching, doorbell help, fail-fast
cancel, timeout sentinel, worker-death lost ranges, per-element seeding of
the worker's stdlib `random` via SHA-256(seed ‖ i)) are implemented and
tested. `_pyrei.c` holds the
`_Channel` / `_Pool` / `_Task` handles, the stage/read/check callbacks
(buffer-protocol -> RAWVEC/RAWSPILL behind the O(1) gate — pool handles
frame RAWSPILL as a named region, the mirror of R's pool framing — pickle
protocol 4 fallback over INLINE/ARENA/SHM_RAW; copy-out reads, STR1 decode,
informative "R payload" errors), the worker's exec callback (the
constructed, bounded (type, message, traceback) error envelope — never a
pickled exception instance; an unpicklable result recovers as the task's
ERR), the around-park GIL hook for worker handles, the `_Caught` outcome
box the collect veneer unwraps and raises, the sentinel singletons, and the
exception hierarchy (incl. TaskError with remote_type/remote_traceback,
WorkerDiedError with slot/pid). `pyrei.Channel` / `pyrei.Pool` are the
facades; `python -m pyrei.child` and `python -m pyrei.worker` are the
spawned-process entries; `pyrei.current_pool()` binds the worker's own
handle inside a task (nested submit/collect). The pytest suite (channel:
echo, batching, spill, sentinels, peer death, fork guard, SIGINT, numpy
tiers; pool: submit/collect, the outcome taxonomy, batching, worker death,
nested submit incl. the GIL park-hook liveness case, collect_any/all,
retire/spawn, attach; map: round-trip/order, chunking invariance, blob and
region paths, numpy x sections, the outcome taxonomy, fail-fast, timeout,
worker death, seed determinism, nested maps, SIGINT) is green and runs in
CI on the three OSes. Deferred: the MORH zero-copy view reader
(SHM_VEC/REF), the R-side pickle-marker recognition and source-drop path,
the cross-language tests, and the trace hook (`rei_pool_set_trace`) land
with the cross-language commit; the map's template/output-area path,
prepared maps (generation re-arm, in-place x swap), and numpy global-RNG
seeding are later follow-ups. License: MIT.
