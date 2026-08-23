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
- Commit messages are a single line (subject only, no body).
- Never push without explicit approval — every push must be approved by
  the user first.

## Status

Repo stand-up: packaging (pyproject + setup.py), the vendor script, a
compiling `_pyrei` skeleton (module + `abi_version()` + `__core_version__`),
the package skeleton with `child.py`/`worker.py` entry stubs, README,
LICENSE (MIT). The channel verbs, staging/reading tiers, spawn, and the
pool binding land next per the plan's Phase 2 checklist. License: MIT.
