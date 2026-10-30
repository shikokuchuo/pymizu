# pymizu — project memory

Python binding for libmizu: lock-free shared-memory IPC (SPSC channels and
work-stealing task pools) via the raw CPython C API. Pre-release
(`CHANGELOG.md` is the release-facing feature summary, Keep a Changelog
format). The version is tag-derived — see Build and test.
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
  remote_type/remote_traceback, `WorkerDiedError` with slot/pid,
  `TaskLostError` for the no-death-verdict lost task), the
  `_Caught` outcome box the collect veneer unwraps and raises, and the
  `MizuNotify` notify records bridging the core's notify endpoints to
  asyncio (see Conventions). The
  create/attach/join verbs share `token_valid`, `parse_ident` (the
  test-only `_ident` override), and `handle_ctx_alloc`.
- `src/map.c` / `src/pymap.h` — the `Pool.map` region layer (see
  Conventions); `pymap.h` is its interface to `_pymizu.c` (capsule names,
  the wire-type gate, registration).
- `src/interop.c` / `src/interop_frame.c` / `src/shmframe.c` — the 'I'
  interchange codec: the value walk and builder over the core's cursor,
  the pymizu.Frame object (the type, methods, the Arrow export, the
  pickle form — `pyframe.h` carries the structs shared with the wire
  side), and the export-provenance registry (`shmframe.c`). The Arrow →
  MIZL frame writer is the vendored core's (`arrow.c`; see
  Conventions). `pyinterop.h` is their interface to `_pymizu.c`;
  `pyshmframe.h` carries the pulled-batch stream state shared by the
  frame paths.
- `src/vendor/libmizu/` — vendored libmizu core (generated; see Vendoring).
- `python/pymizu/` — the Python package. `child.py` / `worker.py` are the
  spawned-process entries (`python -m pymizu.child <token>`,
  `python -m pymizu.worker <suffix> <slot>`). `_r.py` holds the R-peer
  launcher (`pymizu.r_launcher()`; see Conventions). `_aio.py` is the
  asyncio drain half (see Conventions), `_map.py` the map orchestration,
  `_executor.py` the `concurrent.futures` adapter. Ships `py.typed` and
  the `_pymizu.pyi` stub as package data.
- `tests/` — pytest; see Testing.
- `benchmarks/` — `mizu-bench.py` (report-only, asserts nothing; records
  live in `notes.md`: best-known table on top — swap only on a new best,
  dated per row — with the dated run log appended at the bottom),
  `crosslang-map-bench.py` (the cross-language map comparison), and
  `mizu-stdlib-bench.py` (stdlib comparison).
  Task callables live in `benchmarks/tasks.py` (same by-reference pickle
  reason as `tests/helpers.py`).
- `docs/` — the documentation site, built with
  [Great Docs](https://posit-dev.github.io/great-docs/) (unreleased, from
  git main): `docs/index.qmd` is the landing page, `docs/user_guide/` the
  hand-written guides (numeric prefixes order the sidebar, `guide-section`
  frontmatter groups it), `docs/great-docs.yml` the config (site/canonical
  URLs, GitHub link, the `reference:` section list that replaces the old
  quartodoc config, `parser: numpy`, and the `authors:` block;
  `CITATION.cff` sits at the repo root). The README is the short pitch;
  the long-form details live here (mirror of how mizu keeps them in the
  reference vignette). Builds live in `docs/_quarto/` and output in
  `docs/_site/` (both gitignored); `docs/_freeze/` is committed, so CI
  does not re-execute the guide's process-spawning examples.

## Build and test

```sh
pip install .            # builds the extension (setuptools backend)
pip install -e ".[dev]"  # editable + dev tools (pytest, ruff, pyrefly, ...)
python -m pytest         # testpaths/addopts configured in pyproject.toml
ruff check python tests benchmarks          # lint (config in pyproject.toml)
ruff format --check python tests benchmarks # format gate (CI enforces)
pyrefly check            # typecheck (min-severity = "warn": warnings fail)

make help    # the self-documenting developer interface: install, test,
             # lint, check-format, type-check, check (pre-push gate),
             # build, docs, docs-preview, clean

great-docs build       # build the docs site (config docs/great-docs.yml)
great-docs preview     # local preview server
great-docs check-links # validate links in the built site
```

Docs: great-docs is installed from git main (unreleased; 0.17.0 lacks
repo-root config discovery, so the install floats `@main` rather than a
release pin — the docs workflow and local builds both use
`pip install "great-docs @ git+https://github.com/posit-dev/great-docs.git@main"`;
pin a release in the `docs` extra once one has the discovery, as a direct
reference blocks PyPI uploads). A local install goes stale against main
silently — compare the installed commit in site-packages' `direct_url.json`
with `git ls-remote` before a render. The
build puts `python/` on PYTHONPATH, so the render imports pymizu from the
source tree — the extension must be built in place (`pip install -e .`, or
`python setup.py build_ext --inplace` after `pip install setuptools`); a
stale in-place `.so` fails the render with an ImportError against the
current `__init__.py`. Rendering executes the guide examples (they spawn
real channel/pool processes), so the render environment additionally needs
numpy, cloudpickle, and jupyter; the interop page is static (it needs R +
mizu). A cell's displayed value must be a top-level expression —
capture results inside `with Pool.create(...)` blocks and echo after.

`setup.py` holds the explicit `ext_modules` source list (`_pymizu.c` + the
vendored core) and a `build_ext` override forcing clang-cl on Windows
(setuptools' msvc backend resolves cl.exe itself and ignores CC).

The version is tag-derived (`[tool.setuptools_scm]`, no hard-coded
string): building from the repo needs git metadata (all CI checkouts use
`fetch-depth: 0`), building from the sdist needs nothing (PKG-INFO).
`__version__` reads the installed metadata via `importlib.metadata`, with
a `PackageNotFoundError` fallback to `"0.0.0"`. Extras: `numpy` /
`cloudpickle` (runtime), `dev` (test/lint/typecheck toolchain), `docs`
(the render deps only — great-docs itself comes from git main, see the Docs
note), `all` (self-referencing).

The `.venv` install is non-editable: after editing `python/`, reinstall
(`pip install .`) or tests import the stale copy. After editing `src/`
(C), use `pip install . --no-cache-dir`: the version string is unchanged
by source edits, so pip's wheel cache silently serves the previous build
(the install "succeeds" with the old `.so` — the tell is a test that
fails against code you just wrote; `--no-cache-dir` is cheap insurance).
`make install` and `python setup.py build_ext --inplace` do not have the
problem.

`.pre-commit-config.yaml` runs the standard hygiene hooks plus
`ruff-check --fix` and `ruff-format`. The ruff version is pinned
identically in three places — the pre-commit rev, the `dev` extra, and
the CI lint install (currently 0.14.0); bump all three together. A
formatter version skew fails the `ruff format --check` gate, and the
tree is format-clean under the pinned version (keep it that way: run
`ruff format` before committing).

## Vendoring

`tools/vendor-libmizu.sh` (pin in the script) flattens libmizu's `mizu.h`,
`mizu_ext.h`, `internal.h`, and all core TUs except `bytes.c` (upstream's C
reference binding, which this binding replaces) and `ext.c` (the exported
FFI half of `mizu_ext.h` — TUs inline their own copies from the header) —
nothing references either's symbols — into `src/vendor/libmizu/`,
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
  2. Exact-type `str` within the inline budget -> STR1 (UTF-8 payload,
     `MIZU_CE_UTF8` aux; lone surrogates fall through).
  3. Same-language exact-type bool/int/float -> the codec's scalar rows
     written directly (`stage_scalar_fast`, ahead of the buffer probe
     and the codec's container walk; an int past int64 falls through to
     pickle). Foreign scalars route to the 'I' writer below the buffer
     tier.
  4. Buffer-protocol objects: a 0-d buffer of an identity dtype (a
     numpy scalar or 0-d array), same-language only -> the codec's
     numpy row (`PYMIZU_TAG_NUMPY` 'a': wire type, scalar/0-d form,
     element bytes), read back as the exact type — never the raw tier's
     1-D length-1 array. (New in this release: an older build's reader
     errors loudly on the tag.) Otherwise -> the core's raw-tier
     reservation (`mizu_stage_raw`, vendored `stage_raw.c`): RAWVEC
     inline, then past max(inline budget, `MIZU_ZC_FLOOR`) SHM_VEC (one
     MIZH layout write into a spill region, `mizu_stage_retain_zc`
     storing the producer-loan refcount). The channel keeps the arena
     copy below `MIZU_ZC_FLOOR_RAW` and under churn; the pool frames
     RAWSPILL as a named region. A NULL reservation falls through to
     pickle. Ahead of the reservation, a read-only buffer is checked
     for a received view re-sent whole (`stage_ref`: a `_ShmView`, or a
     numpy array whose `.base` chain ends in one over the view's exact
     bytes at its wire type — an LGL view read as int32 included): it
     crosses as REF (the region name, zero payload bytes) after
     `MIZU_ZC_FLAG_REFHELD` is OR'd into the region's flags word. A
     slice, reshape or dtype view goes by value; writable buffers never
     pay the `.base` walk.
  5. Foreign handles (an R peer), the pinned order ahead of the codec
     and pickle: the Arrow front-ends (`__arrow_c_array__`, then
     `__arrow_c_stream__`), then the layout tiers past the zc floor
     (`pymizu_stage_layout`, `src/layout.c`): a `list[str | None]` as
     one MIZS region, a buffer (any strides, identity dtypes) or a
     temporal array as one MIZH region plus the attrs blob (`dim`,
     `POSIXct`, `Date`, `difftime`), a `list` / `tuple` / `dict` tree
     as one MIZL region — buffers and temporals as attributed MIZH
     leaves, string lists and bare strings as MIZS leaves, scalars in
     the 'I' codec's wire types, and a re-sendable view element as a
     remote leaf (directory tag 33: the identifier span, REFHELD OR'd,
     the referenced column's length / attrs_size / NA claim resolved
     off the live region, gated per tree on `MIZU_CAP_MIZL_REF`, the
     node then spending its `63 + 16*n` validity-table tail). The
     value-exactness whitelist gates every tier — anything outside it
     declines to the 'I' writer's copy, whose own decline is the
     send-time DeclinedError.
  6. `_TaskFrame` (the `Pool.submit` payload marker, a tuple subclass
     built only through the `_task_frame` factory) -> the structured
     frame codec (`PYMIZU_TAG_TASK`): fn by module+qualname reference
     (exact function/builtin, no `<` in the qualname, module not
     `__main__`) or its own protocol-4 pickle; args/kwargs as codec
     scalars, None, one flat container level, or buffer leaves — inline
     bytes, or past the zc floor a SHM_VEC region named by a BUFREF leaf
     (at most one region leaf per frame, the stream then inline-only:
     the core's staging seam holds a single spill checkout). A BUFREF
     argument arrives as a read-only view. An exact `Frame` leaf (the
     same nesting rule) past the zc floor stages as a FRAMEREF leaf
     (`PYMIZU_TAG_FRAMEREF` 'F': the region's SHM_VEC aux verbatim, then
     the name) — `pymizu_frame_stage_mizl` writes one MIZL region into
     the same single checkout (the shared `fp->ref` budget), the size
     pass gating on `frame_data_size`'s column-body lower bound
     (interop_frame.c, declared in pyframe.h); a below-floor frame,
     churn, a declined write (a complex column has no Arrow export), or
     a second region-sized leaf sends the whole tuple to pickle. The
     read arm rides `read_shm_vec` (a region-backed Frame, the
     `ctx->gone` discipline); an adopted (from_arrow) Frame needs no
     special casing.
  7. The compact binary codec (`MIZU_PYMIZU_CODEC_MAGIC` 0x50 'P',
     defined in the vendored `mizu_ext.h` next to mizu's own
     `MIZU_CODEC_MAGIC` 0x52 'R' — one registry for both bindings): bool,
     int64-bounded int, float, str, bytes, None, and one flat
     list/tuple/dict level of those, capped at 64 elements. Exact-type
     checks throughout, so subclasses keep their pickle semantics;
     anything else falls back.
  8. Pickle protocol 4 over INLINE/ARENA/SHM_RAW.

  `stage_bytes` frames the pickle/codec/task-frame streams over the
  INLINE/ARENA/SHM_RAW tiers; the INLINE frame stamps the keeperless
  claim (`MIZU_AUX_F_KEEPERLESS`) — these streams commit no retain-table
  entry of their own, and a by-reference leaf's zc loan rides the
  claim-side release machinery, not the keeper-drop reap.

  The buffer gate requests `PyBUF_ND | PyBUF_FORMAT` and verifies
  C-contiguity as `strides == NULL` — never `PyBUF_C_CONTIGUOUS`, which
  implies WRITABLE and would reject a read-only view echoing back. Its
  dtype map (`wire_type_of`) is width-exact:
  uint8/float64/int32/int64/complex128 -> RAW/REAL/INT/INT64/CPLX; int64
  is a native wire tag (`MIZU_TYPE_INT64` 32, `INT64_MIN` the missing
  sentinel, no per-element scan), not a conversion. The conversion pass
  (`cvt_for_buffer` / the core's `mizu_cvt_for_arrow`) is channel-scoped
  and serves only the non-identity dtypes (uint64 past 2^53 warns to
  `NA_real_`; narrow ints widen; bool byte-expands). Reads dispatch on
  the first byte across the three magics; R streams get an informative
  "R payload" error. `_read_stream` exposes the stream parser for
  tests.
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
- The Arrow → MIZL frame writer is the vendored core's `arrow.c`
  (`mizu_arrow_open` / `_remote` / `_root_blob` / `_tz` / `_stage`, the
  `mizu_cvt_*` kernels): pymizu keeps only the stream pull (`ixs_pull`),
  the tier-attempt predicate, the provenance registry and the REF
  emits. Both frame paths — the foreign `__arrow_c_stream__` front-end
  and the same-language exact-`Frame` branch — classify/vet through
  `mizu_arrow_open`; the non-struct `single` path wraps itself as a
  one-column `+s` struct so the core serves it too. The core's
  `mizu_arrow` borrows the ArrowArray structs AND the `batches` pointer
  array — both must outlive the handle until `mizu_arrow_close`
  (`ixs_arrow_open`'s keep out-params). The capability conjunction and
  churn check live in the core stage; same-language callers pass the
  real caps word (a pool worker's `hctx->peer_caps` is 0 — `stage_impl`
  maps same-language + 0 to `MIZU_PY_CAPS`). The plans copy back out
  for the kept emitters/builder (`ixs_plans_fill` — kind / row /
  str_form only; the column bodies and dicts are the core's per-column
  writers `mizu_arrow_col_body` / `_col_blob` over the open handle, and
  the dict levels are read off batch 0's borrowed dictionary — the
  from_arrow path packs its own where a build needs them).
- The provenance registry (`src/shmframe.c`): an outgoing frame whose
  columns all match one registered export stages as the region's REF
  (Phase 3.8); per column, a matched column crosses as a remote leaf
  (MIZL directory tag 33, F2.3) — registered through
  `mizu_arrow_remote`, gated per frame on `MIZU_CAP_MIZL_REF` (bit 4; a
  peer short of it gets full layout leaves). The reader's frame path
  (`tree_column_remote` in interop.c) resolves a remote column onto the
  column block's holds, deduped by region name — one fresh mapping plus
  counted loan per distinct referenced region, released pure-C in
  `frame_cols_decref` (interop_frame.c; the GIL-free export discipline),
  the fcol's `hold` borrowing; the
  generic tree
  walk resolves through `mizu_py_view_resolve_checked`. Both
  remote-leaf decode sites (`tree_column_remote` and `tree_walk`'s REF
  branch) bound the identifier span explicitly ahead of the stack copy:
  the 255 cap in the vendored directory read is the ext tier's and may
  change without deprecation.
- Arrow interop: `Channel.send` accepts any `__arrow_c_array__` producer
  (pyarrow, polars, duckdb; the core's `mizu_cvt_for_arrow` pass). Arrow
  nulls become R missing values. A received zero-copy view exports to an Arrow consumer
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
  fails fast with `MizuError` before any channel region exists
  (`_resolve`).
  The same `r_launcher()` serves pools: called with a slot it spawns
  workers (`mizu:::worker_main(<token>, <slot>)`).
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
  unpicklable result recovers as the task's ERR. Text carrying a lone
  surrogate (an `OSError`'s undecodable path) crosses
  backslash-escaped, never blanked.
- `pymizu.current_pool()` binds the worker's own handle inside a task
  (nested submit/collect).
- Pool surface beyond submit/collect: `submit_batch`,
  `collect_any`/`collect_all`, `retire`/`spawn_workers`, submitter
  `attach(token)`, and a trace hook (`mizu_pool_set_trace`: submit-side
  events on the calling thread, removed with None, hook errors
  unraisable). `submit_batch` items are zero-arg callables (bind
  arguments with `functools.partial`), so their arguments ride the
  callable's own pickle — a frame bound there crosses by value, never
  FRAMEREF. Module-level `pymizu.prune()` runs the vendored reaper,
  reclaiming `/mizu_` regions orphaned by dead creators (a hard-killed
  process runs no finalizers).
- Join tokens: `<pid hex>_<counter hex>`, validated against
  `^[0-9a-f]+_[0-9a-f]+$`; children prepend their compiled-in `/mizu_`
  prefix.
- GIL policy (per handle kind): submitter handles release the GIL around
  every park/wait and reacquire it in the `check` hook for
  `PyErr_CheckSignals()`; worker (exec-capable) handles hold the GIL
  through collect and register the core's around-park `park` hook, which
  drops the GIL for each bounded sleep. Worker handles also register the
  core's idle `sweep` hook (`py_sweep`): it clears the Python-side map
  context cache (`pymizu._map._ctx_cache`), so a parked worker holds no
  map-region mappings or view-x zc loans.
- The `_Pool` and `_Task` handle types participate in the cyclic GC
  (`Py_TPFLAGS_HAVE_GC` + traverse; `_Pool` clears `trace_fn`), so a
  trace hook closing over its pool no longer pins it.
- Sentinels map to Python singletons; `MIZU_ERR` maps to an exception
  hierarchy mirroring the R package's classed errors. The mapping is
  per-verb, not global.
- Event-driven surface (asyncio, over the core's notify endpoints —
  `mizu_channel_notify_*` / `mizu_pool_notify_*` / `mizu_pool_task_notify`
  in the vendored `mizu_ext.h`): no polling, no threads beyond the core's
  listener machinery; a burst collapses to one wake per idle-to-data
  transition (clear-on-send). The split: `_pymizu.c` owns a `MizuNotify`
  record per armed handle (captured loop or NULL for the direct mode,
  a coalesce CAS, refs create/armed/scheduled, the live-record registry
  for the `Py_AtExit` fini); the watch callback takes the GIL and either
  schedules a trampoline via `loop.call_soon_threadsafe` (a `RuntimeError`
  there is a closed loop: disarm) or, for the executor's loop-free arm,
  calls the drain in place. `python/pymizu/_aio.py` is the drain half —
  the channel recv FIFO, the per-pool-handle enrollment (single tasks,
  and one-Future collections for `collect_any_async` /
  `collect_all_async` / maps), the bounded drain quantum (2048 items or
  ~1 ms per callback, `loop.call_soon` re-schedule) — imported lazily by
  the verbs so `import pymizu` never pays for asyncio. Verbs:
  `Channel.recv_async()` + `async for` (terminals stay sentinel VALUES),
  `await task` / `Task.collect_async()` (raises the flattened remote
  error), `Pool.collect_any_async()` / `collect_all_async()` (sync-verb
  semantics incl. `index`), `await` a map handle. Discipline:
  - Enrollment is announce (`_task_notify(tasks, 1)`) before arm, then a
    synchronous re-check; settle (resolve/reject/detach) withdraws every
    announce, restoring each slot's one-writer regime — a detached task
    is blocking-collectible again. Cancelling a Future only detaches,
    never cancels the task/map.
  - One consumer per result: an enrolled task carries the C
    `async_pending` flag and refuses blocking collects and a second
    enrollment ("one consumer per result"); a promise-armed map handle
    likewise refuses a blocking `collect()`. The executor does NOT use
    the flag (announces only): an outside collect of its task degrades
    gracefully — the outside collect claims the slot's wake, the
    executor's future fails "already collected" on the next wake.
  - Drains re-arm the edge-triggered word per pass and re-scan until a
    pass finds nothing new (the announce/recheck transplant); the pool
    scan is one `_tasks_terminal` call per pass.
  - Streaming maps refuse `collect_async()` (the sliding window needs
    the blocking turn loop).
  - Facade `destroy()`/`close()` settle pending futures through
    `call_soon_threadsafe` (asyncio futures are loop-thread-confined);
    futures on a closed loop are skipped on settle. `Pool.destroy()`
    additionally runs the facade's `_destroy_hooks` — the executor
    registers one to fail its outstanding futures (a destroy publishes
    nothing, so no wake would ever reach its drain).
  - Deadlock guard: a handle teardown landing inside the direct-mode
    drain (on the listener thread) would run the core's watch stop from
    inside the watched callback — its queue-drain sync deadlocks. The
    teardown hands off to a detached thread then (`notify_cb_active`
    TLS marker in `_pymizu.c`); the handoff's unwatch parks on the
    handler's completion as designed.
- `PoolExecutor` has no drain thread: the same watch+scan resolves
  stdlib futures in the listener callback (`Future.set_result` is
  thread-safe by design), so `asyncio.wrap_future` is thread-free and
  the 50 ms collect bound is gone. Done callbacks run on the listener
  thread (the stdlib executors' management-thread discipline).
  `_TaskFuture.cancel()` also detaches the task from the outstanding
  set at once (a cancelled queued task frees CANCEL->FREE with no
  publish, so no wake would ever cover it).
- Map conventions (`src/map.c`, `python/pymizu/_map.py`):
  - Handle-first (breaking, mirrors mizu): `Pool.map()` /
    `Pool.starmap()` / `PreparedMap.run()` stage + submit and return a
    `_Map` handle immediately — `handle.collect()` blocks (list, or the
    `_MapOutView` with `collect="view"`), `handle.collect_async()` /
    `await handle` resolves one Future (never on a stream map),
    `handle.cancel()` sets the morsel fail-fast word and cancels the
    outstanding tasks (`CancelledError` settles a pending collect), and
    a dropped uncollected handle cancels at GC. One collection per
    handle. The `timeout=` of `map()`/`run()` bounds SUBMISSION only
    (mid-submit expiry returns the `TIMEOUT` sentinel, no handle); the
    collection deadline is `collect(timeout=)`.
  - `_Map` holds the run state for its kind — `empty` (no region or
    tasks), `blob` (inline chunk tasks), `region` (morsel runners),
    `stream` (a `_StreamWindow` / `_StreamWindowSpec`, the sliding
    submit/collect windows split into `refill()` at map time and
    `drive()` at collect) — plus the run's resolved `seed` /
    `collect_mode` (named for the `_Map.collect()` method, which a
    `collect` slot would shadow) and an optional `PreparedMap`
    back-ref: a clean value collect keeps the stage for the O(1)
    re-arm, a view collect hands the region to the view, an unclean
    run drops it (the next run restages fresh).
  - The one-shot (`pool_map`) and prepared (`PreparedMap`) entries
    share the one path: `_Plan` (the fn/spec, args, template, and
    chunks gates — a bad `chunks` raises even on an empty x,
    identically from both entries), `_probe_x` (the raw-x gate),
    `_write_desc` (the descriptor), `_morsel_geometry`, and
    `_gather_out` (the template gather; its ownership-transfer flag
    tells the caller to skip the unlink). The seed shape checks live in
    `_seed_pair`; `_seed_spec` (the bytes form) and `_seed_wire` (the
    spec map's i64 pair) build on it. Seed and collect are NOT plan
    state: `_resolve_seed` / `_resolve_collect` gate them per run — at
    `map()` for the one-shot, at `run()` for a prepared map — and the
    run's `_Map` handle carries the resolved forms.
  - One fresh region per map (the core-owned `MIZU_MORSEL_MAGIC`,
    `MIZU_ABI_VERSION`-keyed).
    The protocol half — header layout, CLAIM-word claim CAS, AIMD sizing,
    reset/trim, cancel, lost-set scan — is the core's morsel module
    (`mizu_morsel_*`, vendored `morsel.c`; one layout for both bindings). Runners are ordinary tasks submitted with
    `MIZU_ENTRY_RUNNER` via `_Pool._submit_runner`.
  - Cross-language maps (Phase 5): a `pymizu.call` spec as
    `Pool.map()`'s `fn` always takes the region path (the blob path's
    chunk tasks are private frames). The descriptor is the `'I'` form
    (`_pymizu._map_desc_write` / `_map_desc_read`: `list[task, x|nil]`,
    the spec a nested kind 0/1 task tag); runners submit as kind-2
    task streams off the submit-private `_RunnerFrame`
    (`pymizu_ix_stage_runner`: region name, ordinal<<32|generation in
    one i64, seed nil or the `(seed, offset)` i64 pair). The exec hook
    dispatches kind 2 (`pymizu_ix_runner_run` → `_map._runner_ix`),
    so the runner is always same-language as the worker; `_map_ctx`
    picks the descriptor reader on the first byte, with source-kind
    specs becoming per-element closures (`_source_fn`, the ast split
    hoisted per runner, the element bound as `x`). A spec map takes
    int seeds only (`_seed_wire`: bytes has no i64 form; R workers cap
    at R's int32 range, checked at the map entry). Foreign collects
    normalize the R workers' (morsel start, morsel count) histories to
    element spans (`_runs_to_spans`) for the splice and lost-set scan.
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
  - Prepared maps: `Pool.map_prepare` stages the recipe once (fn,
    args, x, template, chunks, stream); `PreparedMap.run(x=, seed=,
    timeout=, collect=)` supplies the execution context per run —
    mirroring mizu's `mizu_map_run()` (the seed rides the run's runner
    payloads, never the staged region, so reseeding a run is free;
    `seed=None` runs unseeded whatever a previous run took). `run()`
    re-arms in O(1) (`_map_reset` bumps the generation and re-stamps
    the CLAIM array; the runner payload carries the run's generation,
    so a prior run's straggler fails its first-call CAS) and returns
    the run's `_Map` handle. A view-collected run restages into a
    fresh region.
  - The element loop is Python (`_run_batch`, `_map_write` on the
    template path) — a C batch loop was tried and reverted 2026-08-24:
    the winsum scaling gap is memory bandwidth, not interpreter
    overhead.
  - `Pool.map(seed=...)`: deterministic per-element streams of the stdlib
    `random` module — `random.seed(SHA-256(seed_bytes + (i +
    offset).to_bytes(8, "little")))`; `seed=(seed, offset)` shifts every
    element's stream by `offset`. On a spec map the (seed, offset) pair
    crosses as i64s and the worker rebuilds `str(seed).encode("ascii")`
    — identical draws to a native int-seeded run.
- Docstrings follow the Great Docs NumPy style (`parser: numpy`):
  `Parameters` / `Returns` / `Raises` sections with bare parameter names
  (no types — the signature's hints are authoritative), Great Docs
  interlinks (`` [`Pool.map()`](`pymizu.Pool.map`) ``) instead of RST
  roles, and executable ```{python} `Examples` cells on the primary entry
  points (they run at docs build; keep them under ~2 s, displayed values
  as top-level expressions). Prose paragraphs belong before the first
  section header — text after a section header parses as a section entry
  and griffe reports "Could not parse line". Docs cells (guides and
  docstring examples) run in a Jupyter kernel that already has a running
  event loop: never `asyncio.run()` there — use top-level `await`.
- Commit messages are a single line (subject only, no body).
- Never push without explicit approval — every push must be approved by
  the user first.

## Testing

- Task callables used in pool tests live in `tests/helpers.py`, not the
  test modules: pickle sends them by reference, so spawned workers must
  import them. The root `conftest.py` puts the repo root on `sys.path`,
  making `tests.helpers` importable to the test process and the workers
  (whose `sys.path[0]` is the repo root).
- A worker's outcome that no spawned worker can produce on cue runs on an
  in-process worker instead: `_pool_worker_join(token, slot)` on a
  `_pool_new` pool, `run()` + `leave()` in a daemon thread (the `leave()`
  is what lets the controller's `stop()` return cleanly), the test-only
  `_exec_fail(1|2)` arm on the worker handle forcing the decode-failure /
  vanished-payload (LOST publish) branch (`tests/test_pool.py`
  `test_task_lost`).
- pytest is configured in `pyproject.toml` (`testpaths = ["tests"]`,
  `addopts = "-ra --durations 10 --strict-markers"`; no custom markers
  exist to declare). Coverage excludes `if TYPE_CHECKING:` blocks.
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
  `tests/test_crosslang_pool.py` / `tests/test_crosslang_map.py` drive
  real R workers (`pymizu.r_launcher()` fixtures) for the Phase 4 /
  Phase 5 acceptance gates.
- `tests/test_no_numpy.py` exercises the numpy-less paths in a subprocess
  whose PYTHONPATH shim makes `import numpy` raise ImportError (the
  `_TAG_MV` memoryview rows standing in for `_TAG_NP`), in both the
  driver and its spawned workers. `tests/test_prune.py` hard-kills a
  creator to test `prune()`; `tests/test_trace.py` covers the trace hook;
  `tests/test_arrow.py` covers the Arrow exchange.
- `tests/test_aio.py` covers the asyncio surface: `asyncio.run` unit
  legs, the edge-trigger accounting (`notify_sends` on `Channel.info()`
  and `Pool.dump()["local"]` — an in-process channel pair publishes from
  the loop thread synchronously, an in-process worker publishes behind
  a busy-waited loop, so the burst deterministically beats the drain),
  quantum splitting, cancellation detachment, and the one-consumer
  refusals. Its in-process pool legs use `_pool_new` +
  `_pool_worker_join` (the worker's `run()` in a daemon thread).
- Channel tests: `close_signal()` is the PEER's verb — the host calling
  it short-circuits its OWN recv (the shared closed word checks either
  bit, so the host's recv reports CLOSED once its rx ring drains). The
  canonical echo teardown is a peer that breaks on a stop value (the
  child epilogue's close then lands CLOSED) or reads the echoes before
  signalling.

## CI

`.github/workflows/ci.yml`: a lint leg (ruff check + `ruff format
--check` + pyrefly, with numpy/cloudpickle/pyarrow/polars installed so
the optional paths typecheck), an sdist leg (the sdist must carry the
vendored C core and build standalone), and a build matrix — ubuntu
3.10/3.13/3.14, macos 3.11/3.14, windows 3.12/3.14 — that installs, runs
an import smoke, and measures coverage (codecov upload from ubuntu
only). All checkouts use `fetch-depth: 0` so setuptools_scm sees the
tags. Windows legs add
the preinstalled LLVM to PATH for clang-cl. Linux legs run the
cross-language R tests only when the `CROSSLANG_PAT` secret is set (mizu
is a private repo; without it the steps skip and the tests probe-skip).

`.github/workflows/docs.yml`: on pushes to main, installs great-docs from
git main and the package editable with the `docs` extra (the render deps),
runs `great-docs build`, and publishes `docs/_site` to
GitHub Pages via the gh-pages branch (Pages must be enabled on the repo;
the custom domain lives in the repo's Pages settings, canonical URLs come
from `site_url` + `seo.canonical.base_url` in `docs/great-docs.yml`).

License: MIT.
