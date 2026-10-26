# Changelog

All notable changes to pymizu are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial pre-release.

### Added

- asyncio integration: event-driven futures over the core's new
  edge-triggered notify endpoints — no polling, and no threads beyond
  the core's own listener machinery (a burst of publishes collapses to
  one wake per idle-to-data transition).
  - `Channel.recv_async()` returns an `asyncio.Future` for the next
    payload, resolving with the identical object a blocking `recv()`
    returns (zero-copy views included) or with the `CLOSED` /
    `PEER_GONE` sentinel — terminals stay values. Outstanding calls
    resolve in FIFO order. `async for x in ch` is the stream form,
    ending on the terminal like the sync iterator.
  - `await task` / `Task.collect_async()` resolves with the task's
    result or raises the flattened remote error `collect()` raises.
  - `Pool.collect_any_async()` and `Pool.collect_all_async()` resolve
    exactly one Future per collection — never one per task — with the
    sync verbs' semantics (input order; errors carry `index`).
  - `await pool.map(fn, x)` / `map_handle.collect_async()` resolves one
    Future with the same gather the blocking collect returns.
  - There are no `timeout=` parameters: `asyncio.timeout` /
    `asyncio.wait_for` are the idiom, composing through cancellation.
    Cancelling a Future only detaches the waiter — a channel message
    passes to the next waiter, and a task or collection returns to
    blocking-collectible; it never cancels the task or the map.
    Drains are bounded per event-loop callback and re-scheduled, so a
    saturated ring cannot starve the loop.
- `Channel.info()` and `Pool.dump()` now report `notify_sends`, the
  count of notify signals the handle sent (armed consumers only).

### Changed

- **Breaking:** `Pool.map()`, `Pool.starmap()`, and `PreparedMap.run()`
  are handle-first, mirroring mizu's map: they stage and submit, then
  return the run's map handle immediately instead of the results.
  `handle.collect()` blocks for the results (the previous return value);
  `handle.collect_async()` / `await handle` resolves them event-driven;
  `handle.cancel()` stops the run (`CancelledError` settles a pending
  collect). The `timeout=` argument now bounds submission only — on its
  expiry the call itself returns the `TIMEOUT` sentinel as before — and
  the collection deadline moved to `handle.collect(timeout=...)`.
- `PoolExecutor` no longer spawns its drain thread: futures resolve
  event-driven off the pool's notify endpoint, so the 50 ms collect
  bound and its completion latency are gone, and `asyncio.wrap_future`
  over one is thread-free asyncio interop. Done callbacks now run on the
  core's listener thread (the same discipline as the stdlib executors'
  management thread).

- Faithful metadata round trips with R (the `MIZU_CAP_LABELED`
  capability): named vectors keep their names, matrices and arrays keep
  their `dimnames` (axis names and the `table` class included), and
  data.frame metadata — the full class vector (tibbles, any class
  ending in `"data.frame"`) and extra attributes — crosses opaquely.
  The new `pymizu.DimArray` (an `ndarray` subclass) homes labeled
  vectors and arrays with `names` / `dimnames` / `axis_names` /
  `r_class` attached; numpy operations drop the labels rather than
  mislabel reshaped data, so a modified array arrives in R as a plain
  array and a pristine echo stays `identical()`. Frame metadata rides
  the `Frame._r_attrs` stash (the class vector decoded, extra
  attributes as undecoded wire spans), spliced verbatim on a pristine
  echo; a frame rebuilt through pyarrow, polars, pandas, `from_arrow`,
  or pickle arrives in R as a plain `data.frame`. Sends of these shapes
  to a peer without the capability decline at send time, never strip;
  and a re-attached or changed label set on a region-backed array
  crosses by value (a by-reference send never resurrects the region's
  labels). The `DimArray` re-export is lazy: a numpy-less
  `import pymizu` stays clean.

- `pymizu.TaskLostError`: a task whose payload region vanished without a
  worker-death verdict (a transient open failure on the worker, not a
  dead worker) now collects as this distinct error instead of a generic
  `MizuError`, with `Task.state` reading `"lost"`.
- Cross-language streaming maps: `Pool.map()` with `stream=True` and a
  `pymizu.call` spec as `fn` now runs over workers of any language
  (previously a `TypeError`). Each chunk in the sliding window stages a
  small shared region of its own — the slice plus the task, one runner
  task per chunk — removed as soon as its chunk completes, so
  shared-memory use stays bounded by the window plus a small per-worker
  cache. Result order, `seed=` invariance, the element-indexed error
  taxonomy, and `template=` (gathered a chunk at a time) are exactly the
  non-streaming spec map's. `collect="view"` stays a `TypeError`
  (per-chunk results cannot combine into one view), as does preparing a
  streaming spec map (`pool.map_prepare()`).
- `Pool.collect_any()` and `Pool.collect_all()` errors now always carry
  a `position` attribute (the 0-based list position). A task error that
  already carries an `index` — a map error's element index, the
  documented `Pool.map` contract — keeps it as before, so both are now
  present.
- NumPy scalars and 0-d arrays (`np.float64(3.5)`, `np.array(3.5)`)
  round-trip with their exact type and shape on the same-language path,
  staged as a new compact-codec row (wire type, scalar/0-d form, element
  bytes) instead of the raw tier's 1-D length-1 array read. Without
  numpy, a received row reads as the plain Python scalar. Note for
  mixed-version deployments: the new tag errors loudly against pymizu
  builds from before this release on same-language channels and pools.
- `Pool.collect_all()`: a result that fails to read mid-batch now
  raises with an `index` (0-based failing slot) and a `values`
  attribute carrying the already-consumed prefix, instead of discarding
  those results. The failing slot and every handle past the prefix stay
  collectible.
- `Pool.submit_batch()`: a mid-batch failure now raises with an
  `accepted` attribute holding the handles already submitted, which stay
  valid and collectible — previously they leaked their result slots.

### Fixed

- Every `DeclinedError` now carries the documented `path` and `reason`
  attributes: the Arrow front-end and the label-stash gates raised
  attribute-less instances, where the value walk's declines set them.
- `Channel.send()` now returns `None` on a successful publish, as
  documented (it returned `True`): a `send(...) is None` check written
  against the documented contract treated every successful send as a
  failure. Terminal states still return the identity-tested `FULL` /
  `CLOSED` / `PEER_GONE` sentinels.
- `Pool.destroy()` no longer crashes the process when another thread is
  inside a verb on the same handle (the classic shape: a `PoolExecutor`
  drain thread parked in `collect_any`): the core handle was unmapped
  and freed under the in-flight call. Teardown now defers to the last
  in-flight verb's return — the count is guarded by the GIL itself —
  and later verbs raise `ValueError` on the closed handle. Indefinite
  waits (`submit()`, `collect()`, `collect_any()`, `collect_all()`,
  `ready_wait()`) slice into bounded core calls and break out on the
  destroy (the closed-handle `ValueError`, or `False` for
  `ready_wait()`), so the teardown runs promptly; a `stop()` wait and
  a worker's idle `run()` still delay it until they return.
- `Channel.destroy()` gets the same in-flight fence: a destroy racing a
  verb on another thread (e.g. a blocking `recv()`) no longer unmaps
  the core handle under the call — teardown defers to the last verb's
  return, as does `Channel.close()`'s once its rendezvous completes.
  Indefinite waits (`recv()`, `recv_batch()`, `ready_wait()`,
  `close()`) slice into bounded core calls and break out on the
  destroy (`CLOSED` / `False`), so the teardown runs promptly.
- A `NaN` timeout is rejected with `ValueError` on every verb and on
  `Pool.map()`: the wait verbs previously mapped it to an indefinite
  wait (a park that never wakes), while the map layer raced it as a
  non-blocking collect.
- `Pool.spawn_workers()` gates the caller's role before launching any
  process: an attached (submitter) handle previously spawned real
  workers and errored only at the join wait.
- A straggler of an unclean `PreparedMap.run()` (timeout, error, worker
  death) can no longer write its in-flight batch into the re-armed
  region's output area after a later run has collected: the unclean run
  drops the staged region, so the next run restages into a fresh one
  (the streaming run's existing discipline).
- A region-backed `Frame` with a string column re-sends through the
  foreign writer (a caps-less peer, nested in a container, or as a
  `pymizu.call` argument) instead of staging a stream the peer rejects
  as malformed.
- `PoolExecutor`: cancelling a queued future no longer fails every other
  outstanding future and kills the drain thread — the worker's
  cancel-skip frees the task, so the drain's next collect claimed a
  stale handle. The drain now drops the freed handles and re-snapshots.
- `PoolExecutor` serializes its core verbs on one lock: the pool handle
  is single-threaded per side, and the caller thread's `submit` and the
  drain thread's `collect_any` could otherwise race the shared spill
  free list with the GIL released.
- `PoolExecutor` throughput no longer degrades as outstanding futures
  pile up: the drain thread resolved each task against a fresh copy of
  the outstanding set, so a burst of n completions cost O(n^2) list
  churn (~13K tasks/s at 4,000 outstanding). The drain's snapshot is now
  persistent — submissions merge in bulk, resolves swap-remove in O(1),
  and bursts of at most 256 completions resolve before the pending
  submissions merge, bounding how long a submission waits to join the
  collect set — sustaining ~40K tasks/s at the same depth.
- `PoolExecutor`: when the drain thread fails every outstanding future
  on an infrastructure error, later submissions raise that error
  instead of silently queueing onto the dead drain thread.
- Conversion warnings (e.g. uint64 past 2^53, the missing-value
  sentinels) now fire for `pymizu.call` spec tasks whose stream is
  rewritten past the inline budget — previously the warning record was
  cut short and the rewrite's record never raised, so the lossy
  conversion was silent.
- A foreign-channel send whose write pass declines mid-write (a buffer
  exporter failing a later `__buffer__` call) now fails the send instead
  of staging a partially written reservation the peer reads as a corrupt
  or wrong value.
- A trace hook that closes over its pool no longer leaks the pool (and
  its workers): the `_Pool` and `_Task` handle types now participate in
  the cyclic GC.
- The worker's map-context cache is dropped at the worker's idle sweep:
  a parked worker no longer keeps completed maps' regions mapped (and a
  view `x`'s zero-copy loan held) until eight other maps evict it.
- Attr'd values (2-D arrays, temporal values, frames) nested within a
  few levels of the interchange depth cap are now declined at send time
  with the depth reason, instead of shipping a stream every reader
  rejects.
- A `bytes` argument to a `pymizu.call` spec stays `bytes` at any size —
  past the zero-copy gate it previously arrived as a uint8 array.
- A task whose error text contains a lone surrogate (e.g. an `OSError`'s
  undecodable path) now crosses with the text backslash-escaped, instead
  of a blanked message and traceback.
- A `DeclinedError` under a long non-ASCII dict key raises cleanly with
  its `path`/`reason` attributes, instead of a `UnicodeDecodeError`.
- `Frame.from_arrow()` on a producer reporting `null_count = -1` (the C
  Data Interface's "not yet computed", with a validity bitmap present)
  now counts the bitmap at adoption instead of raising a bare
  `MemoryError` at the frame's Arrow export.
- A frame with a complex column holding an NA now round-trips through
  pickle (`Frame.__reduce__` emits `None`, the rebuild accepts it);
  previously the rebuild rejected the state as malformed.
- A task frame abandoned after its by-reference buffer leaf staged (a
  later leaf's buffer export failing) no longer orphans the region: the
  abandoned checkout rolls back (via libmizu's newly exported
  `mizu_stage_rollback`), and the pickle fallback reuses it.
- Hardening: over-long strings are declined at send (string-vector
  elements and task code past 2^31 bytes, dict keys past 2^32 bytes,
  column names past 2^31 in total); a crafted frame pickle state with an
  undecodable timezone raises the malformed-state `ValueError` instead
  of a `SystemError`; a factor column stamped known-NA-free whose codes
  contain the NA sentinel now rejects as corrupt at export.
- A channel peer calling `sys.exit()` now exits 0 without a traceback —
  an orderly exit, not a remote error.
- `r_launcher()` / `r_pool_launcher()`: a library path containing the
  platform path separator now reaches the R child intact — each probed
  `.libPaths()` element crosses as its own hex-encoded argv entry. The
  argv layout changed with it, so spawning R processes needs the mizu
  release carrying the matching runner. A nonexistent `rscript=` now
  raises an error naming the interpreter instead of blaming a missing
  mizu.
- A non-C-contiguous array nested in a container now crosses instead of
  raising `DeclinedError`: the nested decline predated the stride-aware
  nested writers, which gather it F-order just as the top-level and
  layout-tier paths already did. Every R matrix arrives in Python as an
  F-order view, so an R `list(m = matrix(...))` now echoes from Python
  whole (labeled leaves — dimnames and names — included).

### Changed

- A spec map's `seed=(seed, offset)` on R workers now rejects an offset
  past 2^53 at `Pool.map()` call time with a local `TypeError`, matching
  the existing 32-bit seed gate — previously every runner failed
  remotely with R's own validation error. R derives per-element streams
  in doubles, exact only to 2^53, so no map that could succeed is
  affected.
- Spec maps to R workers now convert fixed-width non-identity numpy
  dtypes once at stage time, through the same table channel sends use:
  int8/16, uint16/32/64, float16/32, bool, and complex64 `x` buffers
  ride the map region's byte section as converted values (uint64 past
  2^53 warns and lands `NA` on the R side) instead of pickling every
  element through the descriptor, so each worker slices bare bytes
  rather than unpickling all of `x`. Same-language maps keep the
  lossless pickle semantics, and `template=` still takes the exemplar
  dtypes (`float64`/`int32`/`int64`/`complex128`/`uint8`) — declare the
  widened type the workers produce.

- A received double view's Arrow export now honors the region's validity
  section: `NA_real_` exports as an Arrow null and a genuine NaN stays a
  valid value — off the carried bitmap when the region carries one, off
  a lazy payload scan when it does not (the flat reserve's common path),
  and never for a known-NA-free region. An R -> Python -> R relay
  through `pa.array(view)` is now exact. `_MapOutView` shares the rule.
  `np.asarray` keeps the NaN payload as before.
- uint64 frame columns cross verbatim (channels and pools, Python and R
  alike). `Frame.from_arrow()` keeps the dtype exact — the eager float64
  conversion and its silent under-2^53 dtype loss are gone, and the
  past-2^53 warning moves from adoption time to the foreign send. On the
  wire the column stages as a verbatim INT64 MIZL leaf carrying a
  `u64 = TRUE` attribute. R's value landing stays double + `NA` past
  2^53, warned at the read; the Arrow export (`pa.table(frame)`, R's
  `mizu_as_arrow()`) is bit-exact. Bare numpy / Arrow uint64 arrays keep
  converting with the range warning, whose text is now "N uint64
  value(s) beyond +/-2^53 convert to NA_real_".
- An Arrow int64 column with a validity bitmap and a genuine
  `-9223372036854775808` on a valid lane now warns at send ("read as
  NA_integer64_ in R"), mirroring the int32 warning. Both R-semantics
  sentinel counts now fire only for an R peer — the columns are written
  identically to every peer, so they no longer warn on Python-to-Python
  sends; the range warning still reports to every peer.
- Nanosecond Arrow datetime fidelity. `timestamp[ns]` and `duration[ns]`
  columns now keep their counts and unit verbatim end to end:
  `Frame.from_arrow()` adopts them unchanged (previously it rescaled to
  microseconds — and erroneously declined `timestamp[s]` values past the
  microsecond range), `to_dict()` surfaces `datetime64[ns]` /
  `timedelta64[ns]`, and the Arrow export re-derives `timestamp[ns][,tz]`
  / `duration[ns]`. On the wire, a nanosecond column stages as a verbatim
  INT64 MIZL leaf carrying a `tscale = "ns"` attribute when the peer
  advertises the new `MIZU_CAP_MIZL_TSN` capability bit, and falls back
  to the double-second POSIXct/difftime form otherwise — an older pymizu
  or R peer reads microsecond-rounded values, never an error. Second and
  millisecond columns adopt verbatim too and now surface as
  `datetime64[s]` / `datetime64[ms]` instead of coerced microseconds (a
  unit-naive consumer reading raw counts off `to_dict()` sees counts at
  the column's own unit). The frame pickle form carries the unit: pickles
  written by an older pymizu restore as microseconds, and an older pymizu
  restoring a new pickle may mislabel the unit — the pickle form is a
  staging fallback, not a compatibility surface.
- Frame read/export savings on the remote-leaf path. A received frame
  whose K remote columns reference one region (the provenance re-send
  shape) now takes one mapping and one counted loan for it, not K — the
  column block owns a name-deduped holds vector and the per-column holds
  borrow, easing Linux `vm.max_map_count` pressure on wide provenance
  frames. Repeat Arrow exports of one frame no longer re-walk validity
  bitmaps: every bitmap producer records the null count once (the region
  read, the buffer adoption, the copy builds, the first string-block
  check, the scan-built bitmap) and later exports serve it. And the
  NUL-terminated column-name store an Arrow export needs is built once
  per frame and borrowed by every export, instead of copied per export.
  No wire or ABI change.
- The Arrow → MIZL frame writer moved upstream to libmizu (`arrow.c`):
  the vendored core now owns the classify/batch-vetting/dictionary
  validation, the layout write, the fixed-width conversion kernels (now
  `mizu_cvt_*`) and the attribute-blob emitters, and both frame paths
  (the foreign `__arrow_c_stream__` front-end and the same-language
  exact-`Frame` branch) stage through `mizu_arrow_open` /
  `mizu_arrow_remote` / `mizu_arrow_root_blob` / `mizu_arrow_tz` /
  `mizu_arrow_stage`. No wire change — the cross-language suite pins the
  bytes with no test edits. Two visible refinements: the capability
  conjunction and the churn check now have one home in the core stage
  (the same-language paths pass the real peer capability word — they
  previously carried none), and the send-time decline texts name the
  column up front (e.g. `column 't': an Arrow type with no portable home
  (format 'tt')`). A malformed string export now declines at staging
  rather than degrading to NA on the inline tier only.
- `Pool.map(..., chunks=0)` (or a negative count) on an empty input now
  raises `ValueError` instead of returning an empty result, matching
  `Pool.map_prepare`. The one-shot and prepared map entries now share
  the one validation path.

### Added

- Reverse layout writers, Python → R: nested trees, strided arrays, and
  `datetime64` now cross zero-copy where only frames did. A `list` /
  `tuple` / `dict` (string keys) past the zero-copy floor sent on a
  foreign (R) channel stages as one MIZL region — R wraps a list view
  instead of an interchange copy — with buffers (any memory order) and
  `datetime64` / `timedelta64` arrays as attributed MIZH leaves (`dim`,
  `POSIXct`, `Date`, `difftime`), `list[str | None]` and bare strings as
  MIZS leaves, and scalars in the interchange codec's own wire types; a
  C-order or strided array stages through the shared gather, emitting
  the F-order flat the interop writer would have copied. Every element
  outside the value-exact whitelist — a `None` outside a string list, a
  conversion dtype, a non-exact type, a lone-surrogate key — declines
  the whole tree to the interop copy, so tier choice by size never
  changes semantics, and a temporal leaf's `NaT` rides in-band as `NA`.
  A received `_ShmView` / `_ShmStrView` nested in a tree crosses as a
  remote leaf (directory tag 33): only the region's identifier moves,
  REFHELD set at the write, R resolving a view over the original region
  — gated per tree on the peer's `MIZU_CAP_MIZL_REF`, a short peer
  getting the by-value copy. No wire change (both bindings read every
  layout involved, and the remote-leaf tag, since the frame paths).
- Zero-copy frames as pool task arguments: a `pymizu.Frame` argument
  past the zero-copy floor now stages as one shared-memory region, so
  the worker's callable receives a region-backed frame instead of a
  pickled copy — the task-argument leg of the channel path, mirroring
  the result leg that already rode the region. Below the floor, with a
  complex column, or with a second region-sized argument (another frame
  or a large buffer) the whole task pickles as before — one staging
  checkout per task, the buffer-leaf rule. Raw pyarrow and polars
  frames keep the pickle path (container-exactness); constructing the
  `Frame` opts in.
- `pymizu.Frame.from_arrow()`: the opt-in zero-copy frame path for
  Python-to-Python channels. Constructs a `Frame` from any
  `__arrow_c_stream__` producer (a pyarrow `Table` or
  `RecordBatchReader`, a polars `DataFrame`, a duckdb relation),
  adopting the producer's buffers wherever a column's frame layout is
  the Arrow layout — a single-batch producer on offset-0 boundaries,
  nulls included — so construction copies nothing it does not have to.
  A column copies, never declines, where its type forces it (bool,
  narrow ints, floats of other widths, non-microsecond timestamps,
  `large_string` / `string_view`, multi-batch or sliced columns);
  unsupported Arrow types decline with `MizuError` naming the column.
  The frame then crosses a channel as one shared-memory region
  (value-exact, type-normalized) where a plain Arrow object pickles —
  the pickle path stays the default for exact container round trips.
- Streaming maps: `Pool.map(..., stream=True)` and
  `Pool.map_prepare(..., stream=True)` never stage the whole of `x` into
  shared memory — fixed x-slices ride ordinary chunk tasks under a
  sliding submit/collect window of at most
  `min(chunks, 2 * live workers, free result slots)` outstanding tasks,
  so shared-memory residency is bounded by `window x slice` instead of
  `sizeof(x)`. The chunk count defaults to `min(len(x), 32 * live
  workers)` and `chunks=` overrides it outright. Result shapes,
  ordering, `seed` invariance, and the error taxonomy are unchanged;
  fail-fast latency coarsens from ~one adaptive morsel batch to ~one
  chunk. A `pymizu.call()` spec as `fn` with `stream=True` raises
  `TypeError` (same-language workers only). A streaming prepared map
  re-slices a replacement `x` of any shape — only a length change under
  `template=` restages — and restages into a fresh region after an
  unclean run. Mirrors the R binding's `mizu_map(.stream = TRUE)`.
- `pymizu.current_rng()`: inside a `Pool.map(seed=...)` element, the
  element's memoized `numpy.random.Generator` — derived lazily on first
  call from the element's seed material (domain-separated from the
  stdlib derivation, which is unchanged) — so numpy draws gain the
  seeded-map contract: results identical for any chunking, worker
  count, or steal order. `None` outside a seeded element. `seed=`
  covers the stdlib `random` module only; the legacy `np.random.*`
  module functions draw from the worker's shared global RandomState and
  stay order-dependent — the map docs now say so, along with the
  nested-collect caveat (worker helping mid-element wipes the element's
  stash).
- Process-wide default pool: `default_pool()`, `set_default_pool(pool)`
  (returns the previous default for save/restore; `None` clears), and
  the `using_pool(pool)` context manager. Package code taking an
  optional pool resolves it in this order: an explicit argument,
  `current_pool()` inside a task, the default, then its own fallback.
  The registry anchors the handle past `del`, validates the type only
  at set time (a stopped pool is accepted and fails at use), accepts
  `Pool.attach` handles, and reads as unset in a forked child. Mirrors
  the R binding's `mizu_default_pool()` family.
- Arrow export of `Pool.map(template=..., collect="view")` results: the
  `_MapOutView` now carries `__arrow_c_array__` (and the `to_arrow`
  alias), so any Arrow consumer — pyarrow, polars, duckdb — wraps a
  map's output area zero-copy without numpy: flat n * m elements in
  element-major order, matching the buffer protocol view. The export
  holds its own mapping of the map region, opened without the zc
  refcount add (the word at that offset is the morsel header's
  `out_elt`), so it survives the map context's teardown and releases in
  pure C from any thread. int32/int64 areas scan the NA sentinel into a
  validity bitmap; complex128 raises `TypeError`. The `_ShmView`
  export's wire-tag switch and pack/scan build are now one shared
  helper both exporters call.
- Attributed-MIZH dim-array writer (F5): a Fortran-contiguous numpy
  matrix or n-D array of a wire-exact dtype past the zero-copy floor
  sent on a foreign (R) channel now stages one flat MIZH layout write
  plus the `{dim}` attribute blob — R receives a zero-copy matrix view,
  and its re-send crosses by reference — where the interop writer paid
  an attr-tag value copy at every size (11.8x on an 8 MB matrix round
  trip). The tier is buffer-protocol only (no numpy needed writer-side),
  gated on the peer's `MIZU_CAP_ATTRS`, the floor, no churn, and a
  strict F-contiguity walk over the exported strides: C-order, strided,
  and below-floor arrays keep the interop writer's value-exact
  reordering copy, since a zero-copy tier memcpys the buffer and a
  C-order flat stamped `dim` would arrive the transpose. Every decline —
  a no-cap peer, a non-exact dtype, an extent past `INT32_MAX`, a region
  failure — keeps the copy. No wire change (both bindings read
  attributed MIZH roots since 3.5).
- Top-level MIZS string writer (F4): a `list[str | None]` past the
  zero-copy floor sent on a foreign (R) channel now stages one MIZS
  layout write into a spill region — R receives a zero-copy
  character-vector view — where the interop writer paid a full value
  copy of the tree at every size (17.8x on a 1M-element list). The tier
  is exact-type only (list/str subclasses keep their interop/pickle
  semantics), gated on the peer's `MIZU_CAP_MIZS`, the whole block
  passing the floor, and no churn; `None` crosses as the
  validity-bitmap `NA_character_`, an empty string as a zero span with
  the bit set, and UTF-8 is the only encoding written. Every decline —
  a no-cap peer, a below-floor list, a non-str element, a lone
  surrogate, a region failure — keeps the 0x0b copy. No wire change
  (both bindings read MIZS since 3.1).
- Polars string-column REF verification (F3): an unmodified string
  column round-tripped through polars no longer pays the MIZL layout
  write. polars exports strings only as `string_view`, re-viewing the
  export's buffers, so the provenance match now verifies a returned
  `vu` column read-only against the export record — strings of 12 bytes
  or fewer by value (they ride inline in the 16-byte views), longer
  ones by row-byte pointer identity with the recorded leaf's span
  (polars rebases its single data buffer to the first long row), the
  null set by tail-masked bitmap equality, and each variadic data
  buffer gated to start inside the recorded bytes. A pass feeds both
  REF paths — the whole-frame REF and the per-column remote leaf
  (directory tag 33) — and every failure mode (a recompute, a reorder,
  a selection, new nulls, a fresh-buffer equal copy of long strings)
  degrades to the layout write, never wrong data. On the per-column
  path a verified column pins its export record only when it carries a
  data buffer; an all-inline match stands only beside a pinning column
  of the same frame (the write pass dereferences the record after the
  registry lock drops). No wire change.
- Task arguments by reference (F1): the `'I'` task stream gains the ref
  leaf (0x13), so a foreign-pool task's large arguments no longer pay a
  full copy each way. One fresh layout-eligible buffer argument past the
  zero-copy floor stages a single SHM_VEC layout write into the stage's
  one spill checkout (the size pass picks the first candidate whose
  remainder fits the inline budget; a checkout failure re-runs by
  value), and an argument that is already a shared view (a `_ShmView` /
  `_ShmStrView`, or a buffer whose `.base` chain ends in one over the
  view's exact bytes at its wire type) crosses as its region identifier
  alone — REFHELD set at emit, zero payload bytes. The submit side pins
  the spec for the handoff (the new `drop` hook decrefs at the
  claim-side release), and a view the task returns keeps its loan
  through the publish. A view passed as `Pool.map()`'s `x` on a foreign
  pool crosses the same way, the workers reading elements off the shared
  pages. Emission gates on the new `MIZU_CAP_TASKREF` capability bit
  (bit 3, advertised by both bindings): a spec carrying a by-reference
  candidate to a pool without the reader declines locally at submit,
  naming the remedy. A 0x13 leaf anywhere else (a channel value) is the
  informative consume-decline, never a wedge.

- Mixed-language pools (Phase 4): a Python submitter drives a pool of R
  workers, and vice versa. `pymizu.call("pkg::fn", ...)` or
  `pymizu.call(source=..., **names)` builds a task specification —
  positional arguments map to the positional list, keyword arguments to
  the named dict — and `Pool.submit(spec)` stages it as the neutral
  `'I'` task stream (tag 0x12) the worker's exec hook decodes: name
  kind resolves through the worker's module machinery, source kind runs
  the ast split (exec the prefix, eval the trailing expression) in a
  fresh namespace with the arguments bound as names (`_1`, `_2`, ... for
  positional). The workers' language comes from the pool itself (the
  core's worker identity word — creators and attached submitters alike,
  re-read while unset), so there is no language argument and no
  override: a plain callable on a foreign pool errors locally naming
  `pymizu.call()`, as do `submit_batch` and a native-fn `Pool.map()`,
  and a bare unqualified name errors at submit. Task errors cross as
  the bounded err stream (a `TaskError` with `remote_type` /
  `remote_traceback`, or the R peer's `mizu_error_remote`); a result
  without a portable home fails the task with one naming the type,
  never crosses. Worker death is unchanged (`WorkerDiedError` at
  collect). `pymizu.r_pool_launcher()` probes Rscript and an installed
  mizu at the factory and spawns `mizu:::worker_main` per slot, the
  `r_launcher()` pattern with the probed `.libPaths()` propagated in
  argv; the R package's `mizu_py_pool_launcher()` is the mirror.
- Attributed layouts Python → R (Phase 3.6): a frame past the zero-copy
  floor now stages as one MIZL region instead of an interchange copy —
  any `__arrow_c_stream__` producer (polars, pyarrow, pandas) on a
  channel whose peer passes the capability conjunction, and
  `pymizu.Frame` values on same-language handles and homogeneous pools
  (container-exact: a `Frame` reads back as a region-backed `Frame`).
  Atomic columns write bare element bytes with the Arrow validity
  bitmaps fused into the in-band sentinels and landed in the layout's
  validity sections; string columns write the MIZS block; dictionary
  columns write INT32 leaves of 1-based codes with the factor blob;
  `date32`/`timestamp` columns write Date/POSIXct leaves. A peer short
  of the conjunction gets the interchange copy, as before.
- Export-provenance REF (Phase 3.8): a region-backed `Frame`'s Arrow
  exports are recorded per acquisition, and an outgoing frame whose
  columns all match one acquisition's record — an unmodified
  R → polars/pyarrow → R round trip — stages as the region's REF, zero
  payload bytes. Modifications (new nulls, a cast, a rename, a shorter
  selection, a computed column) fail the record and take the layout
  write.
- Region-backed trees, `Frame`s and factors (Phase 3.5): an R list tree
  past the zero-copy floor now crosses as one MIZL region — a plain
  `list` of views (no attributes), a `dict` of views (names only), or a
  region-backed `pymizu.Frame` (the data.frame shape) — instead of an
  interchange copy. One counted reference per tree anchors every element
  view; a re-sent tree element of R's own resolves its `[i,j,...]` path
  to the element's wrap, and a re-sent `Frame` crosses by reference
  (REF). Attributed MIZH roots home too: a standalone factor is
  `list[str | None]`, a `{dim}` array an F-order view over the region
  (integer64 included), a Date or POSIXct a `datetime64` copy. A
  region-backed `Frame` exports `__arrow_c_stream__` off its own
  per-export mapping: fixed-width columns and validity bitmaps are
  zero-copy into Arrow consumers, factor leaves are dictionary columns
  (the region's 1-based codes shifted at export), string columns
  `large_utf8` in place, and `to_dict()` gives read-only numpy views for
  numeric columns. pymizu now advertises `MIZU_CAP_ATTRS | MIZU_CAP_MIZL`
  in its identity word, so a mizu peer stages these layouts for it.

- `_ShmView.to_numpy()` and `.to_arrow()` (Phase 3.4): a received view's
  conveniences. `to_numpy()` applies the copied-read NA rules to the view
  tiers — a logical is a `bool_` copy when NA-free, an integer a float64
  copy with `NA_real_` payloads when NAs are present, int64 always the
  int64 view (warning on an NA verdict) — with NA-freeness read off the
  region's validity section before any data scan; the `{0, 0}` fallback
  scan's verdict is cached on the view. `.to_arrow()` re-exposes the
  `__arrow_c_array__` export as a named method, on the string view too.
  pymizu now stamps its own buffer stages known-NA-free (`{0, -1}` in the
  region's validity words), so a genuine `-2^31`/`-2^63` in a Python
  array survives `to_numpy()` as a value.
- Honest numpy dtypes on the copied reads (Phase 3.2): an R logical
  vector with no `NA` now reads as numpy `bool_` on the raw tiers (one
  holding an `NA` stays int32 with the documented `-2^31` sentinel), and
  an R integer with `NA`s reads as float64 carrying R's `NA_real_`
  payload — every int32 value exact. The integer scan is gated on a
  foreign writer (the channel peer word, a pool result's `worker_ident`),
  so a Python↔Python int32 holding a genuine `-2^31` round-trips
  unchanged, dtype and value. An R int64 keeps its dtype and warns on a
  detected `INT64_MIN`, naming the Arrow export as the NA-honest
  accessor. Zero-copy views stay int32 on the raw page buffer.
- Arrow truth for logicals: every `__arrow_c_array__` export of a
  logical view is now Arrow `bool` — the bit-packed values built at
  export — with a validity bitmap, and integer/int64 views export with a
  validity bitmap when NAs are present (built off the sentinels on a
  pre-section region; the region's validity section once writers stamp
  it). R's NA sentinels read as Arrow nulls instead of visible values.
- Region-backed string views (the MIZS layout): an R character vector
  past the zero-copy floor now arrives as a `_ShmStrView` over the shared
  pages — no copy. `to_list()` materializes the explicit `list[str |
  None]` copy; `__arrow_c_array__` exports Arrow `large_utf8` with the
  validity bitmap, i64 offsets and packed bytes handed over in place
  (pyarrow/polars consumers read the region directly). A view re-sent
  whole crosses back as a reference (zero payload bytes), on
  same-language channels too. pymizu advertises `MIZU_CAP_MIZS` in its
  identity word from this release, so an R sender stages string vectors
  for it on the layout tier instead of the interchange copy.
- The err stream (the `'I'` err tag, 0x11): an uncaught exception in a
  channel peer now crosses to the host as a value before the close
  signal, on every channel — same-language included. `python -m
  pymizu.child` frames the exception (`SystemExit` excepted — an orderly
  exit) with the bounded writer: type name, message and traceback text,
  each truncated at a UTF-8 boundary to fit the slot by construction.
  The host receives a `pymizu.TaskError` carrying `remote_type` and
  `remote_traceback` (and `index` when the remote error carries one);
  `pymizu.is_remote_error()` tests a received value, and the exception
  raises naturally. The pool's ERR envelope reader accepts the err
  stream alongside the pickled frames (the Phase 4 dual format).
- The `'I'` interchange stream on foreign-language channels: every handle
  reads its peer's language and capability mask off the region at
  handshake (the libmizu identity exchange), and a foreign peer gets the
  portable interchange subset instead of the private codecs. Python
  scalars, strings, lists, dicts, numpy arrays (1-D and n-D, any order),
  `datetime64` and stdlib date/datetime temporals, Arrow arrays and
  streams, and `pymizu.Frame` values cross to R; R sends its vectors,
  lists, factors, `data.frame`s, `Date`/`POSIXct`, and matrices back.
  Same-language channels are unchanged: identity dtypes on the raw tier,
  everything else on pickle (the conversion pass and the Arrow front-ends
  are foreign-only now).
- `pymizu.Frame`: the `data.frame` home — `to_dict()` without an Arrow
  library, `__arrow_c_stream__` for polars/pyarrow/pandas consumers,
  picklable, and re-emitted as a `data.frame` on a foreign channel.
- `pymizu.DeclinedError` (a `TypeError`): raised at send time for a value
  outside the portable subset on a foreign channel, with `path` and
  `reason` attributes.


- `pymizu.Channel`: shared-memory SPSC channels between a Python process and
  a spawned peer (`python -m pymizu.child`), with batching, a
  serialization-free raw tier for `bytes` and 1-D contiguous numpy arrays,
  pickle protocol 4 fallback, sentinel outcomes (`FULL`, `TIMEOUT`,
  `CLOSED`, `PEER_GONE`), and OS-notification peer-death detection.
- Channels accept buffers of every fixed-width numeric dtype. int64 is a
  native wire type: it crosses bit-identically and lands in R as an
  `integer64` vector (bit64's layout), with `INT64_MIN` as the missing
  sentinel. bool, int8/16, uint16/32/64, float32, and complex64 convert
  to the nearest R-compatible type at send time. A uint64 value past 2^53
  in magnitude becomes `NA`, and the send issues a `RuntimeWarning`. Pool
  results keep the lossless pickle path, because pools are Python at both
  ends. On a channel, a tuple or a list around an array keeps its exact
  pickle path.
- A received zero-copy view sent on again, whole, crosses by reference:
  the slot carries the region's name and no payload bytes (REF), and the
  region is marked REFHELD first. An R → Python → R relay therefore moves
  nothing on the return hop, and a logical relayed whole returns to R as a
  logical. A slice, a reshape or a dtype view of a received view still
  crosses by value. `_ShmView.flags` exposes the region's flags word. The
  view's buffer export now answers an ND-only request with NULL strides
  (the protocol's contiguous form), so a bare `_ShmView` takes the raw and
  REF paths instead of the Arrow copy path.
- Arrow interop: `channel.send()` accepts an Arrow array from any producer
  with `__arrow_c_array__` (pyarrow, polars, duckdb). Arrow nulls become R
  missing values. A received zero-copy view exports to an Arrow consumer
  through its own `__arrow_c_array__`: `pa.array(view)` or
  `pl.from_arrow(view)`. The export release callback is pure C: a consumer
  can call it from any thread.
- Without numpy, a received zero-copy view is the view object itself:
  `memoryview(view)` and `pa.array(view)` both work on it.
- `pymizu.Pool`: work-stealing task pools over spawned workers
  (`python -m pymizu.worker`), with `submit`/`collect`, `submit_batch`,
  `collect_any`/`collect_all`, `retire`/`spawn_workers`, submitter
  `attach`, nested submit/collect via `pymizu.current_pool()`, and a
  constructed, bounded error envelope (`TaskError` with `remote_type` /
  `remote_traceback`; `WorkerDiedError` with `slot` / `pid`).
- `Pool.map`: parallel map with a shared staging region, adaptive
  batching, fail-fast cancellation, timeout sentinel, worker-death lost
  ranges, and deterministic per-element seeding of the stdlib `random`
  module (SHA-256(seed ‖ i)).
- Optional extras: `pymizu[numpy]` (zero-copy array views, raw-tier
  staging) and `pymizu[cloudpickle]` (lambdas, closures, and local
  functions as task callables).

### Fixed

- A `list[str | None]` sent to an R peer now arrives as a character
  vector at every size. Below the zero-copy floor the interchange writer
  emitted the generic list form (R received a `list`), while past it the
  MIZS tier delivered a character vector — a silent type change chosen
  by payload size. The writer and the MIZS tier now share one probe: a
  list of exact `str` / `None` elements with at least one string crosses
  as the string vector (`None` as `NA_character_`, a length-1 list as
  the scalar form), an all-`None` list stays a list of `NULL`s, and
  tuples remain lists. The rule reaches task arguments and map
  descriptors through the shared writer, so an R worker sees the same
  types as a channel peer.
- A `pymizu.Frame` sent on a foreign channel now honors its adopted
  columns' validity bitmaps: the `'I'` frame emit keyed on the data
  slots alone, so a `Frame.from_arrow()` frame with nulls in a
  fixed-width or dictionary column sent the garbage slot values (zeros)
  instead of the nulls, on every tier. Region-backed columns re-sent the
  same way also pick up their validity sections, and a received factor
  column keeps its 1-based codes instead of shifting them.
- A `uint8` column carrying Arrow nulls in a frame is now declined
  (`R raw vectors have no NA`), matching the array front-end — the
  masked convert wrote a 4-byte sentinel into the 1-byte column.
- The `Frame` Arrow export's schema and array release callbacks now
  cascade to children and dictionaries, per the C Data Interface
  contract (consumers release only the struct they received). pyarrow
  and polars both rely on the cascade; without it their imports leaked
  the export's holder past `del`.
- A channel message that fails to read (a declined R payload, a pickle
  that will not load, a non-UTF-8 string, a corrupt slot) no longer wedges
  the ring behind the unreadable slot: the read consumes the message and
  the verb raises, so the next message arrives. Consumption is limited to
  content failures — a `KeyboardInterrupt`, `SystemExit`, or `MemoryError`
  keeps the slot for a retry.
- Subclasses of buffer-exporting types now keep their pickle semantics on
  channels and pools: a `numpy.ma.MaskedArray` round-trips with its mask
  (and a `numpy.memmap` as a memmap) instead of silently arriving as the
  base buffer. Exact `bytes`, `bytearray`, `memoryview`, `array.array`,
  `numpy.ndarray`, and numpy scalar types — and unrelated exporters such
  as `pyarrow.Buffer` — still stage raw.
- `Channel.recv_batch` no longer drops the messages it already read when a
  later message in the batch fails to read: the batch ends early with the
  prefix, and the failure surfaces on the next receive. A `BaseException`
  out of the payload (an interrupt) still propagates, losing the prefix.
- `Pool.collect_all` on a first non-OK outcome now consumes only the
  reported handle: the results ahead of it stay collectible, as do the
  handles past it (previously the prefix results were consumed and
  dropped).
