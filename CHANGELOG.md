# Changelog

All notable changes to pymizu are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial pre-release.

### Changed

- `Pool.map(..., chunks=0)` (or a negative count) on an empty input now
  raises `ValueError` instead of returning an empty result, matching
  `Pool.map_prepare`. The one-shot and prepared map entries now share
  the one validation path.

### Added

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
