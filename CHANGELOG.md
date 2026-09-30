# Changelog

All notable changes to pymizu are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial pre-release.

### Added

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
