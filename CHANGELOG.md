# Changelog

All notable changes to pymizu are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial pre-release.

### Added

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
