# Changelog

All notable changes to pymizu are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Channel.recv_batch_async()` receives a burst of channel messages as one
  list on one `asyncio.Future`.

### Changed

- A `Frame` string column now always exports to Arrow as `large_string`.
  Columns of a frame built with `Frame.from_arrow()` or received below the
  shared-memory size floor exported as `string` before.
- Faster re-sends of a received `Frame` with unchanged string columns after
  a round trip through an Arrow consumer such as `pa.table(frame)`.
- Faster `Pool.map()` collection and lower shared-memory use for a
  (non-prepared) map collected by value.
- Faster `Pool.map()` with a `pymizu.call()` source spec, especially
  spec-stream maps with many chunks.
- Faster sends of nested list, tuple, and dict payloads between Python
  processes. Both ends need this version: an older pymizu fails the
  receive of such a payload with "corrupt payload slot".
- Faster first task on a new pool: it no longer waits 10 to 25 ms while
  an idle worker loads numpy. Workers now load numpy only when a task or
  map needs it.
- Faster `Pool.submit_batch()` for short batches on an idle pool: a batch
  now wakes at most one idle worker per task, not every idle worker.

### Fixed

- Fixed `Pool.map()` with a multi-element template raising `TypeError`
  during collection when numpy is unavailable.
- `Pool.map(stream=True)` with a `pymizu.call()` spec no longer retains
  completed chunk regions on Python workers.
- `cancel()` on a collected map handle no longer disrupts a later run
  of the same prepared map.

### Removed

- Support for Python 3.10, which reached end of life in October 2026.
  pymizu now requires Python 3.11 or later; pip on Python 3.10 continues
  to install 0.1.1, the last release that supports it.

## [0.1.1] - 2026-11-12

### Changed

- Faster `import pymizu`; optional features now load on first use.
- Faster sends, receives, and pool task round-trips across the board:
  wheels (and source builds) now compile with `-O3` and link-time
  optimization (thin LTO on Windows), and the extension no longer
  exports internal symbols.
- Faster pool and channel operations: task submit and collect,
  `Pool.map()`, `PoolExecutor` submission, channel receives, and
  receiving pickled or numpy-vector payloads.
- Faster `Pool.collect_any()` and `Pool.collect_all()` for collections of
  up to 16 tasks.
- Faster `Frame.from_arrow()` on chunked or sliced input with date,
  timestamp, duration, dictionary, uint64 or string columns.
- Much faster `Pool.submit()` with a received `Frame` as an argument: it
  now crosses by reference, and a task can take several such frames
  without falling back to a copy.
- Much faster frame relay: sends of a frame larger than 64 MB no
  longer create a fresh shared-memory region per send.
- Faster repeated sends of the same `Frame` — much faster when its
  columns have no nulls.
- Faster repeated Arrow exports of a received view or a view-collected
  `Pool.map()` result (e.g. `pa.array(view)`).
- Faster sends of small list, tuple, and dict payloads between Python
  processes.
- Faster `Pool.submit()`: repeated submits of the same callable,
  `pymizu.call()` spec tasks, and tasks with buffer-protocol arguments
  such as numpy arrays.
- Faster task-fn resolution on workers, notably on a module's first use
  per worker.
- Faster `Pool.map()`: per-element dispatch (particularly with no extra
  arguments), result publishing on smaller runs, and collection of
  numpy-scalar results, which no longer pass through pickle.
- Faster `PoolExecutor` submission and resolution of large bursts of
  outstanding futures.
- `PoolExecutor.map()` now honors `chunksize` and `buffersize` like
  `ProcessPoolExecutor`, including lazy input consumption and bounded pending
  tasks when a buffer size is set.
- `Task.collect_async()` and `await task` are much faster for tasks
  that finish between awaits.
- Faster sends to R peers of top-level scalars, nested list, tuple, or
  dict payloads, and int64 numpy arrays; much faster sends of Arrow
  columns with nulls and dictionary (factor) columns.
- Faster `spin=True` channel waits.
- Faster first reads of large zero-copy task arguments and views on
  Linux.
- Faster sends to R peers of Arrow string, dictionary, and temporal arrays.
- Much faster sends of large `Frame` objects to R peers.

### Fixed

- Fixed `Pool.stop()` often waiting an extra 50 ms after its workers had
  exited cleanly.
- Fixed sending a `Frame` taken from a list or dict received from an R
  peer: the peer received the whole container, not the one frame.
- Fixed `Channel.send()` to an R peer rejecting `pyarrow.RecordBatch`,
  `pyarrow.StructArray`, and nanoarrow struct arrays with `TypeError`.
- Fixed `Channel.send()` to an R peer rejecting fixed-width Arrow arrays
  longer than 2^31 - 1 elements.
- Fixed a rare `PoolExecutor` future failing with "task handle already
  collected" when a task finished as it was submitted.
- Fixed `PoolExecutor.submit()` and large executor maps raising
  `SlotsExhaustedError` when the result table filled; user-thread submissions
  now wait for the executor to free a slot.
- Fixed a corrupt stream sent to a Python peer when a list, tuple, or
  dict contained a `str` or `bytes` item larger than 4 GiB; the payload
  now falls back to pickle and arrives intact.
- Fixed a `KeyError` when a worker or collector without numpy received a
  complex map `x` or template from an R host; it now raises an
  informative error.
- Fixed `Pool.map(stream=True)` silently deriving wrong RNG streams on
  R workers when the seed offset plus `len(x)` exceeded 2^53; this now
  raises at the call site.
- Fixed a rare deadlock where `Pool.stop()` could hang indefinitely
  when a `PoolExecutor` wrapped the pool.
- Fixed a rare hang of `Pool.collect_any_async()`,
  `Pool.collect_all_async()`, or an awaited map when the pool was used
  from a new event loop after a previous one closed (for example
  back-to-back `asyncio.run()` calls).
- Fixed a rare crash when unpickling a `Frame` under memory pressure.

## [0.1.0] - 2026-11-04

### Added

First public release.

- `Channel`: lock-free SPSC shared-memory channels between Python
  processes — or between Python and an R peer — with batching, a
  serialization-free raw tier for buffers and numpy arrays, and
  sentinel outcomes (`FULL`, `TIMEOUT`, `CLOSED`, `PEER_GONE`) for a
  full ring, timeout, close, or peer death.
- `Pool`: work-stealing task pools over spawned workers —
  `submit_batch`, `collect_any` / `collect_all`, worker retirement and
  respawn, nested submits from inside a task, and classed remote
  errors instead of pickled exceptions.
- `Pool.map()`: a parallel map — streaming and prepared maps,
  `template=` output areas with a zero-copy `collect="view"`, and
  reproducible per-element random streams (`seed=`).
- Zero-copy arrays — received vectors arrive as views over the shared
  pages for memoryview, numpy, and Arrow consumers, and a view sent on
  again crosses by reference.
- Arrow and data frame interchange — any Arrow C Stream producer
  (pyarrow, polars, duckdb) crosses as one shared-memory region with
  full type fidelity, and `Frame` is the data frame's Python home.
- R interop — channels and pools mix with R processes running mizu:
  `r_launcher()` spawns peers and workers, `pymizu.call()` task specs
  drive them, labeled arrays cross via `DimArray`, and int64 is a
  native wire type on both sides.
- Event-driven asyncio — `Channel.recv_async()` and async iteration,
  `await` on tasks and maps, and `Pool.collect_any_async()` /
  `collect_all_async()` — no polling and no extra threads.
- `PoolExecutor`: a drop-in `concurrent.futures` adapter — futures
  resolve event-driven off the pool's notify endpoint, with
  thread-free `asyncio.wrap_future` interop.
- Agent skill — an agent skill for AI coding assistants ships at
  `skills/pymizu` and on the documentation site.
- Housekeeping — `prune()` reclaims shared memory orphaned by dead
  processes; `default_pool()` / `using_pool()` manage a process-wide
  default pool.
