# Changelog

All notable changes to pymizu are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Pre-release; no tagged version yet. On the first tag, rename this section
to `## [0.1.0] - <date>` and start a fresh `Unreleased` section above it.

### Added

Initial release.

- `Channel`: lock-free SPSC shared-memory channels between a Python
  process and a spawned peer — batching, a serialization-free raw tier
  for buffers and 1-D contiguous numpy arrays, pickle protocol 4
  fallback, sentinel outcomes (`FULL`, `TIMEOUT`, `CLOSED`, `PEER_GONE`),
  and OS-notification peer-death detection.
- `Pool`: work-stealing task pools over spawned workers, with
  `submit_batch`, `collect_any` / `collect_all`, `retire` /
  `spawn_workers`, submitter `attach`, nested submits from inside a task
  (`current_pool()`), and bounded remote errors (`TaskError`,
  `WorkerDiedError`, `TaskLostError`) instead of pickled exceptions.
- `Pool.map()`: a parallel map over a shared staging region — adaptive
  batching, streaming and prepared maps, `template=` output areas with a
  zero-copy `collect="view"`, deterministic per-element seeding, and
  handle-first `collect()` / `collect_async()` / `cancel()`.
- `PoolExecutor`: a `concurrent.futures` adapter resolving futures
  event-driven off the pool's notify endpoint — no drain thread, and
  thread-free `asyncio.wrap_future` interop.
- asyncio integration: `Channel.recv_async()` and async iteration,
  `await task`, `Pool.collect_any_async()` / `collect_all_async()`, and
  awaiting a map handle — no polling and no extra threads; cancelling a
  future detaches without cancelling the task or map.
- Zero-copy views: received vectors and string vectors arrive as
  `_ShmView` / `_ShmStrView` over the shared pages — `memoryview()`,
  numpy (`np.frombuffer`), and Arrow (`__arrow_c_array__`) consumers
  read them in place, and a view re-sent whole crosses by reference.
- `Frame` and `Frame.from_arrow()`: the data.frame's Python home and
  the opt-in zero-copy frame path — any Arrow C Stream producer
  (pyarrow, polars, duckdb) crosses as one shared-memory region, with
  validity, factor, temporal, and uint64-verbatim fidelity.
- R interop: the neutral interchange subset carries scalars, strings,
  lists, dicts, numpy arrays, temporals, and Arrow tables between Python
  and R; `pymizu.call()` task specs drive R workers and peers (both
  spawned by `r_launcher()`), including
  cross-language maps. int64 is a native wire type on both sides.
- Process-wide conveniences: `default_pool()` / `set_default_pool()` /
  `using_pool()`, `current_rng()` inside seeded map elements, `DimArray`
  for labeled R arrays, `prune()` for orphaned shared memory, and the
  `pymizu[numpy]` / `pymizu[cloudpickle]` extras.
- A documented threading contract: each handle role is confined to one
  thread at a time, guarded non-blockingly by `ConcurrentUseError`.
