# Changelog

All notable changes to pymizu are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Faster sends of nested list, tuple, or dict payloads to R peers.

### Fixed

- Fixed a rare deadlock where `Pool.stop()` could hang indefinitely
  when a `PoolExecutor` wrapped the pool.
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
