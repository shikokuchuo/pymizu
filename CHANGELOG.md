# Changelog

All notable changes to pyrei are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial pre-release.

### Added

- `pyrei.Channel`: shared-memory SPSC channels between a Python process and
  a spawned peer (`python -m pyrei.child`), with batching, a
  serialization-free raw tier for `bytes` and 1-D contiguous numpy arrays
  (float64/int32/complex128/uint8), pickle protocol 4 fallback, sentinel
  outcomes (`FULL`, `TIMEOUT`, `CLOSED`, `PEER_GONE`), and OS-notification
  peer-death detection.
- `pyrei.Pool`: work-stealing task pools over spawned workers
  (`python -m pyrei.worker`), with `submit`/`collect`, `submit_batch`,
  `collect_any`/`collect_all`, `retire`/`spawn_workers`, submitter
  `attach`, nested submit/collect via `pyrei.current_pool()`, and a
  constructed, bounded error envelope (`TaskError` with `remote_type` /
  `remote_traceback`; `WorkerDiedError` with `slot` / `pid`).
- `Pool.map`: parallel map with a shared staging region, adaptive
  batching, fail-fast cancellation, timeout sentinel, worker-death lost
  ranges, and deterministic per-element seeding of the stdlib `random`
  module (SHA-256(seed ‖ i)).
- Optional extras: `pyrei[numpy]` (zero-copy array views, raw-tier
  staging) and `pyrei[cloudpickle]` (lambdas, closures, and local
  functions as task callables).
