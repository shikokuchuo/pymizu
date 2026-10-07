# pymizu 水

[![ci](https://github.com/shikokuchuo/pymizu/actions/workflows/ci.yml/badge.svg)](https://github.com/shikokuchuo/pymizu/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/shikokuchuo/pymizu/graph/badge.svg)](https://app.codecov.io/gh/shikokuchuo/pymizu)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Python 3.10+](https://img.shields.io/badge/python-%3E%3D3.10-blue)](https://www.python.org/)

      ________
     /\       \
    /  \ pymizu\
    \  /  水   /
     \/_______/

pymizu makes communication between Python processes cheap enough to divide work at granularities usually reserved for threads.

Shared memory has always been the fastest IPC transport.
A socket round trip costs four system calls and four copies of the data; in shared memory, one process reads the bytes the other wrote — the kernel never touches the data.
pymizu handles the synchronization, waiting, and peer crashes for you, so a task round trip drops from around 100 µs over sockets to under a microsecond — two orders of magnitude (see [Benchmarks](#benchmarks)).

In the default CPython build, the GIL runs CPU-bound threads on one core at a time, so compute parallelism in Python usually means multiple processes.
Processes also isolate failures: a worker that crashes does not take the host down with it.

Channels and work-stealing task pools run over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).
A channel is a two-way message link between a Python process and a helper process that it spawns.
A pool is a set of worker processes that divide submitted tasks among themselves.

The hot path stays in user space: single-producer single-consumer rings with batched publication, spin-then-park waiting, and event-driven peer-death detection.

pymizu is built on [libmizu](https://github.com/shikokuchuo/libmizu), a C library for lock-free shared-memory IPC.
[mizu](https://github.com/shikokuchuo/mizu) binds the same core for R, so Python and R processes can share a channel.

- Zero-copy numpy arrays and Arrow interchange (pyarrow, polars, pandas, duckdb)
- Drop-in `concurrent.futures.Executor` — real `Future`s, `wait`, `as_completed`, `asyncio.wrap_future`
- Reproducible parallel randomness (`seed=`), invariant for any worker count or steal order
- Sentinels, not exceptions, on hot paths; worker crashes detected at OS latency
- Typed (`py.typed` + stubs); Python 3.10+ on Linux, macOS, and Windows

> **Pre-release.** The API is not stable and may change at any time before a release.

## Installation

Install the development version from GitHub:

```sh
pip install git+https://github.com/shikokuchuo/pymizu
```

Requires Python 3.10 or later on a 64-bit platform (Linux: kernel 5.3 or later; Windows: clang-cl to build).
The extension compiles the vendored C core, so no system library is necessary.

Optional extras:

- `pymizu[numpy]`: zero-copy array views and raw-tier array staging
- `pymizu[cloudpickle]`: lambdas, closures, and local functions as pool tasks

To request an extra with the GitHub install, use `pip install "pymizu[numpy] @ git+https://github.com/shikokuchuo/pymizu"`.

## Quickstart

```python
import pymizu

with pymizu.Pool.create(4) as pool:
    task = pool.submit(pow, 2, 16)
    print(task.collect(timeout=5))
```

## Benchmarks

Communication overhead against the stdlib `concurrent.futures` pools (Apple M4 Pro, from `benchmarks/mizu-stdlib-bench.py`):

Against `ProcessPoolExecutor` (tasks run in separate processes, with pickled payloads):

| Benchmark | pymizu | ProcessPoolExecutor | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.7 µs | 94.2 µs | 135x |
| Pipelined throughput, 1 worker | 2,390,000 tasks/s | 18,500 tasks/s | 129x |
| Parallel map overhead, trivial function, 4 workers | 0.4 µs/elt* | 70.3 µs/elt | 176x |
| Parallel map of 2,000 ~5 µs tasks, 4 workers | 4.3 ms | 129 ms | 30x |

Against `ThreadPoolExecutor` (tasks share one process, so the GIL caps CPU-bound work at a single core):

| Benchmark | pymizu | ThreadPoolExecutor | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.7 µs | 9.5 µs | 14x |
| Pipelined throughput, 1 worker | 2,390,000 tasks/s | 405,000 tasks/s | 5.9x |
| Parallel map overhead, trivial function, 4 workers | 0.4 µs/elt* | 2.6 µs/elt | 6.5x |
| Parallel map of 2,000 ~5 µs tasks, 4 workers | 4.3 ms | 50 ms | 12x |

Against `InterpreterPoolExecutor` (Python 3.14+, subinterpreters with per-interpreter GILs): on the trivial-task rows it lands between the thread and process pools — 21.3 µs per round trip and 59,600 tasks/s pipelined on this machine (measured 2026-10-07). The map rows it cannot run at all: task payloads must be shareable — builtin scalars, plain containers, or buffer-protocol objects — so numpy arrays and numpy scalars raise `NotShareableError`.

`benchmarks/mizu-bench.py` runs the pymizu rows standalone.
See [how pymizu compares](https://shikokuchuo.net/pymizu/user-guide/benchmarks.html) with Ray, Dask, joblib, ZeroMQ, and modern CPython.

\* elt = element; microseconds of wall time per map element.

## Channels

`Channel.create()` spawns a peer process and connects both ends over a lock-free ring pair.
The peer program is a Python source string, evaluated with `ch` bound to the peer-side handle:

```python
import pymizu

ch = pymizu.Channel.create("""
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED:
        break
    ch.send(x)
""")
ch.send([1, "a", None])
print(ch.recv(timeout=5))
ch.close()
```

Sends never block for ring space.
Receives report terminal states as sentinel singletons — `pymizu.FULL`, `pymizu.TIMEOUT`, `pymizu.CLOSED`, `pymizu.PEER_GONE` — tested by identity (`x is pymizu.TIMEOUT`), never raised.

## Task pools

`Pool.create()` spawns worker processes that claim tasks from per-submitter injection rings and steal work from each other — no dispatcher in the loop:

```python
with pymizu.Pool.create(4) as pool:
    task = pool.submit(pow, 2, 16)
    print(task.collect(timeout=5))
```

A task error re-raises on collect as `pymizu.TaskError`; the death of the executing worker raises `pymizu.WorkerDiedError`, detected at OS notification latency with no heartbeats or polling.

## Drop-in `concurrent.futures`

`PoolExecutor` adapts a pool to the stdlib `concurrent.futures.Executor` interface — code written against `ProcessPoolExecutor` drops in.
`submit` returns real `Future` objects, so `wait` / `as_completed` / `asyncio.wrap_future` all work unchanged:

```python
with pymizu.PoolExecutor.create(4) as ex:
    futures = [ex.submit(pow, 2, i) for i in range(4)]
    print([f.result() for f in futures])
```

## Parallel map

`Pool.map(fn, x)` maps `fn` over `x` on the pool and returns a list in input order.
The function, its constant arguments, and the data are staged once; runner tasks self-schedule element batches off a shared cursor.
A 1-D numpy array crosses as raw bytes — every worker views the same array in shared memory — and with `template=`, results are written straight into a shared output array, never serialized.
`seed=` gives every element a deterministic random stream, reproducible for any worker count or steal order:

```python
with pymizu.Pool.create(4) as pool:
    print(pool.map(abs, range(-5, 5)))
```

## R interop

Pool workers can be R processes that run the [mizu](https://github.com/shikokuchuo/mizu) package, the R binding of the same core.
`pymizu.r_pool_launcher()` spawns them and `pymizu.call()` describes the task — here R's built-in `mtcars` summarized by R itself, read back into polars:

```python
import polars as pl
import pymizu

with pymizu.Pool.create(4, launcher=pymizu.r_pool_launcher()) as pool:
    task = pool.submit(
        pymizu.call(source="aggregate(mpg ~ cyl, data = mtcars, FUN = mean)")
    )
    print(pl.DataFrame(task.collect()))
```

A numpy array arrives in R as a numeric vector and a polars, pyarrow, or pandas frame as a data.frame; a vector or data.frame arrives back as a numpy array or a `pymizu.Frame`.
For a channel peer, pass the peer program as R source and set the launcher to `pymizu.r_launcher()`.
The launchers need R and the `mizu` package installed; if either is missing they raise `MizuError` before anything is spawned.
The full contract — the portable subset, `pymizu.Frame`, zero-copy frames, and the dtype matrix — is on the [R interop](https://shikokuchuo.net/pymizu/user-guide/interop.html) page.

## Documentation

The [documentation site](https://shikokuchuo.net/pymizu/) covers the full surface: channels and their payload tiers, task pools (batch operations, nested tasks, the `concurrent.futures` executor, observing a running pool), the parallel map (scheduling, reproducible randomness, templates, prepared maps), R interop and the dtype matrix, deployment on Linux, and the API reference.

## Contributing

The repository layout and contributor notes live in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT.
The vendored libmizu core carries third-party RngStreams attribution (upstream `LICENSE.note`).

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of Conduct](https://github.com/shikokuchuo/pymizu/blob/main/.github/CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.
