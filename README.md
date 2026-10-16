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

pymizu makes communication between Python processes fast enough to parallelize work at thread granularity.

pymizu uses shared memory as the fastest IPC transport.
Processes read and write into shared memory directly, with no kernel involvement and no data copies through system sockets.
This means task round trips drop from around 100 µs over sockets to under a microsecond — two orders of magnitude improvement (see [Benchmarks](#benchmarks)).

In the default CPython build, the GIL runs CPU-bound threads on one core at a time, so compute parallelism in Python usually means multiple processes.
Processes also isolate failures: a worker that crashes does not take the host down with it.

Channels and work-stealing task pools run over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).
The hot path stays in user space.
Data moves through single-producer single-consumer rings with batched publication.
Waiting is spin-then-park, and peer death arrives as an OS event.

pymizu is built on [libmizu](https://github.com/shikokuchuo/libmizu), a C library for lock-free shared-memory IPC.
[mizu](https://github.com/shikokuchuo/mizu) binds the same core for R, so Python and R can talk to each other.

- Zero-copy numpy arrays and Arrow interchange (pyarrow, polars, pandas, duckdb)
- Drop-in `concurrent.futures.Executor` — real `Future`s, `wait`, `as_completed`, `asyncio.wrap_future`
- Reproducible parallel randomness (`seed=`), invariant for any worker count or steal order
- Nested parallel maps — a task can map over its own pool, deadlock-free

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

`benchmarks/mizu-bench.py` runs the pymizu rows standalone.
See [how pymizu compares](https://shikokuchuo.net/pymizu/user-guide/benchmarks.html) with Ray, Dask, joblib, ZeroMQ, and modern CPython.

\* elt = element; microseconds of wall time per map element.

## Drop-in `concurrent.futures`

`PoolExecutor` adapts a pool to the stdlib `concurrent.futures.Executor` interface — code written against `ProcessPoolExecutor` drops in.
`submit` returns real `Future` objects, so `wait` / `as_completed` / `asyncio.wrap_future` all work unchanged:

```python
with pymizu.PoolExecutor.create(4) as ex:
    futures = [ex.submit(pow, 2, i) for i in range(4)]
    print([f.result() for f in futures])
```

## Parallel map

`Pool.map(fn, x)` maps `fn` over `x` on the pool and returns the results in order.
The function and data cross into shared memory once.
Workers divide the elements among themselves, claiming ranges from a shared cursor.

A map also nests — the mapped function can map over its own pool with `pymizu.current_pool()`, deadlock-free.

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

- A numpy array arrives in R as a numeric vector, and a polars, pyarrow, or pandas frame as a data frame
- A vector or data frame arrives back as a numpy array or a `pymizu.Frame`
- Arrow data (pyarrow tables and record batch readers, or any `__arrow_c_stream__` producer) crosses the same way

The full contract — the portable subset, `pymizu.Frame`, zero-copy frames, and the dtype matrix — is on the [R interop](https://shikokuchuo.net/pymizu/user-guide/interop.html) page.
The mizu README shows the reverse direction: an R host that drives Python workers with `mizu_py_pool_launcher()`.

## Documentation

The [documentation site](https://shikokuchuo.net/pymizu/) covers the full surface: channels and their payload tiers, task pools (batch operations, nested tasks, the `concurrent.futures` executor, observing a running pool), the parallel map (scheduling, reproducible randomness, templates, prepared maps), R interop and the dtype matrix, deployment on Linux, and the API reference.

## Contributing

The repository layout and contributor notes live in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT.
The vendored libmizu core carries third-party RngStreams attribution (upstream `LICENSE.note`).

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of Conduct](https://github.com/shikokuchuo/pymizu/blob/main/.github/CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.
