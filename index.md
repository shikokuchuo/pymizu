# pymizu 水

          ________
         /\       \
        /  \ pymizu\
        \  /  水   /
         \/_______/

pymizu uses shared memory to make communication between Python processes fast enough to parallelize work at thread granularity.

Task round trips drop from around 100 µs over sockets to under a microsecond -- two orders of magnitude (see [Benchmarks](#benchmarks)). The hot path never enters the kernel and data is never copied through a system socket.

In the default CPython build, the GIL runs CPU-bound threads on one core at a time, so compute parallelism in Python usually means multiple processes. Processes also isolate failures: a worker that crashes does not take the host down with it.

Channels and work-stealing task pools run over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows): lock-free single-producer single-consumer rings with batched publication, spin-then-park waiting, and peer death delivered as an OS event.

pymizu is built on [libmizu](https://github.com/shikokuchuo/libmizu), a C library for lock-free shared-memory IPC. [mizu](https://github.com/shikokuchuo/mizu) binds the same core for R, so Python and R can talk to each other.

- Zero-copy numpy arrays and Arrow interchange (pyarrow, polars, pandas, duckdb)
- Drop-in `concurrent.futures.Executor` -- real `Future`s, `wait`, `as_completed`, `asyncio.wrap_future`
- Event-driven asyncio -- `await` tasks and maps, `async for` and [recv_async()](./reference/Channel.recv_async.md#pymizu.Channel.recv_async) on channels -- wake-ups as data arrives, no polling threads
- Reproducible parallel random numbers (`seed=`) -- identical results for any worker count or steal order
- Nested parallel maps -- a task can map over its own pool, deadlock-free

> **Pre-release.** The API is not stable and may change at any time before a release.

### Installation

``` sh
pip install git+https://github.com/shikokuchuo/pymizu
```

Requires Python 3.10 or later on a 64-bit platform. See [Installation](user-guide/installation.md) for platform notes, optional extras, and deployment on Linux.

### Quickstart

``` python
import pymizu

with pymizu.Pool.create(4) as pool:
    task = pool.submit(pow, 2, 16)
    result = task.collect(timeout=5)

result
```

    65536

### Benchmarks

Communication overhead against the stdlib `concurrent.futures` pools (Apple M4 Pro, from `benchmarks/mizu-stdlib-bench.py`).

Against `ProcessPoolExecutor` (tasks run in separate processes, with pickled payloads):

| Benchmark | pymizu | ProcessPoolExecutor | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.7 µs | 94.2 µs | 135x |
| Pipelined throughput, 1 worker | 2,390,000 tasks/s | 18,500 tasks/s | 129x |
| Parallel map overhead, trivial function, 4 workers | 0.4 µs/elt\* | 70.3 µs/elt | 176x |
| Parallel map of 2,000 ~5 µs tasks, 4 workers | 4.3 ms | 129 ms | 30x |

Against `ThreadPoolExecutor` (tasks share one process, so the GIL caps CPU-bound work at a single core):

| Benchmark | pymizu | ThreadPoolExecutor | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.7 µs | 9.5 µs | 14x |
| Pipelined throughput, 1 worker | 2,390,000 tasks/s | 405,000 tasks/s | 5.9x |
| Parallel map overhead, trivial function, 4 workers | 0.4 µs/elt\* | 2.6 µs/elt | 6.5x |
| Parallel map of 2,000 ~5 µs tasks, 4 workers | 4.3 ms | 50 ms | 12x |

\* elt = element; microseconds of wall time per map element.

See [Benchmarks](user-guide/benchmarks.md) for how pymizu compares with other frameworks, and for the benchmark scripts.

### Drop-in `concurrent.futures`

[PoolExecutor](./reference/PoolExecutor.md#pymizu.PoolExecutor) adapts a pool to the stdlib `concurrent.futures.Executor` interface -- code written against `ProcessPoolExecutor` drops in, and `submit` returns real `Future` objects:

``` python
with pymizu.PoolExecutor.create(4) as ex:
    futures = [ex.submit(pow, 2, i) for i in range(4)]
    result = [f.result() for f in futures]

result
```

    [1, 2, 4, 8]

See [Task pools](user-guide/pools.md#concurrentfutures-interop) for wrapping an existing pool and how cancellation works.

### Parallel map

`Pool.map(fn, x)` maps `fn` over `x` on the pool and returns the run's handle immediately. The function and data cross into shared memory once. Workers divide the elements among themselves. `handle.collect()` returns the results in order -- or `await handle` in asyncio code.

A map also nests -- the mapped function can map over its own pool with [pymizu.current_pool()](./reference/current_pool.md#pymizu.current_pool), deadlock-free.

``` python
with pymizu.Pool.create(4) as pool:
    result = pool.map(abs, range(-5, 5)).collect()

result
```

    [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]

See [Parallel map](user-guide/map.md).

### R interop

Pool workers can be R processes that run the [mizu](https://github.com/shikokuchuo/mizu) package, the R binding of the same core. [pymizu.r_pool_launcher()](./reference/r_pool_launcher.md#pymizu.r_pool_launcher) spawns them and [pymizu.call()](./reference/call.md#pymizu.call) describes the task -- here R's built-in `mtcars` summarized by R itself, read back into polars:

``` python
import polars as pl
import pymizu

with pymizu.Pool.create(4, launcher=pymizu.r_pool_launcher()) as pool:
    task = pool.submit(
        pymizu.call(source="aggregate(mpg ~ cyl, data = mtcars, FUN = mean)")
    )
    print(pl.DataFrame(task.collect()))
```

- A numpy array arrives in R as a numeric vector, and a polars, pyarrow, or pandas frame as a data frame
- A vector or data frame arrives back as a numpy array or a [pymizu.Frame](./reference/Frame.md#pymizu.Frame)
- Arrow data (pyarrow tables and record batch readers, or any [\_\_arrow_c_stream\_\_](./reference/Frame.__arrow_c_stream__.md#pymizu.Frame.__arrow_c_stream__) producer) crosses the same way

The mizu README shows the reverse direction: an R host that drives Python workers with `mizu_py_pool_launcher()`. See [R interop](user-guide/interop.md).

### Where next

- [Benchmarks](user-guide/benchmarks.md) -- overhead against the stdlib pools, and how pymizu compares with other frameworks.
- [Channels](user-guide/channels.md) -- sending and receiving, batch operations, and what crosses a channel.
- [Task pools](user-guide/pools.md) -- outcomes, nested tasks, growing and observing a pool.
- [Parallel map](user-guide/map.md) -- scheduling, reproducible randomness, templates, prepared maps.
- [R interop](user-guide/interop.md) -- R peers and workers, what crosses the language boundary, and the dtype matrix.
- [API reference](reference/index.md) -- the generated function and class reference.
