# pyrei れい

[![ci](https://github.com/shikokuchuo/pyrei/actions/workflows/ci.yml/badge.svg)](https://github.com/shikokuchuo/pyrei/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

      ________
     /\       \
    /  \ pyrei \
    \  /  れい  /
     \/_______/

pyrei is the Python binding to [librei](https://github.com/shikokuchuo/librei), a C library for lock-free shared-memory IPC.

Parallel computation and data exchange between Python processes: channels and work-stealing task pools over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).

A channel is a two-way message link between a Python process and a helper process that it spawns.
A pool is a set of worker processes that divide submitted tasks among themselves.
In both, one process writes data and the other reads it in place — never copied through a socket, pipe, or file.

The hot path stays in user space: single-producer single-consumer rings with batched publication, spin-then-park waiting, and event-driven peer-death detection.

The GIL runs CPU-bound threads on one core at a time, so compute parallelism in Python usually means multiple processes.
pyrei makes the communication between these processes cheap enough that you can divide work at granularities usually reserved for threads.

Pre-release.

## Channels

```python
import pyrei

ch = pyrei.Channel.create("""
import pyrei
while True:
    x = ch.recv()
    if x is pyrei.CLOSED:
        break
    ch.send(x)
""")
ch.send([1, "a", None])
print(ch.recv(timeout=5))
ch.close()
```

`Channel.create()` spawns a peer process (`python -m pyrei.child <token>`) and connects both ends over a lock-free ring pair.
The peer program is a Python source string, evaluated with `ch` bound to the peer-side handle.

Sends never block for ring space.
Receives report terminal states as sentinel singletons — `pyrei.FULL`, `pyrei.TIMEOUT`, `pyrei.CLOSED`, `pyrei.PEER_GONE` — tested by identity (`x is pyrei.TIMEOUT`), never raised.

`None` crosses as an immediate.
`bytes` and 1-D contiguous numpy arrays of float64, int32, complex128, or uint8 ride a serialization-free raw tier (they arrive as arrays; `bytes` arrives as uint8).
Strings cross as raw UTF-8; booleans, numbers, and flat containers of them ride a compact binary codec.
Everything else crosses as a pickle protocol 4 stream.

## Task pools

```python
import pyrei

with pyrei.Pool.create(4) as pool:
    task = pool.submit(pow, 2, 16)
    print(task.collect(timeout=5))
```

`Pool.create()` spawns worker processes (`python -m pyrei.worker <token> <slot>`) that claim tasks from per-submitter injection rings and steal work from each other.
A submission is one shared-memory write plus at most one directed wake: no dispatcher process is in the loop.

A task callable rides pickle: under stock pickle it must be an importable reference (the multiprocessing constraint); installing cloudpickle lifts that transparently.
A task error re-raises on collect as `pyrei.TaskError`, carrying the remote type name and traceback text — a constructed, bounded envelope, never a pickled exception instance.
A cancellation raises `pyrei.CancelledError`; the death of the executing worker raises `pyrei.WorkerDiedError`, detected at OS notification latency with no heartbeats or polling.

`Pool.collect_any()` and `Pool.collect_all()` wait on several handles at once.
Inside a task, `pyrei.current_pool()` returns the worker's own handle: a nested submit pushes onto the worker's deque, and a nested collect helps instead of parking, so nested fan-outs never deadlock the pool.

## Parallel map

```python
with pyrei.Pool.create(4) as pool:
    print(pool.map(abs, range(-5, 5)))
```

`Pool.map(fn, x)` maps `fn` over `x` on the pool and returns a list in input order.
One call stages `fn`, the constant `args=`/`kwargs=`, and `x` exactly once — a shared region, or inline in chunk tasks when small — then submits one runner task per live worker.

Runners self-schedule adaptively sized element batches off a shared cursor: a trivial `fn` runs in large batches at near-zero scheduling overhead; an expensive or skewed one self-limits to fine claims that keep the workers balanced.
A 1-D C-contiguous buffer of float64, int32, complex128, or uint8 travels as bare bytes — workers wrap it once and index per element, never deserializing `x`.
`chunks=` overrides the scheduling granularity outright.

`seed=` (an int or bytes) derives deterministic per-element streams of the stdlib `random` module: element `i` runs under `random.seed(SHA-256(seed_bytes + i.to_bytes(8, "little")))`, so results are identical for any chunking, worker count, or steal order.

An error raised by `fn` re-raises as `pyrei.TaskError` carrying the failing element's 0-based `index`; failure is fail-fast — peers stop within about one batch.
Worker death raises `pyrei.WorkerDiedError` carrying the lost element ranges as `lost` (0-based half-open pairs, conservative).
On `timeout=` expiry the outstanding work is cancelled and the `pyrei.TIMEOUT` sentinel is returned, never raised.
Ctrl-C during a map cancels its outstanding tasks.
A map inside a task runs on the worker's own handle via `pyrei.current_pool()`, at fork/join cost.

## Benchmarks

Headline numbers against the stdlib `concurrent.futures` pools (Apple M4 Pro, from `benchmarks/rei-stdlib-bench.py`):

Against `ProcessPoolExecutor` (tasks run in separate processes, with pickled payloads):

| Benchmark | pyrei | ProcessPoolExecutor | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.7 µs | 90.2 µs | 129x |
| Pipelined throughput, 1 worker | 2,190,000 tasks/s | 18,700 tasks/s | 117x |
| Parallel map overhead, trivial function, 4 workers | 0.4 µs/elt* | 69.4 µs/elt | 173x |
| Parallel map of 2,000 ~5 µs tasks, 4 workers | 4.3 ms | 129 ms | 30x |

Against `ThreadPoolExecutor` (tasks share one process, so the GIL caps CPU-bound work at a single core):

| Benchmark | pyrei | ThreadPoolExecutor | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.7 µs | 9.5 µs | 14x |
| Pipelined throughput, 1 worker | 2,190,000 tasks/s | 407,000 tasks/s | 5.4x |
| Parallel map overhead, trivial function, 4 workers | 0.4 µs/elt* | 2.5 µs/elt | 6.3x |
| Parallel map of 2,000 ~5 µs tasks, 4 workers | 4.3 ms | 50 ms | 12x |

`benchmarks/rei-bench.py` runs the pyrei rows standalone.

\* elt = element; microseconds of wall time per map element.

## R interop

A channel peer can be an R process that runs the `rei` package.
Pass the peer program as R source, and set the launcher to `pyrei.r_launcher()`:

```python
import pyrei

ch = pyrei.Channel.create(
    """
library(rei)
repeat {
  x <- rei_recv(ch, timeout = 30)
  if (inherits(x, "rei_sentinel")) break
  rei_send(ch, x)
}
""",
    launcher=pyrei.r_launcher(),
)

import numpy as np
ch.send(np.array([1.5, 2.5, 3.5]))   # arrives in R as a numeric vector
print(ch.recv(timeout=5))            # echoes back as a float64 array
ch.close()
```

`r_launcher()` needs R and the `rei` R package installed.
If R or the package is missing, it raises `ReiError` before the channel is created.

A launcher is one callable that takes the join token and spawns the peer process.
For a different spawn method, write your own launcher.

The reverse direction is also possible: an R host spawns a Python peer with `rei::rei_py_launcher()`.

What crosses the language boundary:

- numpy float64, int32, and uint8 arrays arrive in R as numeric, integer, and raw vectors — and back.
- `bytes` stages as a raw vector.
- Strings cross both ways (`str` rides the shared STR1 tier); `NA_character_` arrives as `None`.
- A large R atomic vector arrives as a zero-copy, read-only numpy view over the shared pages — no copy, no parse.
- Python-only payloads do not cross: R declines pyrei's compact codec streams and pickled objects with an informative error.

## Requirements

- Python 3.10 or later, on a 64-bit platform.
- Linux: kernel 5.3 or later (`pidfd_open`, no fallback).
- Windows: the build uses clang-cl.
  MSVC does not support the C11 atomics that the core needs.
  `setup.py` forces clang-cl through a `build_ext` override.

## Install

```sh
pip install .
```

The extension compiles the vendored C core, so no system library is necessary.

Optional extras:

- `pyrei[numpy]`: zero-copy array views and raw-tier array staging.
- `pyrei[cloudpickle]`: lambdas, closures, and local functions as pool tasks.

## Linux memory allocator

This section applies only to Linux with glibc.

When pyrei starts a channel peer or a pool worker, that process changes two settings of the C memory allocator.
It raises the mmap threshold to 32 MB and the trim threshold to 128 MB.
This keeps large payloads in fast memory.
Without this change, glibc asks the kernel to map and unmap each large payload, and that work is slow.

Your own Python process does not change.
If you want the same settings there, set them before Python starts:

```sh
export GLIBC_TUNABLES=glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=134217728
```

If you have set `GLIBC_TUNABLES`, pyrei respects your values.

## Layout

- `src/_pyrei.c`: the extension module.
  It uses the raw CPython C API (no pybind11, Cython, or cffi).
- `src/vendor/librei/`: the vendored librei core.
  `tools/vendor-librei.sh` generates this directory.
  Do not edit these files by hand.
- `python/pyrei/`: the Python package.
  `child.py` and `worker.py` are the entry points for spawned processes (`python -m pyrei.child <token>`, `python -m pyrei.worker <suffix> <slot>`).
- `tests/`: the pytest suite.
  `tests/helpers.py` holds the task callables (pickle sends them by reference, so the workers must import them).

## License

MIT.
The vendored librei core carries third-party RngStreams attribution (upstream `LICENSE.note`).

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of Conduct](https://github.com/shikokuchuo/pyrei/blob/main/.github/CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.
