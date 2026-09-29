# pymizu 水

[![ci](https://github.com/shikokuchuo/pymizu/actions/workflows/ci.yml/badge.svg)](https://github.com/shikokuchuo/pymizu/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

      ________
     /\       \
    /  \ pymizu\
    \  /  水   /
     \/_______/

pymizu makes communication between Python processes cheap enough to divide work at granularities usually reserved for threads.

In the default CPython build, the GIL runs CPU-bound threads on one core at a time, so compute parallelism in Python usually means multiple processes.
Processes also isolate failures: a worker that crashes does not take the host down with it.

Channels and work-stealing task pools run over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).

A channel is a two-way message link between a Python process and a helper process that it spawns.
A pool is a set of worker processes that divide submitted tasks among themselves.
In both, one process writes data and the other reads it in place — never copied through a socket, pipe, or file.

The hot path stays in user space: single-producer single-consumer rings with batched publication, spin-then-park waiting, and event-driven peer-death detection.

pymizu is built on [libmizu](https://github.com/shikokuchuo/libmizu), a C library for lock-free shared-memory IPC.
[mizu](https://github.com/shikokuchuo/mizu) binds the same core for R, so Python and R processes can share a channel.

Pre-release.
The API is not stable and may change at any time before a release.

## Channels

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

`Channel.create()` spawns a peer process (`python -m pymizu.child <token>`) and connects both ends over a lock-free ring pair.
The peer program is a Python source string, evaluated with `ch` bound to the peer-side handle.

Sends never block for ring space.
Receives report terminal states as sentinel singletons — `pymizu.FULL`, `pymizu.TIMEOUT`, `pymizu.CLOSED`, `pymizu.PEER_GONE` — tested by identity (`x is pymizu.TIMEOUT`), never raised.

`None` crosses as an immediate.
`bytes` and 1-D contiguous numpy arrays ride a serialization-free raw tier (they arrive as arrays; `bytes` arrives as uint8).
float64, int32, int64, complex128, and uint8 cross unchanged; every other fixed-width numeric dtype (bool, uint64, float32, ...) converts once at send time into the nearest R-compatible wire type — see the [dtype matrix](#the-dtype-matrix).
Arrow arrays (anything with `__arrow_c_array__`: pyarrow, polars, a duckdb result column) cross the same way, with Arrow nulls becoming R missing values.
Strings cross as raw UTF-8; booleans, numbers, and flat containers of them ride a compact binary codec.
Everything else crosses as a pickle protocol 4 stream.

## Task pools

```python
import pymizu

with pymizu.Pool.create(4) as pool:
    task = pool.submit(pow, 2, 16)
    print(task.collect(timeout=5))
```

`Pool.create()` spawns worker processes (`python -m pymizu.worker <token> <slot>`) that claim tasks from per-submitter injection rings and steal work from each other.
A submission is one shared-memory write plus at most one directed wake: no dispatcher process is in the loop.

A task callable rides pickle: under stock pickle it must be an importable reference (the multiprocessing constraint); installing cloudpickle lifts that transparently.
A task error re-raises on collect as `pymizu.TaskError`, carrying the remote type name and traceback text — a constructed, bounded envelope, never a pickled exception instance.
A cancellation raises `pymizu.CancelledError`; the death of the executing worker raises `pymizu.WorkerDiedError`, detected at OS notification latency with no heartbeats or polling.

`Pool.collect_any()` and `Pool.collect_all()` wait on several handles at once.
Inside a task, `pymizu.current_pool()` returns the worker's own handle: a nested submit pushes onto the worker's deque, and a nested collect helps instead of parking, so nested fan-outs never deadlock the pool.

## Parallel map

```python
with pymizu.Pool.create(4) as pool:
    print(pool.map(abs, range(-5, 5)))
```

`Pool.map(fn, x)` maps `fn` over `x` on the pool and returns a list in input order.
One call stages `fn`, the constant `args=`/`kwargs=`, and `x` exactly once — a shared region, or inline in chunk tasks when small — then submits one runner task per live worker.

Runners self-schedule adaptively sized element batches off a shared cursor: a trivial `fn` runs in large batches at near-zero scheduling overhead; an expensive or skewed one self-limits to fine claims that keep the workers balanced.
A 1-D C-contiguous buffer of float64, int32, int64, complex128, or uint8 travels as bare bytes — workers wrap it once and index per element, never deserializing `x`.
`chunks=` overrides the scheduling granularity outright.

`seed=` (an int or bytes) derives deterministic per-element streams of the stdlib `random` module: element `i` runs under `random.seed(SHA-256(seed_bytes + i.to_bytes(8, "little")))`, so results are identical for any chunking, worker count, or steal order.

An error raised by `fn` re-raises as `pymizu.TaskError` carrying the failing element's 0-based `index`; failure is fail-fast — peers stop within about one batch.
Worker death raises `pymizu.WorkerDiedError` carrying the lost element ranges as `lost` (0-based half-open pairs, conservative).
On `timeout=` expiry the outstanding work is cancelled and the `pymizu.TIMEOUT` sentinel is returned, never raised.
Ctrl-C during a map cancels its outstanding tasks.
A map inside a task runs on the worker's own handle via `pymizu.current_pool()`, at fork/join cost.

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

\* elt = element; microseconds of wall time per map element.

## R interop

A channel peer can be an R process that runs the `mizu` package.
Pass the peer program as R source, and set the launcher to `pymizu.r_launcher()`:

```python
import pymizu

ch = pymizu.Channel.create(
    """
library(mizu)
repeat {
  x <- mizu_recv(ch, timeout = 30)
  if (inherits(x, "mizu_sentinel")) break
  mizu_send(ch, x)
}
""",
    launcher=pymizu.r_launcher(),
)

import numpy as np
ch.send(np.array([1.5, 2.5, 3.5]))   # arrives in R as a numeric vector
print(ch.recv(timeout=5))            # echoes back as a float64 array
ch.close()
```

`r_launcher()` needs R and the `mizu` R package installed.
If R or the package is missing, it raises `MizuError` before the channel is created.

A launcher is one callable that takes the join token and spawns the peer process.
For a different spawn method, write your own launcher.

The reverse direction is also possible: an R host spawns a Python peer with `mizu::mizu_py_launcher()`.

What crosses the language boundary:

- A 1-D contiguous numpy array of any fixed-width numeric dtype arrives as an R vector — and back.
  See the dtype matrix below.
- An Arrow array arrives as an R vector, with Arrow nulls as R missing values: `ch.send(pa.array([1, None, 3]))`.
  Anything with `__arrow_c_array__` works (pyarrow, polars, a duckdb result column).
- `bytes` stages as a raw vector.
- Strings cross both ways (`str` rides the shared STR1 tier); `NA_character_` arrives as `None`.
- A large R atomic vector arrives as a zero-copy, read-only numpy view over the shared pages — no copy, no parse.
  Without numpy it arrives as a buffer exporter, and any Arrow consumer wraps the shared pages zero-copy through the Arrow PyCapsule protocol: `pa.array(view)`, `pl.from_arrow(view)`.
- Python-only payloads do not cross: R declines pymizu's compact codec streams and pickled objects with an informative error.

### The dtype matrix

Conversion happens once, at send time, fused into the copy that staging always is.
Identity rows (float64, int32, int64, complex128, uint8) are a plain memcpy.

| Python sends | R receives | Notes |
|----|----|----|
| `bytes` / uint8 | raw | Arrow uint8 with nulls: `TypeError` (R raw has no NA) |
| int8 / int16 / uint16 | integer | widened, exact |
| int32 | integer | |
| uint32 | double | widened, exact |
| int64 | integer64 (bit64's layout) | native wire type, bit-exact; `INT64_MIN` reads as `NA` |
| uint64 | double | exact to ±2^53; past it, `NA` plus one warning |
| float32 / float64 | double | |
| bool | logical | |
| complex64 / complex128 | complex | (buffer protocol only; Arrow has no standard complex) |
| Arrow bool / numeric with nulls | logical / numeric with `NA` | the validity bitmap is honored, slices included |
| Arrow strings, temporal, dictionary, nested | — | `TypeError` at send time |
| Arrow ChunkedArray / Table | — | `TypeError` at send time; `combine_chunks()` first |

NA semantics:

- R's missing values are sentinels in the data: `INT_MIN` for integer/logical, `INT64_MIN` for integer64, a specific NaN payload (`NA_real_`) for double.
  Python to R: Arrow nulls convert to the sentinels, so R sees correct `NA`s.
  R to Python: no Arrow nulls are synthesized — `NA_integer_` reads as `-2147483648`, `NA_integer64_` as `-9223372036854775808`, `NA_real_` as a NaN.
- A genuine int32 value of `-2147483648` collides with the NA sentinel and reads as `NA` in R; likewise an int64 value of `-9223372036854775808` (`INT64_MIN`) reads as `NA_integer64_`.
  numpy sends stay silent; an Arrow int32 send with a validity bitmap warns once — int64 sends are never scanned, so that collision stays silent too.
- Python-side compute treats `NA_real_` as a NaN value; whether the exact payload survives arithmetic is platform-dependent — do not rely on it either way.

Round trips are stable after the first hop, and a pure pass-through echo is bit-exact (an untouched received view re-stages as untouched bytes, so even the `NA_real_` payload survives a relay).

| Python sends | R sees | Back in Python | |
|----|----|----|----|
| uint8 | raw | uint8 | exact |
| int8 / int16 / uint16 | integer | int32 | widened, values exact |
| int32 | integer | int32 | exact |
| uint32 | double | float64 | exact |
| int64 | integer64 | int64 | exact |
| uint64, ≤ ±2^53 | double | float64 | dtype lost, values exact |
| uint64, past ±2^53 | `NA` | NaN | lost on the first hop |
| float32 | double | float64 | widened, values exact |
| float64 | double | float64 | exact |
| bool | logical | int32 0/1 | dtype lost |
| complex64 / complex128 | complex | complex128 | exact |

| R sends | Python sees | Back in R | |
|----|----|----|----|
| integer | int32 | integer | exact |
| integer64 (bit64) | int64 | integer64 | exact |
| double | float64 | double | exact |
| raw | uint8 | raw | exact |
| complex | complex128 | complex | exact |
| logical | int32 | **integer** | the tag does not survive the Python hop (values do) |

The channel/pool split: channels convert; pools are Python-both-ends by construction, so pool task results keep the lossless pickle path for every dtype.
Python-to-Python channels also convert — the peer's language is unknowable at send time.
For an exact Python-to-Python channel send of a non-identity dtype, nest the array in a tuple or list: it keeps the pickle path.

## Requirements

- Python 3.10 or later, on a 64-bit platform.
- Linux: kernel 5.3 or later.
- Windows: the build requires clang-cl.

## Install

```sh
pip install .
```

The extension compiles the vendored C core, so no system library is necessary.

Optional extras:

- `pymizu[numpy]`: zero-copy array views and raw-tier array staging.
- `pymizu[cloudpickle]`: lambdas, closures, and local functions as pool tasks.

## Linux memory allocator

This section applies only to Linux with glibc.

When pymizu starts a channel peer or a pool worker, that process changes two settings of the C memory allocator.
It raises the mmap threshold to 32 MB and the trim threshold to 128 MB.
This keeps large payloads in fast memory.
Without this change, glibc asks the kernel to map and unmap each large payload, and that work is slow.

Your own Python process does not change.
If you want the same settings there, set them before Python starts:

```sh
export GLIBC_TUNABLES=glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=134217728
```

If you have set `GLIBC_TUNABLES`, pymizu respects your values.

## Layout

- `src/_pymizu.c`: the extension module.
  It uses the raw CPython C API (no pybind11, Cython, or cffi).
- `src/vendor/libmizu/`: the vendored libmizu core.
  `tools/vendor-libmizu.sh` generates this directory.
  Do not edit these files by hand.
- `python/pymizu/`: the Python package.
  `child.py` and `worker.py` are the entry points for spawned processes (`python -m pymizu.child <token>`, `python -m pymizu.worker <suffix> <slot>`).
- `tests/`: the pytest suite.
  `tests/helpers.py` holds the task callables (pickle sends them by reference, so the workers must import them).

## License

MIT.
The vendored libmizu core carries third-party RngStreams attribution (upstream `LICENSE.note`).

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of Conduct](https://github.com/shikokuchuo/pymizu/blob/main/.github/CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.
