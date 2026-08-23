# pyrei れい

Lock-free shared-memory channels and task pools for Python.

pyrei moves data between Python processes on the same machine.
It provides lock-free channels and work-stealing task pools over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).
The hot path stays entirely in user space.
Communication between processes becomes cheap enough that you can divide work at granularities usually reserved for threads.

pyrei is the Python binding for [librei](https://github.com/shikokuchuo/librei).
The extension compiles the vendored C core, so no system library is necessary.
A channel can also connect a Python process to an R process that uses the `rei` package.

Pre-release (v0.1.0).

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
Everything else crosses as a pickle protocol 4 stream.
A channel can also connect a Python process to an R process that uses the `rei` package (R sends numeric vectors and strings; Python reads them as arrays and `str`).

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

Optional extras:

- `pyrei[numpy]`: zero-copy array views and raw-tier array staging.
- `pyrei[cloudpickle]`: lambdas, closures, and local functions as pool tasks.

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
  If `Rscript` and the installed `rei` package are not present, the cross-language tests skip.

## License

MIT.
The vendored librei core carries third-party RngStreams attribution (upstream `LICENSE.note`).
