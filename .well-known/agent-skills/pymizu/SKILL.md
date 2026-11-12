---
name: pymizu
description: >
  Write correct Python code for async and parallel computing with pymizu:
  task pools, parallel map, channels and asyncio over shared memory on one
  machine. Use when the user wants to run Python code in parallel or in the
  background, speed up a loop, escape the GIL with processes, use asyncio or
  concurrent.futures, share data with Python or R processes, or convert from
  multiprocessing, joblib, Ray or Dask. Single machines only: pymizu has no
  remote workers.
metadata:
  author: Charlie Gao (@shikokuchuo)
  version: "1.0"
license: MIT
compatibility: Requires Python >= 3.11, 64-bit; Linux kernel >= 5.3.
---

# pymizu: async and parallel Python over shared memory

pymizu runs Python callables on worker processes on the same machine and
moves data between processes through shared memory. A task round trip costs
under a microsecond (about 100 µs over sockets), so work divides as finely
as with threads — but without the GIL, since workers are processes — and
numpy arrays arrive as zero-copy views instead of pickle copies.

Install with `pip install pymizu` (extras: `pymizu[numpy]`,
`pymizu[cloudpickle]`). The package is experimental: where an installed
version disagrees with this skill, its documentation is the authority.

## One machine only

Shared memory stops at the machine boundary. Pools and channels run on one
machine (a laptop, a server, one container, a single cluster node); there is
no remote launcher. Work spanning machines (a Slurm cluster, cloud workers)
needs a distributed framework such as Ray or Dask instead — and a pymizu
pool created inside each such worker is ordinary Python code.

In the default CPython build the GIL runs CPU-bound threads on one core at a
time, so compute parallelism means processes. Processes also isolate
failures: a crashed worker does not take down the host.

## Quickstart

```python
import pymizu

with pymizu.Pool.create(4) as pool:          # spawns 4 worker processes
    task = pool.submit(pow, 2, 16)           # returns a handle at once
    print(task.collect(timeout=5))           # 65536

    m = pool.map(abs, range(-5, 5))
    print(m.collect())                       # [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]
# the with block stops the pool
```

## Core principle: a task is pickled, not captured

A worker is a fresh interpreter (`python -m pymizu.worker`), spawned, not
forked: nothing from your process is inherited — no globals, no imports, no
loaded state. What crosses is exactly the callable and its arguments,
pickled (protocol 4).

### Rule 1: the callable must be importable by reference

With the standard pickle — as with `multiprocessing` — a submitted callable
must be importable by module and qualname. Functions defined in `__main__`
(a script's top level, the REPL, a notebook cell), lambdas, closures, and
local functions fail at submit. Installing cloudpickle lifts the
restriction.

```python
# WRONG in a script/notebook without cloudpickle: fn lives in __main__
task = pool.submit(lambda x: x * 2, 21)

# CORRECT: an importable function
from mymod import double
task = pool.submit(double, 21)

# CORRECT anywhere: a stdlib or library reference
task = pool.submit(math.sqrt, 2.0)
```

### Rule 2: pass everything the task needs as arguments

There is no globals detection. Constants ride the pickle with the task; a
numpy array over 32 KB crosses as a read-only zero-copy view, not a pickled
copy. Module-level state is re-imported on the worker, never shared: a
model loaded in your process does not exist there — pass the path and load
(or cache) on the worker.

### Rule 3: submit's timeout is queue space, not the task's runtime

`pool.submit(fn, *args, timeout=...)` bounds only the wait for queue space
(`SubmitTimeoutError` on expiry). A callable taking its own `timeout=`
keyword cannot receive it through submit: bind it first with
`functools.partial(fn, timeout=...)`.

### Other traps

- `pool.submit()` returns a handle, never a value; `task.collect()` yields
  the value, exactly once ("already collected" on a second call).
- A collect timeout returns the `TIMEOUT` sentinel, it does not raise: test
  `r is pymizu.TIMEOUT`, never `try`/`except`.
- A result has one consumer: a task with a pending async consumer refuses a
  blocking collect; `task.cancel()` is always allowed.
- A loop of tiny submits: use `pool.map()`, which stages once.

## Pools

```python
with pymizu.Pool.create(4) as pool:   # __exit__ stops the pool
    ...
pool.stop()                           # cancels pending tasks, joins workers
```

Stop every pool you create — the `with` form does it for you; otherwise
workers linger until the handle is garbage-collected. A pool lives only as
long as its creating process. Two arguments matter early:

- `result_slots` (default 4096): bounds uncollected tasks per submitter;
  past it `submit()` raises `SlotsExhaustedError`, so collect or cancel
  before submitting more.
- `launcher`: `pymizu.launcher(stdout=, stderr=)` redirects worker output;
  `pymizu.r_launcher()` spawns R workers.

Sizing and growth (`max_workers`, `max_submitters`, `slot_size`,
`pool.spawn_workers()` / `pool.retire()`) are covered by the pools guide.

Threading: a handle is single-producer (submit/map from one thread) and
single-waiter (blocking collects from one thread, possibly a different one);
a concurrent second raises `ConcurrentUseError`. To produce from several
threads, `Pool.attach(pool.token)` one handle per thread, or use
`PoolExecutor`, whose submits are thread-safe. `pool.destroy()` is safe
from any thread.

### Submit and collect

```python
task = pool.submit(slow, x)
r = task.collect(timeout=30)     # pymizu.TIMEOUT sentinel on expiry
if r is pymizu.TIMEOUT: ...      # the task keeps running; collect again later
```

A task that raised re-raises at collect as `TaskError` carrying
`remote_type` and `detail` (the worker-side traceback); a dead worker
raises `WorkerDiedError` (`slot`, `pid`). `task.cancel()` skips a queued
task and discards a running one's result; the collect then raises
`CancelledError`.

### Several tasks

```python
tasks = [pool.submit(f, i) for i in range(8)]
pool.collect_all(tasks, timeout=60)   # values in input order
pool.collect_any(tasks, timeout=60)   # (index, value) of the first done
```

A failure raises with a 0-based `index` attribute; only that handle is
consumed, the rest stay collectible. A timeout returns `TIMEOUT` and
consumes nothing. `pool.submit_batch(fns)` takes zero-arg callables (bind
arguments with `functools.partial`).

### Nested tasks

Inside a task, `pymizu.current_pool()` is the running worker's own handle.
Nested submits go onto that worker's queue, and a worker blocked in a nested
collect executes other work instead of sleeping, so divide-and-conquer
cannot deadlock.

### A default pool, and the executor

`pymizu.set_default_pool(pool)` registers a process-wide default
(`pymizu.using_pool(pool)` scopes it); package code resolves an optional
pool as: an explicit argument, `pymizu.current_pool()` inside a task, the
default, then a fallback. `PoolExecutor.create(4)` adapts a pool to
`concurrent.futures.Executor` — real Futures, `wait`, `as_completed`,
thread-safe submits — the drop-in replacement for `ProcessPoolExecutor`.

## Parallel map

`pool.map(fn, x)` stages `fn`, its constant arguments and `x` into shared
memory once, then workers claim adaptively sized batches off a shared
cursor. It returns a `Map` handle at once; `handle.collect()` returns the
list in input order.

```python
m = pool.map(f, xs, args=(k,))          # constants via args= / kwargs=
m = pool.map(np.abs, arr, template=np.empty(1))   # one array, unserialized
m = pool.map(sim, range(B), seed=123)   # reproducible streams
```

- `seed=` gives every element a deterministic stdlib `random` stream,
  identical for any worker count or steal order; `seed=(seed, offset)`
  shifts the streams across runs. numpy draws call `pymizu.current_rng()`
  inside the task (the element's own Generator); the legacy `np.random.*`
  module functions stay order-dependent.
- `template=` (an example buffer such as `np.empty(m)`) declares every
  result `m` values of that dtype: results write into a shared output area
  and collect as one array, zero-copy with `collect="view"`.
- A task error re-raises from the collect as `TaskError` with the failing
  element's 0-based `index`; the map fails fast. Worker death raises
  `WorkerDiedError` with the lost element ranges as `lost`.
- `map()`'s `timeout` bounds submission only (returns `TIMEOUT`, no
  handle); the collection deadline is `handle.collect(timeout)`, whose
  expiry cancels the outstanding work and returns `TIMEOUT`.
- `pool.starmap(fn, x)` unpacks each element as call arguments (zip first
  for the multi-iterable form).
- Nested: `pool.map(...)` over `pymizu.current_pool()` inside a task.
- Repeated simulation: `pm = pool.map_prepare(f, xs)` stages once; each
  `pm.run(seed=s)` costs only submission and collection.
- `stream=True` streams slices when `x` must not be staged whole (blocking
  collect only).

## Outcomes: sentinels for transport states, exceptions for failures

The verbs that move data and wait with a bound (`send()`, `recv()`,
`collect()`) return sentinel singletons for the normal states of a loop,
never raise: `pymizu.TIMEOUT`, `FULL`, `CLOSED`, `PEER_GONE`. Test by
identity (`x is pymizu.TIMEOUT` or `pymizu.is_sentinel(x)`); no
`try`/`except` sees them.

Failures raise from the `MizuError` tree: `TaskError` (the task raised),
`WorkerDiedError`, `CancelledError`, `SlotsExhaustedError`,
`SubmitTimeoutError`, `StoppedError`, `TaskLostError`, `StartupError`,
`ShmError`, `DeclinedError`, `ForeignPayloadError`, `ConcurrentUseError` —
fields and details in the reference. A peer's error on a channel instead
arrives as a *value* (a `TaskError`; `pymizu.is_remote_error(x)` tests it —
raise it yourself with `raise x`).

## Asyncio

```python
task = pool.submit(slow, x)
result = await task                     # or task.collect_async()

results = await pool.collect_all_async(tasks)
values = await pool.map(f, xs)          # the map handle itself
```

One Future stands per task or collection. Cancelling a Future detaches the
waiter — the task keeps running and is blocking-collectible again; only
`task.cancel()` cancels. The async verbs take no `timeout` argument: use
`asyncio.timeout()` or `asyncio.wait_for`. Rejections are the exceptions
the sync verbs raise. A streaming map refuses `collect_async()`.

Channels are event-driven too: `await ch.recv_async()` for the next
message, `async for x in ch:` for the stream. Terminal states resolve as
*values* — the `CLOSED` / `PEER_GONE` sentinels — they do not reject. A
blocking `recv()` while async waiters are pending raises
`ConcurrentUseError`.

`PoolExecutor` needs no event-loop integration: `asyncio.wrap_future`
bridges its real Futures.

## Channels

A channel is a two-way link to one child process. `Channel.create()` takes
the peer as a Python *source string*, evaluated with `ch` bound to its end.

```python
with pymizu.Channel.create("""
import pymizu
while True:
    x = ch.recv(timeout=30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x * 2)
""") as ch:
    ch.send(21)
    print(ch.recv(timeout=5))    # 42
# the with block closes the channel, waiting for the peer to drain
```

bytes and numpy arrays cross without serialization; everything else is
pickled. `send()` returns None or a sentinel; `recv()` a value or one;
`send_batch()` / `recv_batch()` move several at once; `ch.alive()` probes
the peer. Large arrays arrive as read-only zero-copy views that pin shared
memory until garbage-collected — do not hoard them. A side belongs to one
thread at a time; two-way traffic from two threads needs two channels.

## R interop

Workers and channel peers can be R processes running mizu, the R binding of
the same core. `pymizu.r_launcher()` spawns them (it probes `Rscript` for
an installed mizu first). An R pool refuses a plain callable with an error
naming `call`: `pymizu.call()` builds the language-neutral task — a
qualified `"pkg::fn"` name or a `source=` string in R, plus the constant
arguments — and a `call()` spec as `fn` maps over an R pool (int seeds
only).

```python
with pymizu.Pool.create(4, launcher=pymizu.r_launcher()) as pool:
    task = pool.submit(
        pymizu.call(source="aggregate(mpg ~ cyl, data = mtcars, FUN = mean)")
    )
    frame = task.collect()    # a pymizu.Frame
```

A numpy array arrives in R as a numeric vector, a polars, pyarrow, or
pandas frame as a data frame; vectors and data frames come back as numpy
arrays or a `pymizu.Frame`. A non-portable argument raises `DeclinedError`
at submit (`path`, `reason`).

## Converting from other frameworks

The step is always the same: the callable must be importable (or cloudpickle
installed), and everything it needs crosses as arguments. Nothing is
inferred.

| Elsewhere | pymizu |
|---|---|
| `ProcessPoolExecutor(4)` | `PoolExecutor.create(4)` (drop-in) |
| `multiprocessing.Pool(4)` | `pymizu.Pool.create(4)` |
| `executor.submit(f, a)` / `apply_async` | `pool.submit(f, a)` → `task.collect()` |
| `pool.map(f, xs)` | `pool.map(f, xs).collect()` |
| `pool.starmap(f, xs)` | `pool.starmap(f, xs).collect()` |
| `joblib.Parallel(n_jobs=4)(delayed(f)(i) ...)` | `pool.map(f, xs).collect()` |
| `initializer=` / shared globals | importable module state; pass the rest as arguments |
| `multiprocessing.Queue` between two processes | `pymizu.Channel.create(peer_source)` |
| a `random.seed` per worker | `seed=` on `pool.map()`, `pymizu.current_rng()` |
| Ray / Dask across machines | none: more than one machine stays with them |

## Debugging and operations

- A worker is a fresh interpreter with only the callable and its arguments,
  so test the callable locally first with the same arguments.
- `pool.info()` is the state snapshot, `pool.stats()` the counters,
  `pool.dump()` the first tool when a pool hangs, `pool.trace(fn)` the
  lifecycle hook; worker stderr reaches your console, or a file via
  `pymizu.launcher(stderr=)`.
- A dead worker fails only the tasks it had claimed; the survivors carry on.
- Handles do not survive `fork()`; Linux containers need `/dev/shm` sized
  for the in-flight payloads (`docker run --shm-size=4g`).
- `pymizu.prune()` reclaims shared memory orphaned when every attached
  process dies at once; never needed while any participant survives.

## Where to look next

The documentation is the authority for an experimental API: the user guide
(pools, map, channels, interop) and the API reference at
https://shikokuchuo.net/pymizu/, with the full text for agents at
https://shikokuchuo.net/pymizu/llms-full.txt.
