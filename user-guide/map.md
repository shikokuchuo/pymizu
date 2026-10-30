# Parallel map

`Pool.map(fn, x)` maps `fn` over `x` on the pool and returns the run's handle immediately -- the work is set up and submitted at the call; collecting happens on the handle. It is not a loop over `pool.submit()`. The function, its constant `args=`/`kwargs=`, and the data are written to shared memory once, and one *runner* task per worker divides up the elements:

``` python
import pymizu

with pymizu.Pool.create(4) as pool:
    result = pool.map(abs, range(-5, 5)).collect()

result
```

    [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]

## The map handle

A map collects exactly once, through the handle:

- `handle.collect(timeout=None)` blocks for the results: a list in input order, or with `template=` the gathered output area (a view with `collect="view"`). On `timeout` expiry the outstanding work is cancelled and the `TIMEOUT` sentinel is returned, never raised.
- `handle.collect_async()` -- or simply `await handle` -- resolves one `asyncio.Future` with the same results when they arrive, with no blocking or polling. Cancelling the Future only detaches the waiter (the handle collects blocking again); it never cancels the map. A streaming map (`stream=True`) collects blocking only: its sliding task window advances in the blocking collect loop.
- `handle.cancel()` stops the run: workers halt within about one batch and a pending collect -- blocking or async -- ends with [CancelledError](../reference/CancelledError.md#pymizu.CancelledError).

[Pool.map()](../reference/Pool.map.md#pymizu.Pool.map)'s own `timeout=` bounds submission only: on its expiry the tasks already in are cancelled and [Pool.map()](../reference/Pool.map.md#pymizu.Pool.map) itself returns the `TIMEOUT` sentinel (no handle). A dropped uncollected handle cancels at garbage collection, so no run of `fn` is wasted.

## Scheduling

Each runner claims batches of elements from a shared counter, and the batch size adapts. A cheap `fn` runs in large batches at near-zero scheduling cost. An expensive or uneven one takes smaller batches to keep the workers balanced. `n_chunks=` sets the batch size directly.

A 1-D C-contiguous numpy array of float64, int32, int64, complex128, or uint8 crosses as raw bytes. Each worker views the same array in shared memory and reads its elements directly, so `x` is never serialized:

``` python
import numpy as np

with pymizu.Pool.create(4) as pool:
    result = pool.map(np.negative, np.arange(-5.0, 5.0)).collect()

result
```

    [np.float64(5.0),
     np.float64(4.0),
     np.float64(3.0),
     np.float64(2.0),
     np.float64(1.0),
     np.float64(-0.0),
     np.float64(-1.0),
     np.float64(-2.0),
     np.float64(-3.0),
     np.float64(-4.0)]

## Unpacked elements (starmap)

`pool.starmap(fn, x)` unpacks each element of `x` as the call's positional arguments: `fn(*element, *args, **kwargs)`. This is the `multiprocessing.Pool.starmap` convention:

``` python
with pymizu.Pool.create(4) as pool:
    result = pool.starmap(pow, [(2, 10), (3, 3)]).collect()

result
```

    [1024, 27]

For the multi-iterable shape of `concurrent.futures.Executor.map`, zip the iterables first: `pool.starmap(fn, zip(xs, ys))`. Starmap needs Python workers: a [pymizu.call](../reference/call.md#pymizu.call) spec raises `TypeError`. Everything else (seeding, templates, streaming, the outcomes) is [Pool.map](../reference/Pool.map.md#pymizu.Pool.map)'s.

## Reproducible randomness

`seed=` (an int or bytes) gives every element its own deterministic stream of the stdlib `random` module. Element `i` is seeded from SHA-256 of the seed and `i`. Results are identical for any chunking, worker count, or steal order:

``` python
import random

def draw(i):
    return random.random()

with pymizu.Pool.create(4) as pool:
    a = pool.map(draw, range(100), seed=123).collect()
    b = pool.map(draw, range(100), seed=123, n_chunks=10).collect()
    print(a == b)
```

    True

Pass `seed=(seed, offset)` to shift every element's stream by `offset` positions, for maps split across runs or processes.

`seed=` covers the stdlib `random` module only. A task that draws from numpy calls [pymizu.current_rng()](../reference/current_rng.md#pymizu.current_rng) inside the task. This is the element's own `numpy.random.Generator`, derived from the same seed and created once per element:

``` python
def draw_np(i):
    return pymizu.current_rng().random()

with pymizu.Pool.create(4) as pool:
    a = pool.map(draw_np, range(100), seed=123).collect()
    b = pool.map(draw_np, range(100), seed=123, n_chunks=10).collect()
    print(a == b)
```

    True

The legacy `np.random.*` module functions draw from the worker's shared global `RandomState`. A seeded map does not make them deterministic: results depend on claim and steal order. Other RNG libraries (torch, jax) are out of scope. A seeded map element must not submit and collect nested tasks. While a worker waits for nested results it runs other work, possibly another map's batches mid-element, which can wipe the element's [current_rng()](../reference/current_rng.md#pymizu.current_rng) stash. The stdlib streams survive.

## Templates

A `template=` example buffer (for example `np.empty(m, dtype=...)`) declares that every `fn` result is `m` values of that dtype. Workers write their results straight into a shared `n x m` output area, so each result crosses the process boundary exactly once, never serialized. Each result must be a matching buffer. If `m == 1`, a plain Python scalar also works:

``` python
import math

with pymizu.Pool.create(4) as pool:
    result = pool.map(math.log1p, [1.0, 2.0, 3.0], template=np.empty(1)).collect()

result
```

    array([0.69314718, 1.09861229, 1.38629436])

With a template, `collect="copy"` (the default) returns the output as one numpy array of shape `(n, m)`. If `m == 1`, the shape is `(n,)`. If numpy is not installed, the output is a memoryview. `collect="view"` returns the shared output area itself as a read-only zero-copy view. The shared memory is released when the view is garbage-collected. The view exports `__arrow_c_array__`, so an Arrow consumer such as pyarrow or polars wraps it without numpy. With numpy, reach the view through the result's `.base` (`arr.base` when `m == 1`, one link deeper when `m > 1`). Without numpy, it is `mv.obj` on the memoryview.

## Streaming maps

With `stream=True`, the map never writes the whole of `x` to shared memory: it streams slices of `x` to workers as they take work, and the return value is unchanged. Slices travel as ordinary chunk tasks under a sliding submit/collect window. At most `min(n_chunks, 2 * live workers, free result slots)` tasks stay outstanding. Shared-memory use is bounded by `window x slice` instead of `sizeof(x)`. The chunk count defaults to `min(len(x), 32 * live workers)`, and `n_chunks=` overrides it.

``` python
with pymizu.Pool.create(4) as pool:
    result = pool.map(abs, range(-5, 5), stream=True).collect()

result
```

    [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]

Everything else (ordering, `template=` and `collect=`, `seed=` reproducibility, the errors) is exactly the non-streaming map's. Two trade-offs come with fixed chunks. The adaptive batch sizing is lost: raise `n_chunks=` to reduce skew. Slices are serialized like ordinary task payloads, so each chunk pays a submit's serialization cost instead of the zero-copy path a raw array gets. A streaming map with Python workers shares one description of the map with every worker, each read at most once. A [pymizu.call](../reference/call.md#pymizu.call) spec as `fn` streams too, over workers of any language. Each chunk in the window writes a small shared-memory region of its own: the slice plus the task that runs on it, one runner task per chunk. The region is removed as soon as the chunk completes. Shared-memory use stays bounded by the window plus a small per-worker cache rather than by `sizeof(x)`. Each chunk also writes its results separately, so they cannot combine into one view. `collect="view"` raises `TypeError` on a streaming spec map, as does preparing one with `pool.map_prepare()`.

## Prepared maps

`pool.map_prepare()` prepares a map once for repeated runs: the data is written to shared memory once, and each worker sets up the map once. Each `m.run()` then returns the run's handle -- costing only the task submissions and the collection -- and reuses each worker's cached set-up:

``` python
with pymizu.Pool.create(4) as pool:
    with pool.map_prepare(abs, range(-5, 5)) as m:
        print(m.run().collect())
        print(m.run().collect())
```

    [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]
    [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]

`m.run(x=...)` replaces the data for that and later runs. A raw-array replacement of the same dtype and length is copied over the old data in place. Any other replacement is written out fresh. A streaming prepared map keeps `x` on the submitter, so a replacement of any shape re-slices. Only a length change under `template=` uses fresh shared memory. After a run collected with `collect="view"`, the next run uses fresh shared memory, because the previous output area belongs to the returned view. `m.run(seed=...)` reseeds the run's element streams, and `m.run(collect=...)` chooses the result form: both are per-run options -- the seed travels with the run's tasks, not the staged data.

## Nested maps

A map can nest: a task can start a map on the pool handle of the worker running it, via [pymizu.current_pool()](../reference/current_pool.md#pymizu.current_pool). The runner submissions go straight onto the worker's own queue, and while the worker waits it runs its own runners while idle workers steal the rest: nested maps never deadlock the pool.

``` python
def nested_total(parts):
    return sum(pymizu.current_pool().map(sum, parts).collect())

parts = [range(0, 250), range(250, 500), range(500, 750), range(750, 1000)]

with pymizu.Pool.create(4) as pool:
    total = pool.submit(nested_total, parts).collect()

total
```

    499500

(This example needs cloudpickle: `nested_total` is not an importable reference.)

The first nested map of a worker claims a submitter slot. With the default `max_submitters = 8`, one slot is held by the creating process, so at most 7 workers can nest concurrently. Raise `max_submitters` for wider nested fan-outs. Do not nest inside the element of a seeded map. While a worker waits for nested results it runs other work, possibly another map's batches mid-element, which can wipe the element's [current_rng()](../reference/current_rng.md#pymizu.current_rng) stash. See [Reproducible randomness](#reproducible-randomness).

## Outcomes

- An error raised by `fn` re-raises as [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError) carrying the failing element's 0-based `index`. The map fails fast: the workers stop within about one batch.
- Worker death raises [pymizu.WorkerDiedError](../reference/WorkerDiedError.md#pymizu.WorkerDiedError) carrying the lost element ranges as `lost` (0-based half-open `(lo, hi)` pairs, possibly wider than what was lost).
- On `timeout=` expiry the outstanding work is cancelled and the `pymizu.TIMEOUT` sentinel is returned, never raised.
- Ctrl-C during a map cancels its outstanding tasks.

A map started inside a task runs on the worker's own pool handle, via [pymizu.current_pool()](../reference/current_pool.md#pymizu.current_pool). A [pymizu.call](../reference/call.md#pymizu.call) spec as `fn` is the cross-language map. See [R interop](interop.md#mapping-over-a-foreign-pool).
