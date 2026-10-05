# Parallel map

`Pool.map(fn, x)` maps `fn` over `x` on the pool and returns the results as a list in input order. It is not a loop over `pool.submit()`. The function, its constant `args=`/`kwargs=`, and the data are written to shared memory once, and one *runner* task per worker divides up the elements:


``` python
import pymizu

with pymizu.Pool.create(4) as pool:
    result = pool.map(abs, range(-5, 5))

result
```


    [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]


# Scheduling

Runners self-schedule element batches off a shared cursor, and the batch size adapts. A cheap `fn` runs in large batches at near-zero scheduling cost. An expensive or uneven one automatically takes smaller batches to keep the workers balanced. `chunks=` sets the batch size directly.

A 1-D C-contiguous numpy array of float64, int32, int64, complex128, or uint8 crosses as raw bytes. Each worker views the same array in shared memory and reads its elements directly -- `x` is never serialized:


``` python
import numpy as np

with pymizu.Pool.create(4) as pool:
    result = pool.map(np.negative, np.arange(-5.0, 5.0))

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


# Reproducible randomness

`seed=` (an int or bytes) gives every element its own deterministic stream of the stdlib `random` module -- element `i` is seeded from SHA-256 of the seed and `i`. Results are identical for any chunking, worker count, or steal order:


``` python
import random

def draw(i):
    return random.random()

with pymizu.Pool.create(4) as pool:
    a = pool.map(draw, range(100), seed=123)
    b = pool.map(draw, range(100), seed=123, chunks=10)
    print(a == b)
```


    True


Pass `seed=(seed, offset)` to shift every element's stream by `offset` positions, for maps split across runs or processes.

`seed=` covers the stdlib `random` module only. A task drawing from numpy calls [pymizu.current_rng()](../reference/current_rng.md#pymizu.current_rng) inside the task -- the element's own `numpy.random.Generator`, derived from the same seed and memoized per element:


``` python
def draw_np(i):
    return pymizu.current_rng().random()

with pymizu.Pool.create(4) as pool:
    a = pool.map(draw_np, range(100), seed=123)
    b = pool.map(draw_np, range(100), seed=123, chunks=10)
    print(a == b)
```


    True


The legacy `np.random.*` module functions draw from the worker's shared global `RandomState`, so a seeded map does not make them deterministic -- results would depend on claim and steal order. Other RNG universes (torch, jax) are out of scope. A seeded map element must not nested-submit and collect: worker helping can run another map's batches mid-element, wiping the element's [current_rng()](../reference/current_rng.md#pymizu.current_rng) stash (the stdlib streams survive).


# Templates

A `template=` exemplar buffer (for example `np.empty(m, dtype=...)`) declares that every `fn` result is `m` values of that dtype. Workers write their results straight into a shared `n x m` output area, so each result crosses the process boundary exactly once, never serialized. Each result must be a matching buffer -- or, when `m == 1`, a plain Python scalar:


``` python
import math

with pymizu.Pool.create(4) as pool:
    result = pool.map(math.log1p, [1.0, 2.0, 3.0], template=np.empty(1))

result
```


    array([0.69314718, 1.09861229, 1.38629436])


With a template, `collect="copy"` (the default) returns the output as one numpy array of shape `(n, m)` -- `(n,)` when `m == 1` -- or a memoryview when numpy is not installed. `collect="view"` returns the shared output area itself as a read-only zero-copy view, and the shared memory is released when the view is garbage-collected. The view exports `__arrow_c_array__`, so an Arrow consumer such as pyarrow or polars wraps it without numpy. With numpy, reach the view through the result's `.base` (`arr.base` when `m == 1`, one link deeper when `m > 1`). Without numpy, it is `mv.obj` on the memoryview.


# Prepared maps

`pool.map_prepare()` prepares a map once for repeated runs: the data is written to shared memory once, and each worker sets up the map once. Each `pool.map_run()` then costs only the task submissions and the collection, and reuses each worker's cached map context:


``` python
with pymizu.Pool.create(4) as pool:
    m = pool.map_prepare(abs, range(-5, 5))
    print(pool.map_run(m))
    print(pool.map_run(m))
    m.close()
```


    [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]
    [5, 4, 3, 2, 1, 0, 1, 2, 3, 4]


`map_run(m, x=...)` replaces the data for that and later runs. A raw-buffer replacement of the same dtype and length swaps in place at memcpy cost. Any other replacement is written out fresh. After a run collected with `collect="view"`, the next run uses fresh shared memory, because the previous output area belongs to the returned view.


# Outcomes

- An error raised by `fn` re-raises as [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError) carrying the failing element's 0-based `index`. The map fails fast -- the workers stop within about one batch.
- Worker death raises [pymizu.WorkerDiedError](../reference/WorkerDiedError.md#pymizu.WorkerDiedError) carrying the lost element ranges as `lost` (0-based half-open `(lo, hi)` pairs, conservative).
- On `timeout=` expiry the outstanding work is cancelled and the `pymizu.TIMEOUT` sentinel is returned, never raised.
- Ctrl-C during a map cancels its outstanding tasks.

A map started inside a task runs on the worker's own pool handle, via [pymizu.current_pool()](../reference/current_pool.md#pymizu.current_pool). A [pymizu.call](../reference/call.md#pymizu.call) spec as `fn` is the cross-language map -- see [R interop](interop.md#mapping-over-a-foreign-pool).
