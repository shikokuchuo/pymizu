# PreparedMap

A map set up once in shared memory, run many times.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2057-L2391)

``` python
PreparedMap(
    pool,
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    n_chunks=None,
    template=None,
    stream=False,
)
```

Create with [`Pool.map_prepare()`](../reference/pymizu.Pool.map_prepare.md); run with [run()](../reference/PreparedMap.md#pymizu.PreparedMap.run), which returns the run's map handle (the collect happens on it, as for [`Pool.map()`](../reference/pymizu.Pool.map.md)). Re-arming is cheap, and workers reuse their cached set-up, so a re-run skips the pickling, the region creation, and the worker-side set-up. A run collected with `collect="view"` hands its region to the view, so the next run stages a fresh one. Close with `close()` (or a with block) to release the shared memory.

## Parameters

`pool: pymizu.Pool`  
The [`Pool`](../reference/Pool.md#pymizu.Pool) to run on.

`fn: _Callable[…, _Any]`  
The function to map, or a [`call`](../reference/call.md#pymizu.call) spec, as for [`Pool.map()`](../reference/pymizu.Pool.map.md).

`x: _Any`  
The data to map over.

`args: _Any = ()`  
Constant positional arguments appended to every call.

`kwargs: dict | None = None`  
Constant keyword arguments for every call.

`n_chunks: int | None = None`  
Overrides the number of batches the elements are split into.

`template: _Any = None`  
An example buffer declaring each result's dtype and length, as for [`Pool.map()`](../reference/pymizu.Pool.map.md).

`stream: bool = False`  
Stream slices of `x` instead of writing it to shared memory whole, as for [`Pool.map()`](../reference/pymizu.Pool.map.md). The map is set up here; `seed` and `collect` are per-run options of [`PreparedMap.run()`](../reference/PreparedMap.md#pymizu.PreparedMap.run) (mirroring mizu's `mizu_map_run()`).

## Examples

Set the map up once, then re-run it over replacement data at copy cost (same dtype and length swap in place):

``` python
import numpy as np
import pymizu

with pymizu.Pool.create(2) as pool:
    with pool.map_prepare(np.square, np.arange(4.0)) as pm:
        first = pm.run().collect(timeout=10)
        second = pm.run(np.array([4.0, -5.0, 6.0, 7.0])).collect(
            timeout=10
        )
second
```

    [np.float64(16.0), np.float64(25.0), np.float64(36.0), np.float64(49.0)]

## Methods

| Name              | Description                                             |
|-------------------|---------------------------------------------------------|
| [close()](#close) | Release the staged shared memory.                       |
| [run()](#run)     | Run the prepared map once; return the run's map handle. |

### close()

Release the staged shared memory.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2372-L2381)

``` python
close()
```

Safe to call more than once; garbage collection releases it otherwise.

### run()

Run the prepared map once; return the run's map handle.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2253-L2370)

``` python
run(
    x=None,
    *,
    seed=None,
    timeout=None,
    collect=None,
)
```

#### Parameters

`x: _Any = None`  
Replaces the staged data for this and later runs. When both the staged and the replacement x are arrays of the same dtype and length, the swap is an in-place copy, so a loop over same-shaped data (optimizer steps, simulation sweeps) runs at copy cost, skipping the region creation and the worker-side set-up. Any other x is staged fresh on this run.

`seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None = None`  
Deterministic per-element seeding for this run, as for [`Pool.map()`](../reference/pymizu.Pool.map.md); `None` (the default) runs unseeded. The seed travels with the run's tasks, not the staged data, so reseeding a run costs nothing extra: a repeated-simulation loop varies `seed` per run while the set-up is paid once.

`timeout: float | None = None`  
Seconds before the run's submission gives up; on expiry the tasks already in are cancelled and the TIMEOUT [`Sentinel`](../reference/Sentinel.md#pymizu.Sentinel) is returned (no handle). The collection deadline is the handle's `collect()`.

`collect: str | None = None`  
`"copy"` (the default) or `"view"`, which requires `template`, as for [`Pool.map()`](../reference/pymizu.Pool.map.md). The choice is per run.

#### Returns

`The run's map handle, collected exactly once, by collect()`  
(blocking) or `collect_async()` (event-driven). A run is a producer on the pool handle: a concurrent run or submit from a second thread raises [ConcurrentUseError](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError). After a successful collection with `collect="copy"`, the next run reuses the staged data. With `collect="view"`, or when a run ends with a timeout, error, or worker death, the next run stages fresh data.
