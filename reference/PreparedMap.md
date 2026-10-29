# PreparedMap

A map staged once into a persistent region, run many times.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2006-L2358)

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

Create with [`Pool.map_prepare()`](../reference/Pool.map_prepare.md#pymizu.Pool.map_prepare); run with [run()](../reference/PreparedMap.md#pymizu.PreparedMap.run), which returns the run's map handle (the collect happens on it, as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map)). Re-arming is O(1) -- a generation bump, a cursor reset, a cancel-word clear -- and workers reuse their cached contexts, so a re-run pays neither the descriptor pickle nor the region create nor the worker-side re-attach. A view-collected run hands its region to the view; the next run restages into a fresh one (the mirror of mizu). Close with `close()` (or a with block) to unlink the region.

## Parameters

`pool: pymizu.Pool`  
The [`Pool`](../reference/Pool.md#pymizu.Pool) to run on.

`fn: _Callable[…, _Any]`  
The function to map, or a [`call`](../reference/call.md#pymizu.call) spec -- as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map).

`x: _Any`  
The data to map over.

`args: _Any = ()`  
Constant positional arguments appended to every call.

`kwargs: dict | None = None`  
Constant keyword arguments for every call.

`n_chunks: int | None = None`  
Overrides the morsel count (the scheduling granularity).

`template: _Any = None`  
An exemplar buffer declaring each result's dtype and length -- as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map).

`stream: bool = False`  
Stream slices of `x` instead of staging it whole -- as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map). The recipe is staged here; `seed` and [collect](../reference/Task.collect.md#pymizu.Task.collect) are per-run options of [`PreparedMap.run()`](../reference/PreparedMap.md#pymizu.PreparedMap.run) (mirroring mizu's `mizu_map_run()`).

## Examples

Stage the map once, then re-run it over replacement data at memcpy cost (same dtype and length swap in place):

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

| Name | Description |
|----|----|
| [close()](#close) | Unlink the staged region (idempotent; the GC backstop is the |
| [run()](#run) | Run the prepared map once; return the run's map handle. |

### close()

Unlink the staged region (idempotent; the GC backstop is the

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2342-L2348)

``` python
close()
```

capsule destructor).

### run()

Run the prepared map once; return the run's map handle.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2217-L2340)

``` python
run(
    x=None,
    seed=None,
    timeout=None,
    collect=None,
)
```

#### Parameters

`x: _Any = None`  
Replaces the staged data for this and later runs. When both the staged and the replacement x are raw-buffer eligible with the same dtype and length, the swap is an in-place memcpy over the region's x section -- the iterate-over-same-shape loop (optimizer steps, simulation sweeps) runs at memcpy cost, skipping the region create and the worker-side re-attach. Any other x restages transparently on this run.

`seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None = None`  
Deterministic per-element seeding for this run, as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map); `None` (the default) runs unseeded. The seed rides the run's runner payloads, never the staged region, so reseeding a run is free -- the repeated-simulation loop varies `seed` per run while the stage is paid once.

`timeout: float | None = None`  
Seconds before the run's submission gives up; on expiry the tasks already in are cancelled and the TIMEOUT [`Sentinel`](../reference/Sentinel.md#pymizu.Sentinel) is returned (no handle). The collection deadline is the handle's [collect()](../reference/Task.collect.md#pymizu.Task.collect).

`collect: str | None = None`  
`"copy"` or `"view"` with `template`; `"list"` without -- as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map). The choice is per run.

#### Returns

`The run's map handle -- collected exactly once, by`  
[collect()](../reference/Task.collect.md#pymizu.Task.collect) (blocking) or [collect_async()](../reference/Task.collect_async.md#pymizu.Task.collect_async) (event-driven). A run is a producer on the pool handle: a concurrent run or submit from a second thread raises [ConcurrentUseError](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError). A clean value collect keeps the stage for the O(1) re-arm; a view collect hands the region to the view, and an unclean run (timeout, an error, a worker death) drops it -- the next run restages into a fresh one either way.
