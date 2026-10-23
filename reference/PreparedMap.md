# PreparedMap

A map staged once into a persistent region, run many times.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L1662-L2064)

``` python
PreparedMap(
    pool,
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    n_chunks=None,
    seed=None,
    template=None,
    collect=None,
    stream=False,
)
```

Create with [`Pool.map_prepare()`](../reference/Pool.map_prepare.md#pymizu.Pool.map_prepare); run with [run()](../reference/PreparedMap.md#pymizu.PreparedMap.run). Re-arming is O(1) -- a generation bump, a cursor reset, a cancel-word clear -- and workers reuse their cached contexts, so a re-run pays neither the descriptor pickle nor the region create nor the worker-side re-attach. A view-collected run hands its region to the view; the next run restages into a fresh one (the mirror of mizu). Close with `close()` (or a with block) to unlink the region.

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

`seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None = None`  
Deterministic per-element seeding -- as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map).

`template: _Any = None`  
An exemplar buffer declaring each result's dtype and length -- as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map).

`collect: str | None = None`  
'copy' or 'view' with `template`; 'list' without.

`stream: bool = False`  
Stream slices of `x` instead of staging it whole -- as for [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map).

## Examples

Stage the map once, then re-run it over replacement data at memcpy cost (same dtype and length swap in place):

``` python
import numpy as np
import pymizu

with pymizu.Pool.create(2) as pool:
    with pool.map_prepare(np.square, np.arange(4.0)) as pm:
        first = pm.run(timeout=10)
        second = pm.run(np.array([4.0, -5.0, 6.0, 7.0]), timeout=10)
second
```

    [np.float64(16.0), np.float64(25.0), np.float64(36.0), np.float64(49.0)]

## Methods

| Name | Description |
|----|----|
| [close()](#close) | Unlink the staged region (idempotent; the GC backstop is the |
| [run()](#run) | Run the prepared map once; return its results (the same |

### close()

Unlink the staged region (idempotent; the GC backstop is the

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2048-L2054)

``` python
close()
```

capsule destructor).

### run()

Run the prepared map once; return its results (the same

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L1880-L1990)

``` python
run(
    x=None,
    timeout=None,
)
```

shapes and outcome taxonomy as [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map)).

#### Parameters

`x: _Any = None`  
Replaces the staged data for this and later runs. When both the staged and the replacement x are raw-buffer eligible with the same dtype and length, the swap is an in-place memcpy over the region's x section -- the iterate-over-same-shape loop (optimizer steps, simulation sweeps) runs at memcpy cost, skipping the region create and the worker-side re-attach. Any other x restages transparently on this run.

`timeout: float | None = None`  
Seconds before the run gives up; on expiry the outstanding work is cancelled and the TIMEOUT [`Sentinel`](../reference/Sentinel.md#pymizu.Sentinel) is returned, never raised.

#### Returns

`The results -- the same shapes and outcome taxonomy as`  
[`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map).
