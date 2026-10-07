# PreparedMap


A map staged once into a persistent region, run many times.


Usage

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


Create with <a href="../reference/Pool.map_prepare.html#pymizu.Pool.map_prepare" class="gdls-link"><code>Pool.map_prepare()</code></a>; run with [run()](PreparedMap.md#pymizu.PreparedMap.run). Re-arming is O(1) -- a generation bump, a cursor reset, a cancel-word clear -- and workers reuse their cached contexts, so a re-run pays neither the descriptor pickle nor the region create nor the worker-side re-attach. A view-collected run hands its region to the view; the next run restages into a fresh one (the mirror of mizu). Close with `close()` (or a with block) to unlink the region.


## Parameters


`pool: pymizu.Pool`  
The <a href="../reference/Pool.html#pymizu.Pool" class="gdls-link"><code>Pool</code></a> to run on.

`fn: _Callable[…, _Any]`  
The function to map, or a <a href="../reference/call.html#pymizu.call" class="gdls-link"><code>call</code></a> spec -- as for <a href="../reference/Pool.map.html#pymizu.Pool.map" class="gdls-link"><code>Pool.map()</code></a>.

`x: _Any`  
The data to map over.

`args: _Any = ()`  
Constant positional arguments appended to every call.

`kwargs: dict | None = None`  
Constant keyword arguments for every call.

`n_chunks: int | None = None`  
Overrides the morsel count (the scheduling granularity).

`seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None = None`  
Deterministic per-element seeding -- as for <a href="../reference/Pool.map.html#pymizu.Pool.map" class="gdls-link"><code>Pool.map()</code></a>.

`template: _Any = None`  
An exemplar buffer declaring each result's dtype and length -- as for <a href="../reference/Pool.map.html#pymizu.Pool.map" class="gdls-link"><code>Pool.map()</code></a>.

`collect: str | None = None`  
'copy' or 'view' with `template`; 'list' without.

`stream: bool = ``False`  
Stream slices of `x` instead of staging it whole -- as for <a href="../reference/Pool.map.html#pymizu.Pool.map" class="gdls-link"><code>Pool.map()</code></a>.


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

------------------------------------------------------------------------


### close()


Unlink the staged region (idempotent; the GC backstop is the


Usage

``` python
close()
```


capsule destructor).


------------------------------------------------------------------------


### run()


Run the prepared map once; return its results (the same


Usage

``` python
run(
    x=None,
    timeout=None,
)
```


shapes and outcome taxonomy as <a href="../reference/Pool.map.html#pymizu.Pool.map" class="gdls-link"><code>Pool.map()</code></a>).


#### Parameters


`x: _Any = None`  
Replaces the staged data for this and later runs. When both the staged and the replacement x are raw-buffer eligible with the same dtype and length, the swap is an in-place memcpy over the region's x section -- the iterate-over-same-shape loop (optimizer steps, simulation sweeps) runs at memcpy cost, skipping the region create and the worker-side re-attach. Any other x restages transparently on this run.

`timeout: float | None = None`  
Seconds before the run gives up; on expiry the outstanding work is cancelled and the TIMEOUT <a href="../reference/Sentinel.html#pymizu.Sentinel" class="gdls-link"><code>Sentinel</code></a> is returned, never raised.


#### Returns


`The results -- the same shapes and outcome taxonomy as`  
<a href="../reference/Pool.map.html#pymizu.Pool.map" class="gdls-link"><code>Pool.map()</code></a>.
