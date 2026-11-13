# PreparedMap

A reusable map staged in shared memory.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2079-L2411)

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

Create with [`Pool.map_prepare()`](../reference/pymizu.Pool.map_prepare.md). [run()](../reference/PreparedMap.md#pymizu.PreparedMap.run) returns a [`Map`](../reference/Map.md#pymizu.Map) handle for one run. Preparation stores the map description and shared data. Later runs reuse the staged data and the workers' cached setup when possible. Close with `close()` or a `with` block to release the shared memory.

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
Stream slices of `x` instead of writing it to shared memory whole, as for [`Pool.map()`](../reference/pymizu.Pool.map.md).

## Examples

Run a prepared map again with replacement data:

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

| Name              | Description                       |
|-------------------|-----------------------------------|
| [close()](#close) | Release the staged shared memory. |
| [run()](#run)     | Run the prepared map once.        |

### close()

Release the staged shared memory.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2392-L2401)

``` python
close()
```

Safe to call more than once; garbage collection releases it otherwise.

### run()

Run the prepared map once.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2268-L2390)

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
Replacement data for this and later runs. If the staged and replacement arrays have the same dtype and length, the run copies the replacement into the existing region. Any other value is staged fresh. With `stream=True`, the staged data stays on the submitter and any replacement is sliced again. A length change with `template` stages fresh shared memory for the output area.

`seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None = None`  
The deterministic per-element seed for this run, as for [`Pool.map()`](../reference/pymizu.Pool.map.md). `None` runs unseeded. The seed travels with the run's tasks, so changing it does not restage the map.

`timeout: float | None = None`  
Seconds to wait for submissions. None waits indefinitely. On expiry, the submitted tasks are cancelled and the run returns the TIMEOUT [`Sentinel`](../reference/Sentinel.md#pymizu.Sentinel) without a handle. The handle's `collect(timeout)` sets the collection deadline.

`collect: str | None = None`  
`"copy"` (the default) or `"view"`, which requires `template`, as for [`Pool.map()`](../reference/pymizu.Pool.map.md). The choice is per run.

#### Returns

`The run's Map handle, or the TIMEOUT`  
[`Sentinel`](../reference/Sentinel.md#pymizu.Sentinel) if submission times out.

#### Raises

`ConcurrentUseError`  
A concurrent run or submit from a second thread.

#### Details

After a successful collection with `collect="copy"`, the next run reuses the staged data. After a view collection, timeout, error, or worker death, the next run stages fresh data.
