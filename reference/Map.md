# Map

The handle for one submitted map run.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L1587-L2076)

``` python
Map(
    pool,
    plan,
)
```

Returned by [`Pool.map()`](../reference/pymizu.Pool.map.md) and [`PreparedMap.run()`](../reference/PreparedMap.md#pymizu.PreparedMap.run). Each handle collects exactly once. `collect()` blocks for the results. `collect_async()` and `await` provide the same results without blocking. A streaming map requires `collect()` because its sliding task window advances in the blocking loop.

`cancel()` stops the run. Workers halt within about one batch, and the outstanding tasks are cancelled. An uncollected handle cancels its run at garbage collection.

## Methods

| Name | Description |
|----|----|
| [cancel()](#cancel) | Stop the run: workers halt within about one batch. |
| [collect()](#collect) | Block for the map's results. |
| [collect_async()](#collect_async) | Return an `asyncio.Future` for the results from `collect()`. |

### cancel()

Stop the run: workers halt within about one batch.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2040-L2058)

``` python
cancel()
```

Advisory, as [`Task.cancel()`](../reference/Task.md#pymizu.Task.cancel): the outstanding tasks are cancelled, so a queued task is skipped, and a running batch completes with its result dropped. A pending collect, blocking or async, ends with [`CancelledError`](../reference/CancelledError.md#pymizu.CancelledError). Safe to call more than once. A no-op on a collected handle.

### collect()

Block for the map's results.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L1962-L1987)

``` python
collect(timeout=None)
```

For a non-prepared map, successful collection with `collect="copy"` releases its shared memory immediately. With `collect="view"`, the returned view takes ownership of the shared memory.

#### Parameters

`timeout: float | None = None`  
Seconds to wait for completion; None waits indefinitely. On expiry the outstanding work is cancelled and the `TIMEOUT` [`Sentinel`](../reference/Sentinel.md#pymizu.Sentinel) is returned rather than raised.

#### Returns

`Without template, a list of results in input order. With`  
`template`, the gathered output area as a numpy array (a memoryview without numpy) of shape `(n, m)`, or `(n,)` for `m == 1`. With `collect="view"`, a zero-copy view of that area. An empty map returns the empty result.

### collect_async()

Return an `asyncio.Future` for the results from `collect()`.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L1989-L2011)

``` python
collect_async()
```

Collection does not block or poll. An element error rejects the Future with an indexed [TaskError](../reference/TaskError.md#pymizu.TaskError). Cancelling the Future detaches the waiter and leaves the map running. The handle can then be collected with `collect()`. Only `cancel()` cancels the map. A streaming map raises [MizuError](../reference/MizuError.md#pymizu.MizuError) because its sliding task window advances in the blocking collect loop.
