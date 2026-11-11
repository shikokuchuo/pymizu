# Map

A staged, submitted map; collects exactly once, through this handle.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L1565-L2031)

``` python
Map(
    pool,
    plan,
)
```

Returned by [`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map) and [`PreparedMap.run()`](../reference/PreparedMap.md#pymizu.PreparedMap.run). `collect()` blocks. `collect_async()` or `await` resolves one asyncio Future when the results arrive (not on a streaming map, whose sliding task window advances in the blocking collect loop). `cancel()` stops the run: workers halt within about one batch and the outstanding tasks are cancelled. A dropped uncollected handle cancels at GC, so no run of fn is wasted. One collection per handle, as for tasks.

## Methods

| Name | Description |
|----|----|
| [cancel()](#cancel) | Stop the run: workers halt within about one batch. |
| [collect()](#collect) | Block for the map's results; collect exactly once. |
| [collect_async()](#collect_async) | Resolve one asyncio.Future with the results `collect()` returns. |

### cancel()

Stop the run: workers halt within about one batch.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L2000-L2013)

``` python
cancel()
```

Advisory, as [`Task.cancel()`](../reference/Task.cancel.md#pymizu.Task.cancel): the outstanding tasks are cancelled, so a queued task is skipped, and a running batch completes with its result dropped. A pending collect, blocking or async, ends with [`CancelledError`](../reference/CancelledError.md#pymizu.CancelledError). Safe to call more than once.

### collect()

Block for the map's results; collect exactly once.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L1930-L1948)

``` python
collect(timeout=None)
```

#### Parameters

`timeout: float | None = None`  
Seconds to wait for completion; None waits indefinitely. On expiry the outstanding work is cancelled and the `TIMEOUT` [`Sentinel`](../reference/Sentinel.md#pymizu.Sentinel) is returned rather than raised.

#### Returns

`The results, with the same shapes and outcomes as`  
[`PreparedMap.run()`](../reference/PreparedMap.md#pymizu.PreparedMap) documents.

### collect_async()

Resolve one asyncio.Future with the results `collect()` returns.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_map.py#L1950-L1971)

``` python
collect_async()
```

The event-driven collect, which never blocks or polls. An element error rejects with its index. Cancelling the Future detaches the waiter, so the handle can be collected blocking again, and never cancels the map (only `cancel()` does). A streaming map refuses, because its sliding task window advances in the blocking collect loop.
