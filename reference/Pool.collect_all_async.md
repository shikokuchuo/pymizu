# Pool.collect_all_async()

The event-driven [`collect_all()`](../reference/Pool.collect_all.md#pymizu.Pool.collect_all):

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L975-L995)

``` python
Pool.collect_all_async(tasks)
```

exactly one `asyncio.Future` per collection.

## Parameters

`tasks: _Iterable[Task]`  
The [`Task`](../reference/Task.md#pymizu.Task) handles to wait on.

## Returns

`An asyncio.Future resolving with all values in input`  
order; the first non-OK outcome by position rejects with the exception the sync verb raises (`index` attribute carried -- only that handle is consumed, the rest stay collectible). Cancelling the Future un-enrolls every task and never cancels them.
