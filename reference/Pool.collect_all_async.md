# Pool.collect_all_async()

The event-driven [`collect_all()`](../reference/Pool.collect_all.md#pymizu.Pool.collect_all).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1221-L1240)

``` python
Pool.collect_all_async(tasks)
```

One `asyncio.Future` for the whole collection, resolved when every task is terminal.

## Parameters

`tasks: _Iterable[Task]`  
The [`Task`](../reference/Task.md#pymizu.Task) handles to wait on.

## Returns

`An asyncio.Future resolving with all values in input order. The`  
first failure by position rejects with the exception the sync verb raises, `index` attribute included; only that handle is consumed, and the rest stay collectible. Cancelling the Future detaches every task and never cancels them.
