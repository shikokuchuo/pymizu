# Pool.collect_any_async()

The event-driven [`collect_any()`](../reference/Pool.collect_any.md#pymizu.Pool.collect_any).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1199-L1220)

``` python
Pool.collect_any_async(tasks)
```

One `asyncio.Future` for the whole collection, resolved when a task finishes.

## Parameters

`tasks: _Iterable[Task]`  
The [`Task`](../reference/Task.md#pymizu.Task) handles to wait on.

## Returns

`An asyncio.Future resolving with the first finished task's`  
`(index, value)` (with the sync verb's tie-breaks). A failure rejects with the exception the sync verb raises, `index` attribute included. There is no `timeout` argument: use `asyncio.timeout` or `asyncio.wait_for`. Cancelling the Future detaches every task, so each can be collected blocking again, and never cancels them.
