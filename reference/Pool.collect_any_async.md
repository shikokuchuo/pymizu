# Pool.collect_any_async()

The event-driven [`collect_any()`](../reference/Pool.collect_any.md#pymizu.Pool.collect_any):

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L948-L970)

``` python
Pool.collect_any_async(tasks)
```

exactly one `asyncio.Future` per collection -- never one per task -- resolved when a task finishes.

## Parameters

`tasks: _Iterable[Task]`  
The [`Task`](../reference/Task.md#pymizu.Task) handles to wait on.

## Returns

`An asyncio.Future resolving with the first finished`  
task's `(index, value)` (the sync verb's tie-breaks); a failure rejects with the exception the sync verb raises (`index` attribute carried). There is no `timeout` argument: use `asyncio.timeout` or `asyncio.wait_for`. Cancelling the Future detaches every task -- each is blocking-collectible again -- and never cancels them.
