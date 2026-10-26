# Pool.collect_any_async()

The event-driven [`collect_any()`](../reference/Pool.collect_any.md#pymizu.Pool.collect_any):

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L864-L886)

``` python
Pool.collect_any_async(tasks)
```

exactly one `asyncio.Future` per collection -- never one per task -- driven by the enrolled tasks' notify announces.

## Parameters

`tasks: _Iterable[Task]`  
The [`Task`](../reference/Task.md#pymizu.Task) handles to wait on.

## Returns

`An asyncio.Future resolving with the first terminal task's`  
`(index, value)` (the sync verb's tie-breaks); a non-OK outcome rejects with the exception the sync verb raises (`index` attribute carried). There is no `timeout` argument: `asyncio.timeout` / `asyncio.wait_for` are the idiom. Cancelling the Future un-enrolls every task -- each returns to blocking-collectible -- and never cancels them.
