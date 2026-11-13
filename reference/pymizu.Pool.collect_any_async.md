# collect_any_async()

Return an `asyncio.Future` for the first finished task.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1094-L1114)

``` python
collect_any_async(tasks)
```

The async form of [`collect_any()`](../reference/pymizu.Pool.collect_any.md).

## Parameters

`tasks: _Iterable[Task]`  
The [`Task`](../reference/Task.md#pymizu.Task) handles to wait on.

## Returns

`An asyncio.Future resolving with the first finished task's`  
`(index, value)`, with the tie-breaks of [collect_any()](../reference/pymizu.Pool.collect_any.md). A failure rejects with the exception [collect_any()](../reference/pymizu.Pool.collect_any.md) raises, including its `index` attribute. There is no `timeout` argument. Use `asyncio.timeout` or `asyncio.wait_for` instead. Cancelling the Future detaches every task without cancelling it, so each can be collected with a blocking call.
