# collect_all_async()

Return an `asyncio.Future` that resolves when every task finishes.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1116-L1134)

``` python
collect_all_async(tasks)
```

The async form of [`collect_all()`](../reference/pymizu.Pool.collect_all.md).

## Parameters

`tasks: _Iterable[Task]`  
The [`Task`](../reference/Task.md#pymizu.Task) handles to wait on.

## Returns

`An asyncio.Future resolving with all values in input order. The`  
first failure by position rejects with the exception [collect_all()](../reference/pymizu.Pool.collect_all.md) raises, including its `index` attribute. Only that handle is consumed, and the rest stay collectible. Cancelling the Future detaches every task without cancelling it.
