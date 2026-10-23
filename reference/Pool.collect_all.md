# Pool.collect_all()

Wait until every task is terminal.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L784-L806)

``` python
Pool.collect_all(
    tasks,
    timeout=None,
)
```

## Parameters

`tasks: _Iterable[Task]`  
The [`Task`](../reference/Task.md#pymizu.Task) handles to wait on.

`timeout: float | None = None`  
Seconds to wait; None waits indefinitely.

## Returns

`All values in input order. On the first non-OK outcome by`  
position, raises with an `index` attribute (0-based) -- only the reported handle is consumed; every other, the results ahead of it included, stays collectible. A result that fails to read raises with `index` and a `values` attribute carrying the consumed prefix. The `TIMEOUT` sentinel consumes nothing.
