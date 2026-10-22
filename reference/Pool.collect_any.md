# Pool.collect_any()

Wait on several tasks.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L750-L768)

``` python
Pool.collect_any(
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

`(index, value) of the first terminal task, or the`  
`TIMEOUT` sentinel. A non-OK outcome raises with an `index` attribute (0-based).
