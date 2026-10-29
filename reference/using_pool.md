# using_pool()

Use [pool](../reference/Task.pool.md#pymizu.Task.pool) as the default for the with block, then restore.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1601-L1620)

``` python
using_pool(pool)
```

## Parameters

`pool: Pool | None`  
The pool to make the default; `None` scopes a cleared default.

## Returns

`A context manager yielding pool. The previous default`  
returns on exit, including on exception.
