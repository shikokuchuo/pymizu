# current_pool()

The pool handle of the worker running the task.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1734-L1749)

``` python
current_pool()
```

## Returns

`The worker's own pool handle inside a task; None outside one.`  

## Details

A task uses it for nested submission: a nested submit goes onto the worker's own queue, with no ring and no wait, and a nested collect runs other work instead of sleeping, so nested fan-outs are cheap and cannot deadlock the pool. The process-wide default set with [`default_pool()`](../reference/default_pool.md#pymizu.default_pool) never overrides this binding.
