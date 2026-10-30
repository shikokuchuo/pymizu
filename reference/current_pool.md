# current_pool()

The pool handle of the worker running the task.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1491-L1507)

``` python
current_pool()
```

## Returns

`The worker's own pool handle inside a task; None outside one.`  

## Details

A task uses it for nested submission: a nested submit goes straight onto the worker's own queue (no ring, no wait), and a nested collect runs other work instead of sleeping, so nested fan-outs are cheap and never deadlock the pool. The process-wide default set with [`default_pool()`](../reference/default_pool.md#pymizu.default_pool) never overrides this binding.
