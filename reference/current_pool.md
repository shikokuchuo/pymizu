# current_pool()

The evaluating worker's own pool handle, inside a task.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1403-L1420)

``` python
current_pool()
```

## Returns

`The worker's own pool handle inside a task; None outside one.`  

## Details

A task uses it for nested submission: a nested submit pushes onto the worker's own work-stealing deque (no ring, no wait), and a nested collect helps -- executes work -- instead of parking, so nested fan-outs run at fork/join cost and never deadlock the pool. The user-set process-wide default is [`default_pool()`](../reference/default_pool.md#pymizu.default_pool), which never overrides this runtime-owned binding.
