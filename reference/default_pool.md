# default_pool()

The process-wide default pool, or None when none is set.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1653-L1675)

``` python
default_pool()
```

Set with [`set_default_pool()`](../reference/set_default_pool.md#pymizu.set_default_pool); scoped use with [`using_pool()`](../reference/using_pool.md#pymizu.using_pool). Package code taking an optional pool resolves it in this order: an explicit `pool` argument, then [`current_pool()`](../reference/current_pool.md#pymizu.current_pool) inside a task (the evaluating worker's own pool, for nested submission), then [default_pool()](../reference/default_pool.md#pymizu.default_pool), then the caller's own fallback, such as sequential execution or an error.

Setting a default checks only the type: a stopped pool is accepted and fails at use time with the usual stopped-pool errors. Handles from [`Pool.attach()`](../reference/pymizu.Pool.attach.md) are valid defaults; ownership and teardown stay with the pool's creator.

The default is process-global: every thread sees the same pool, and [`current_pool()`](../reference/current_pool.md#pymizu.current_pool) remains the thread-local mechanism. After a `fork()`, a child process reads it as unset, because handles are process-private.
