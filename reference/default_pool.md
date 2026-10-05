# default_pool()


The process-wide default pool, or None when none is set.


Usage

``` python
default_pool()
```


Set with :func:[set_default_pool](set_default_pool.md#pymizu.set_default_pool); scoped use with :func:[using_pool](using_pool.md#pymizu.using_pool). Package code taking an optional pool resolves it in this order: an explicit `pool` argument, then :func:[current_pool](current_pool.md#pymizu.current_pool) inside a task (the evaluating worker's own pool, for nested submission), then [default_pool()](default_pool.md#pymizu.default_pool), then the caller's own fallback -- sequential execution or an error.

Setting a default checks the type only: a stopped pool is accepted (liveness is transient; a probe would prove nothing about use time) and fails at use time with the usual stopped-pool errors. Handles from :meth:[Pool.attach](Pool.attach.md#pymizu.Pool.attach) are valid defaults; ownership and teardown stay with the pool's creator.

The default is process-global -- every thread sees the same pool, and :func:[current_pool](current_pool.md#pymizu.current_pool) remains the thread-local mechanism. After a `fork()`, a child process reads it as unset: handles are process-private.
