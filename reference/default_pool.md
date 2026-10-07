# default_pool()


The process-wide default pool, or None when none is set.


Usage

``` python
default_pool()
```


Set with <a href="../reference/set_default_pool.html#pymizu.set_default_pool" class="gdls-link"><code>set_default_pool()</code></a>; scoped use with <a href="../reference/using_pool.html#pymizu.using_pool" class="gdls-link"><code>using_pool()</code></a>. Package code taking an optional pool resolves it in this order: an explicit `pool` argument, then <a href="../reference/current_pool.html#pymizu.current_pool" class="gdls-link"><code>current_pool()</code></a> inside a task (the evaluating worker's own pool, for nested submission), then [default_pool()](default_pool.md#pymizu.default_pool), then the caller's own fallback -- sequential execution or an error.

Setting a default checks the type only: a stopped pool is accepted (liveness is transient; a probe would prove nothing about use time) and fails at use time with the usual stopped-pool errors. Handles from <a href="../reference/Pool.attach.html#pymizu.Pool.attach" class="gdls-link"><code>Pool.attach()</code></a> are valid defaults; ownership and teardown stay with the pool's creator.

The default is process-global -- every thread sees the same pool, and <a href="../reference/current_pool.html#pymizu.current_pool" class="gdls-link"><code>current_pool()</code></a> remains the thread-local mechanism. After a `fork()`, a child process reads it as unset: handles are process-private.
