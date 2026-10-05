# current_pool()


The evaluating worker's own pool handle, inside a task.


Usage

``` python
current_pool()
```


A task uses it for nested submission: a nested submit pushes onto the worker's own work-stealing deque (no ring, no wait), and a nested collect helps -- executes work -- instead of parking, so nested fan-outs run at fork/join cost and never deadlock the pool. None outside a task; the user-set process-wide default is :func:[default_pool](default_pool.md#pymizu.default_pool), which never overrides this runtime-owned binding.
