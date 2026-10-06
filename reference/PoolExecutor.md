# PoolExecutor


A :class:`concurrent.futures.Executor` over a pymizu pool.


Usage

``` python
PoolExecutor(
    pool,
    *,
    stop_pool=False,
)
```


`pool` is the pool to wrap; its lifetime stays with its owner unless `stop_pool` is set -- then :meth:`shutdown` stops it. :meth:`create` spawns an owned pool instead. :meth:`submit` returns a real :class:`concurrent.futures.Future`: `wait` / `as_completed` / `result(timeout)` / `asyncio.wrap_future` and the stdlib base `Executor.map` all work unchanged, so code written against `ProcessPoolExecutor` drops in.

Deltas from stdlib semantics: a task's failure surfaces as :class:[pymizu.TaskError](TaskError.md#pymizu.TaskError) (the remote error envelope); `Future.running()` is always False (futures resolve at completion); cancellation is advisory, mirroring :meth:[pymizu.Task.cancel](Task.md#pymizu.Task.cancel) -- and `shutdown(cancel_futures=True)` cancels every outstanding future, since the pool exposes no queued-versus-running signal (a running task completes, but its result is discarded). [map](Pool.map.md#pymizu.Pool.map) is the stdlib base implementation over `submit` -- for bulk maps, :meth:[pymizu.Pool.map](Pool.map.md#pymizu.Pool.map) is the faster path.


## Methods

| Name | Description |
|----|----|
| [create()](#create) | Spawn an owned pool -- `Pool.create(workers, **pool_kwargs)` |
| [shutdown()](#shutdown) | Stdlib shutdown: `wait` waits for the outstanding futures; |
| [submit()](#submit) | Schedule `fn(*args, **kwargs)`; return a Future. |

------------------------------------------------------------------------


### create()


Spawn an owned pool -- `Pool.create(workers, **pool_kwargs)`


Usage

``` python
create(
    workers=1,
    **pool_kwargs,
)
```


-- and wrap it; :meth:`shutdown` stops it.


------------------------------------------------------------------------


### shutdown()


Stdlib shutdown: `wait` waits for the outstanding futures;


Usage

``` python
shutdown(
    wait=True,
    *,
    cancel_futures=False,
)
```


`cancel_futures` cancels the not-yet-started ones. Stops the pool only when `stop_pool` was set.


------------------------------------------------------------------------


### submit()


Schedule `fn(*args, **kwargs)`; return a Future.


Usage

``` python
submit(
    fn,
    /,
    *args,
    **kwargs,
)
```


RuntimeError after shutdown.
