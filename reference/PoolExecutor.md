# PoolExecutor


A `concurrent.futures.Executor` over a pymizu pool.


Usage

``` python
PoolExecutor(
    pool,
    *,
    stop_pool=False,
)
```


`submit()` returns a real `concurrent.futures.Future`: `wait` / `as_completed` / `result(timeout)` / `asyncio.wrap_future` and the stdlib base `Executor.map` all work unchanged, so code written against `ProcessPoolExecutor` drops in. `create()` spawns an owned pool instead.


## Parameters


`pool: pymizu.Pool`  
The pool to wrap; its lifetime stays with its owner unless `stop_pool` is set -- then `shutdown()` stops it.

`stop_pool: bool = ``False`  
When True, `shutdown()` stops the wrapped pool.


## Notes

Deltas from stdlib semantics: a task's failure surfaces as [pymizu.TaskError](TaskError.md#pymizu.TaskError) (the remote error envelope); `Future.running()` is always False (futures resolve at completion); cancellation is advisory, mirroring <a href="../reference/Task.html#pymizu.Task" class="gdls-link"><code>Task.cancel()</code></a> -- and `shutdown(cancel_futures=True)` cancels every outstanding future, since the pool exposes no queued-versus-running signal (a running task completes, but its result is discarded). [map](Pool.map.md#pymizu.Pool.map) is the stdlib base implementation over `submit` -- for bulk maps, [Pool.map](Pool.map.md#pymizu.Pool.map) is the faster path.


## Examples

Use as a drop-in `concurrent.futures.Executor`:


``` python
import pymizu

with pymizu.PoolExecutor.create(2) as ex:
    futures = [ex.submit(pow, 2, i) for i in range(4)]
    results = [f.result() for f in futures]
results
```


    [1, 2, 4, 8]


## Methods

| Name | Description |
|----|----|
| [create()](#create) | Spawn an owned pool -- |
| [shutdown()](#shutdown) | Stdlib shutdown. |
| [submit()](#submit) | Schedule `fn(*args, **kwargs)` on the pool. |

------------------------------------------------------------------------


### create()


Spawn an owned pool --


Usage

``` python
create(
    workers=1,
    **pool_kwargs,
)
```


<a href="../reference/Pool.create.html#pymizu.Pool.create" class="gdls-link"><code>Pool.create()</code></a> with `workers` and `pool_kwargs` -- and wrap it; `shutdown()` stops it.


#### Parameters


`workers: int = ``1`  
Number of worker processes for the pool.

`pool_kwargs: _Any = {}`  
Forwarded to <a href="../reference/Pool.create.html#pymizu.Pool.create" class="gdls-link"><code>Pool.create()</code></a>.


#### Returns


`A PoolExecutor wrapping the spawned pool.`  


------------------------------------------------------------------------


### shutdown()


Stdlib shutdown.


Usage

``` python
shutdown(
    wait=True,
    *,
    cancel_futures=False,
)
```


#### Parameters


`wait: bool = ``True`  
Wait for the outstanding futures.

`cancel_futures: bool = ``False`  
Cancel the not-yet-started futures. Stops the pool only when `stop_pool` was set.


------------------------------------------------------------------------


### submit()


Schedule `fn(*args, **kwargs)` on the pool.


Usage

``` python
submit(
    fn,
    /,
    *args,
    **kwargs,
)
```


#### Parameters


`fn: _Callable[…, _Any]`  
The callable to run.

`args: _Any = ()`  
Positional arguments for `fn`.

`kwargs: _Any = {}`  
Keyword arguments for `fn`.


#### Returns


`A ``concurrent.futures.Future`` resolving with the result.`  


#### Raises


`RuntimeError`  
After shutdown.
