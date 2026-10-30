# PoolExecutor

A `concurrent.futures.Executor` over a pymizu pool.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L49-L338)

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
The pool to wrap; its lifetime stays with its owner unless `stop_pool` is set -- then [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops it.

`stop_pool: bool = False`  
When True, [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops the wrapped pool.

## Notes

`submit()` (and the stdlib [map](../reference/Pool.map.md#pymizu.Pool.map) over it) is thread-safe, as the `concurrent.futures` contract requires: an internal lock serializes the single-producer pool handle. This is the adapter to reach for when several threads must submit to one pool. Differences from the stdlib: a task's failure surfaces as [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError); `Future.running()` is always False (futures resolve at completion); cancellation works like [`Task.cancel()`](../reference/Task.md#pymizu.Task) -- it never interrupts a running task -- and `shutdown(cancel_futures=True)` cancels every outstanding future, since the pool exposes no queued-versus-running signal (a running task completes, but its result is discarded). An internal failure (for example the wrapped pool is stopped from under the executor) fails every outstanding future; later submissions raise instead of queueing onto a broken executor. Futures resolve -- and `add_done_callback` callbacks run -- on pymizu's listener thread, the same discipline as the stdlib executors' management thread: keep callbacks short, and never block one on the pool itself. [map](../reference/Pool.map.md#pymizu.Pool.map) is the stdlib base implementation over `submit` -- for bulk maps, [`Pool.map`](../reference/Pool.map.md#pymizu.Pool.map) is the faster path.

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

| Name                    | Description                                 |
|-------------------------|---------------------------------------------|
| [create()](#create)     | Spawn an owned pool --                       |
| [shutdown()](#shutdown) | Shut down the executor.                     |
| [submit()](#submit)     | Schedule `fn(*args, **kwargs)` on the pool. |

### create()

Spawn an owned pool --

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L128-L149)

``` python
create(
    n_workers=1,
    **pool_kwargs,
)
```

[`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create) with `n_workers` and `pool_kwargs` -- and wrap it; [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops it.

#### Parameters

`n_workers: int = 1`  
Number of worker processes for the pool.

`pool_kwargs: _Any = {}`  
Forwarded to [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create).

#### Returns

`A PoolExecutor wrapping the spawned pool.`  

### shutdown()

Shut down the executor.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L205-L233)

``` python
shutdown(
    wait=True,
    *,
    cancel_futures=False,
)
```

#### Parameters

`wait: bool = True`  
Wait for the outstanding futures.

`cancel_futures: bool = False`  
Cancel the not-yet-started futures. Stops the pool only when `stop_pool` was set.

### submit()

Schedule `fn(*args, **kwargs)` on the pool.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L151-L203)

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

`A concurrent.futures.Future resolving with the result.`  

#### Raises

`RuntimeError`  
After shutdown.

`pymizu.MizuError`  
The internal failure that broke the executor (every outstanding future failed with it).
