# PoolExecutor

A `concurrent.futures.Executor` over a pymizu pool.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L86-L529)

``` python
PoolExecutor(
    pool,
    *,
    stop_pool=False,
)
```

`submit()` returns a real `concurrent.futures.Future`: `wait` / `as_completed` / `result(timeout)` / `asyncio.wrap_future` and `map` with `ProcessPoolExecutor` chunking all work unchanged, so code written against `ProcessPoolExecutor` drops in. `create()` spawns an owned pool instead.

## Parameters

`pool: pymizu.Pool`  
The pool to wrap. Its lifetime stays with its owner unless `stop_pool` is set, in which case [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops it.

`stop_pool: bool = False`  
When True, [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops the wrapped pool.

## Notes

`submit()` (and the stdlib `map` over it) is thread-safe, as the `concurrent.futures` contract requires: an internal lock serializes the single-producer pool handle. Use this adapter when several threads must submit to one pool. Differences from the stdlib: a task's failure surfaces as [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError); `Future.running()` is always False (futures resolve at completion); cancellation works like [`Task.cancel()`](../reference/Task.md#pymizu.Task) and never interrupts a running task; and `shutdown(cancel_futures=True)` cancels every outstanding future, since the pool exposes no queued-versus-running signal (a running task completes, but its result is discarded). An internal failure (for example the wrapped pool is stopped from under the executor) fails every outstanding future, and later submissions raise instead of queueing onto a broken executor. Futures resolve, and `add_done_callback` callbacks run, on pymizu's listener thread, the same discipline as the stdlib executors' management thread: keep callbacks short, and never block one on the pool itself. If the result table is full, `submit()` waits for the executor to collect a result; a submission from a listener-thread done callback instead raises [SlotsExhaustedError](../reference/SlotsExhaustedError.md#pymizu.SlotsExhaustedError), since waiting there would deadlock the drain. `map()` honors `chunksize` and `buffersize` like `ProcessPoolExecutor`; for bulk maps, [`Pool.map`](../reference/Pool.map.md#pymizu.Pool.map) remains the faster path.

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
| [create()](#create) | Spawn an owned pool and wrap it. |
| [map()](#map) | Return an iterator applying `fn` to every zipped input element. |
| [shutdown()](#shutdown) | Shut down the executor. |
| [submit()](#submit) | Schedule `fn(*args, **kwargs)` on the pool. |

### create()

Spawn an owned pool and wrap it.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L173-L195)

``` python
create(
    n_workers=1,
    **pool_kwargs,
)
```

Calls [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create) with `n_workers` and `pool_kwargs`; [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops the pool.

#### Parameters

`n_workers: int = 1`  
Number of worker processes for the pool.

`pool_kwargs: _Any = {}`  
Forwarded to [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create).

#### Returns

`A PoolExecutor wrapping the spawned pool.`  

### map()

Return an iterator applying `fn` to every zipped input element.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L197-L290)

``` python
map(
    fn,
    *iterables,
    timeout=None,
    chunksize=1,
    buffersize=None,
)
```

#### Parameters

`fn: _Callable[…, _Any]`  
The callable to run.

`iterables: _Any = ()`  
One or more iterables, zipped lazily as for the built-in `map`.

`timeout: float | None = None`  
One overall deadline for generating the result iterator, in seconds; None means no limit.

`chunksize: int = 1`  
Elements per submitted task. Values greater than one use chunk tasks; the default submits one task per element.

`buffersize: int | None = None`  
Maximum submitted tasks whose results have not been yielded. None collects all inputs eagerly; a positive value bounds pending tasks and consumes the input lazily.

#### Returns

`An iterator of results in input order. An error in a chunk`  
discards that chunk's earlier results, matching `ProcessPoolExecutor`.

### shutdown()

Shut down the executor.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L392-L421)

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

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L292-L381)

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
