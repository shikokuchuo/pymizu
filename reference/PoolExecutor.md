# PoolExecutor

A `concurrent.futures.Executor` backed by a pymizu pool.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L76-L521)

``` python
PoolExecutor(
    pool,
    *,
    stop_pool=False,
)
```

`submit()` returns a standard `concurrent.futures.Future`. The stdlib `wait`, `as_completed`, and `asyncio.wrap_future` helpers accept it. `map()` supports `ProcessPoolExecutor`-style chunking. `create()` spawns an owned pool.

## Parameters

`pool: pymizu.Pool`  
The pool to wrap. Its lifetime stays with its owner unless `stop_pool` is set, in which case [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops it.

`stop_pool: bool = False`  
When True, [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops the wrapped pool.

## Notes

`submit()` and the stdlib `map()` are thread-safe. An internal lock serializes the single-producer pool handle. Use this adapter when several threads submit to one pool.

Task failures raise [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError). `Future.running()` always returns False because futures resolve at completion. Cancellation follows [`Task.cancel()`](../reference/Task.md#pymizu.Task) and never interrupts a running task. `shutdown(cancel_futures=True)` cancels every outstanding future because the pool cannot distinguish queued from running tasks. A running task completes, but its result is discarded.

An internal error, such as another caller stopping the wrapped pool, fails every outstanding future. Later submissions raise instead of queueing work.

Futures resolve, and `add_done_callback` callbacks run, on pymizu's listener thread. Keep callbacks short. Never block a callback on the pool itself. If the result table is full, `submit()` waits for the executor to collect a result. A submission from a listener-thread done callback raises [SlotsExhaustedError](../reference/SlotsExhaustedError.md#pymizu.SlotsExhaustedError) because waiting there would deadlock the drain.

`map()` honors `chunksize` and `buffersize` like `ProcessPoolExecutor`. For bulk maps, [`Pool.map`](../reference/pymizu.Pool.map.md) is faster.

## Examples

Use as a `concurrent.futures.Executor`:

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

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L165-L187)

``` python
create(
    n_workers=1,
    **pool_kwargs,
)
```

Calls [`Pool.create()`](../reference/pymizu.Pool.create.md) with `n_workers` and `pool_kwargs`; [shutdown()](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops the pool.

#### Parameters

`n_workers: int = 1`  
Number of worker processes for the pool.

`pool_kwargs: _Any = {}`  
Forwarded to [`Pool.create()`](../reference/pymizu.Pool.create.md).

#### Returns

`A PoolExecutor wrapping the spawned pool.`  

### map()

Return an iterator applying `fn` to every zipped input element.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L189-L282)

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

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L384-L413)

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

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_executor.py#L284-L373)

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
