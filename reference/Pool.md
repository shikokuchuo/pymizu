# Pool

A shared-memory work-stealing task pool handle (process-private).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L703-L1521)

``` python
Pool()
```

Create the controller side with [`Pool.create()`](../reference/pymizu.Pool.create.md), which spawns the workers (`python -m pymizu.worker`). Other processes join as submitters with [`Pool.attach()`](../reference/pymizu.Pool.attach.md). The pool exists only as long as the process that created it: dropping the handle shuts the pool down as [`Pool.stop()`](../reference/pymizu.Pool.stop.md) does, but without waiting for the workers. Handles do not survive `fork()`.

Task callables travel by pickle. With the standard pickle a submitted callable must be an importable reference (as with `multiprocessing`), and installing cloudpickle lifts that restriction.

Threading: a handle has two single-threaded halves. The producing half ([`submit()`](../reference/pymizu.Pool.submit.md), [`submit_batch()`](../reference/pymizu.Pool.submit_batch.md), [`map()`](../reference/pymizu.Pool.map.md)) belongs to one thread, and a concurrent second producer raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError). The waiting half (the blocking collects and [`stop()`](../reference/pymizu.Pool.stop.md)) likewise belongs to one thread, which may be a different one from the producer's. To produce from several threads, [`attach()`](../reference/pymizu.Pool.attach.md) one handle per thread (each is its own producer, with its own submission queue), or use [`PoolExecutor`](../reference/PoolExecutor.md#pymizu.PoolExecutor), whose submits are thread-safe. Teardown is the exception: [`destroy()`](../reference/pymizu.Pool.destroy.md) is safe from any thread.

## Attributes

| Name                          | Description                                 |
|-------------------------------|---------------------------------------------|
| [token](pymizu.Pool.token.md) | The join token for worker/submitter attach. |

## Functions

| Name | Description |
|----|----|
| [attach()](pymizu.Pool.attach.md) | Attach to a live pool as a submitter, by its join token. |
| [collect_all()](pymizu.Pool.collect_all.md) | Wait until every task is terminal. |
| [collect_all_async()](pymizu.Pool.collect_all_async.md) | The event-driven [`collect_all()`](../reference/pymizu.Pool.collect_all.md). |
| [collect_any()](pymizu.Pool.collect_any.md) | Wait on several tasks. |
| [collect_any_async()](pymizu.Pool.collect_any_async.md) | The event-driven [`collect_any()`](../reference/pymizu.Pool.collect_any.md). |
| [create()](pymizu.Pool.create.md) | Create a pool and spawn its worker processes. |
| [destroy()](pymizu.Pool.destroy.md) | Tear down the pool handle immediately, without waiting. |
| [dump()](pymizu.Pool.dump.md) | A read-only debugging snapshot of the whole pool region (dict). |
| [info()](pymizu.Pool.info.md) | A read-only snapshot of the pool's state (dict). |
| [map()](pymizu.Pool.map.md) | Map `fn` over `x` on the pool; return a map handle. |
| [map_prepare()](pymizu.Pool.map_prepare.md) | Stage a map once for repeated runs; return a [PreparedMap](../reference/PreparedMap.md#pymizu.PreparedMap). |
| [retire()](pymizu.Pool.retire.md) | Ask the worker in `slot` to exit cleanly. |
| [spawn_workers()](pymizu.Pool.spawn_workers.md) | Spawn `n` additional workers and wait for them to join. |
| [starmap()](pymizu.Pool.starmap.md) | Map `fn` over `x`, unpacking each element as call arguments. |
| [stats()](pymizu.Pool.stats.md) | Cumulative per-worker and per-submitter counters. |
| [stop()](pymizu.Pool.stop.md) | Orderly shutdown (controller only). |
| [submit()](pymizu.Pool.submit.md) | Submit `fn(*args, **kwargs)` as a task; return a Task handle. |
| [submit_batch()](pymizu.Pool.submit_batch.md) | Submit one task per zero-arg callable in `fns` in one call. |
| [trace()](pymizu.Pool.trace.md) | Register a hook called as `fn(event, id)` at each task event. |
