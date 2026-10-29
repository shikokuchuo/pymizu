# Pool

A shared-memory work-stealing task pool handle (process-private).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L610-L1435)

``` python
Pool()
```

Create the controller side with [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create) (which spawns the workers, `python -m pymizu.worker`); other processes join as submitters with [`Pool.attach()`](../reference/Pool.attach.md#pymizu.Pool.attach). The pool's lifetime is bound to the creating process: dropping the handle shuts the pool down as [`Pool.stop()`](../reference/Pool.stop.md#pymizu.Pool.stop) does, but without the wait. Handles do not survive `fork()`.

Task callables ride pickle: under stock pickle a submitted callable must be an importable reference (the multiprocessing constraint); installing cloudpickle lifts that transparently.

Threading: a handle has two single-threaded halves. The producing half -- [`submit()`](../reference/Pool.submit.md#pymizu.Pool.submit), [`submit_batch()`](../reference/Pool.submit_batch.md#pymizu.Pool.submit_batch), [`map()`](../reference/Pool.map.md#pymizu.Pool.map) -- serializes on one thread; a concurrent second producer raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError). The waiting half -- the blocking collects and [`stop()`](../reference/Pool.stop.md#pymizu.Pool.stop) -- likewise serializes on one thread, which may be a different one from the producer's. To produce from several threads, [`attach()`](../reference/Pool.attach.md#pymizu.Pool.attach) one handle per thread (each is its own producer, with its own injection ring), or use [`PoolExecutor`](../reference/PoolExecutor.md#pymizu.PoolExecutor), whose submits are thread-safe. Teardown is the exception: [`destroy()`](../reference/Pool.destroy.md#pymizu.Pool.destroy) is safe from any thread.
