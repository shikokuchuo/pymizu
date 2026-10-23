# Pool

A shared-memory work-stealing task pool handle (process-private).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L521-L1219)

``` python
Pool()
```

Create the controller side with [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create) (which spawns the workers, `python -m pymizu.worker`); other processes join as submitters with [`Pool.attach()`](../reference/Pool.attach.md#pymizu.Pool.attach). The pool's lifetime is bound to the creating process: dropping the handle shuts the pool down as [`Pool.stop()`](../reference/Pool.stop.md#pymizu.Pool.stop) does, but without the wait. Handles do not survive `fork()`.

Task callables ride pickle: under stock pickle a submitted callable must be an importable reference (the multiprocessing constraint); installing cloudpickle lifts that transparently.
