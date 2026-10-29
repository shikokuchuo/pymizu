# Pool.starmap()

Map `fn` over `x`, unpacking each element as the call's

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1196-L1248)

``` python
Pool.starmap(
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    template=None,
    n_chunks=None,
    seed=None,
    timeout=None,
    stream=False,
    collect=None,
)
```

positional arguments: `fn(*element, *args, **kwargs)`; return the run's map handle, as [`map()`](../reference/Pool.map.md#pymizu.Pool.map).

The `multiprocessing.Pool.starmap` convention; for the multi-iterable shape of `concurrent.futures.Executor.map`, zip first: `pool.starmap(fn, zip(xs, ys))`. Elements must be iterables (a 2-D buffer's rows qualify; a 1-D buffer's scalars do not). A [`call`](../reference/call.md#pymizu.call) spec cannot starmap: element unpacking is Python-only. Everything else -- seeding, templates, streaming, the error taxonomy -- is exactly [`map()`](../reference/Pool.map.md#pymizu.Pool.map)'s.
