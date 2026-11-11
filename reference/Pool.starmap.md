# Pool.starmap()

Map `fn` over `x`, unpacking each element as call arguments.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1286-L1337)

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

Calls `fn(*element, *args, **kwargs)` and returns the run's [`Map`](../reference/Map.md#pymizu.Map) handle, as [`map()`](../reference/Pool.map.md#pymizu.Pool.map) does. This is the `multiprocessing.Pool.starmap` convention. For the multi-iterable shape of `concurrent.futures.Executor.map`, zip first: `pool.starmap(fn, zip(xs, ys))`. Elements must be iterables (a 2-D buffer's rows qualify; a 1-D buffer's scalars do not). A [`call`](../reference/call.md#pymizu.call) spec cannot starmap, because element unpacking is Python-only. Everything else (seeding, templates, streaming, the errors) works as in [`map()`](../reference/Pool.map.md#pymizu.Pool.map).
