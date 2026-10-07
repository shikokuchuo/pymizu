# Pool.starmap()


Map `fn` over `x`, unpacking each element as the call's


Usage

``` python
Pool.starmap(
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    n_chunks=None,
    seed=None,
    timeout=None,
    template=None,
    collect=None,
    stream=False,
)
```


positional arguments: `fn(*element, *args, **kwargs)`.

The `multiprocessing.Pool.starmap` convention; for the multi-iterable shape of `concurrent.futures.Executor.map`, zip first: `pool.starmap(fn, zip(xs, ys))`. Elements must be iterables (a 2-D buffer's rows qualify; a 1-D buffer's scalars do not). A <a href="../reference/call.html#pymizu.call" class="gdls-link"><code>call</code></a> spec cannot starmap: element unpacking is Python-only. Everything else -- seeding, templates, streaming, the error taxonomy -- is exactly <a href="../reference/Pool.map.html#pymizu.Pool.map" class="gdls-link"><code>map()</code></a>'s.
