# map_prepare()

Stage a map for repeated runs and return a [PreparedMap](../reference/PreparedMap.md#pymizu.PreparedMap).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1379-L1407)

``` python
map_prepare(
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    template=None,
    n_chunks=None,
    stream=False,
)
```

This takes the same arguments as [`map()`](../reference/pymizu.Pool.map.md) except `seed`, `timeout`, and `collect`. Those are per-run options of [`PreparedMap.run()`](../reference/PreparedMap.md#pymizu.PreparedMap.run). See [`PreparedMap`](../reference/PreparedMap.md#pymizu.PreparedMap) for reuse and shared-memory lifetime.
