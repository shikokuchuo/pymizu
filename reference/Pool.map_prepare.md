# Pool.map_prepare()

Stage a map once for repeated runs; return a [PreparedMap](../reference/PreparedMap.md#pymizu.PreparedMap).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1335-L1371)

``` python
Pool.map_prepare(
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

Takes the same arguments as [`map()`](../reference/Pool.map.md#pymizu.Pool.map) except `seed`, `timeout`, and `collect`, which are per-run options of [`PreparedMap.run()`](../reference/PreparedMap.md#pymizu.PreparedMap.run). The set-up (pickling the description, creating the shared region, the worker-side attach) is paid once here. Each `pm.run()` re-arms cheaply, reuses the workers' cached set-up, and returns the run's [`Map`](../reference/Map.md#pymizu.Map) handle, on which the results are collected. A run collected with `collect="view"` hands its region to the view, so the next run stages a fresh one. With `stream=True` the staged `x` stays on the submitter, so a replacement `x` of any shape re-slices. Only a length change under `template` stages fresh shared memory, because the output area is sized for the staged length. Close the handle (or use it as a context manager) to release the region.
