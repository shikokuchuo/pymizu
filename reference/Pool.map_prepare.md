# Pool.map_prepare()

Stage a map once for repeated runs; return a

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1254-L1294)

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

[`PreparedMap`](../reference/PreparedMap.md#pymizu.PreparedMap).

Takes the same arguments as [`map()`](../reference/Pool.map.md#pymizu.Pool.map) (minus `seed`, `timeout`, and [collect](../reference/Task.collect.md#pymizu.Task.collect) -- the per-run options, which move to [`PreparedMap.run()`](../reference/PreparedMap.md#pymizu.PreparedMap.run)). The set-up -- pickling the description, creating the shared region, the worker-side attach -- is paid once here; each `pm.run()` re-arms cheaply, reuses the workers' cached set-up, and returns the run's map handle (the collect happens on it). A run collected with `collect="view"` hands its region to the view, so the next run stages a fresh one. With `stream=True` the staged `x` stays on the submitter, so a replacement `x` of any shape simply re-slices -- only a length change under `template` stages fresh shared memory (the output area is sized for the staged length). Close the handle (or use it as a context manager) to release the region.
