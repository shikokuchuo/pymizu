# Pool.map_prepare()


Stage a map once for repeated runs; return a map handle.


Usage

``` python
Pool.map_prepare(
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    chunks=None,
    seed=None,
    template=None,
    collect=None,
)
```


Takes the same arguments as [map](Pool.map.md#pymizu.Pool.map) (minus `timeout`, which is per-run). The descriptor pickle, the region create, and the worker-side attach are paid once here; each [map_run](Pool.map_run.md#pymizu.Pool.map_run) re-arms in O(1) and reuses the workers' cached contexts. A run collected with `collect="view"` hands its region to the view, so the next run restages into a fresh one. Close the handle (or use it as a context manager) to unlink the region.
