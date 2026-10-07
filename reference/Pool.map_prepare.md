# Pool.map_prepare()


Stage a map once for repeated runs; return a


Usage

``` python
Pool.map_prepare(
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    n_chunks=None,
    seed=None,
    template=None,
    collect=None,
    stream=False,
)
```


<a href="../reference/PreparedMap.html#pymizu.PreparedMap" class="gdls-link"><code>PreparedMap</code></a>.

Takes the same arguments as <a href="../reference/Pool.map.html#pymizu.Pool.map" class="gdls-link"><code>map()</code></a> (minus `timeout`, which is per-run and moves to [PreparedMap.run](PreparedMap.md#pymizu.PreparedMap.run)). The descriptor pickle, the region create, and the worker-side attach are paid once here; each `pm.run()` re-arms in O(1) and reuses the workers' cached contexts. A run collected with `collect="view"` hands its region to the view, so the next run restages into a fresh one. With `stream=True` the staged `x` stays submitter-side, so a replacement `x` of any shape simply re-slices -- only a length change under `template` restages (the output area is sized for the staged length). Close the handle (or use it as a context manager) to unlink the region.
