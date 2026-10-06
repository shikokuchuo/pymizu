# PreparedMap


A map staged once into a persistent region, run many times.


Usage

``` python
PreparedMap(
    pool,
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


Create with :meth:[pymizu.Pool.map_prepare](Pool.map_prepare.md#pymizu.Pool.map_prepare); run with :meth:[run](PreparedMap.md#pymizu.PreparedMap.run). Re-arming is O(1) -- a generation bump, a cursor reset, a cancel-word clear -- and workers reuse their cached contexts, so a re-run pays neither the descriptor pickle nor the region create nor the worker-side re-attach. A view-collected run hands its region to the view; the next run restages into a fresh one (the mirror of mizu). Close with :meth:`close` (or a with block) to unlink the region.


## Methods

| Name | Description |
|----|----|
| [close()](#close) | Unlink the staged region (idempotent; the GC backstop is the |
| [run()](#run) | Run the prepared map once; return its results (the same shapes |

------------------------------------------------------------------------


### close()


Unlink the staged region (idempotent; the GC backstop is the


Usage

``` python
close()
```


capsule destructor).


------------------------------------------------------------------------


### run()


Run the prepared map once; return its results (the same shapes


Usage

``` python
run(
    x=None,
    timeout=None,
)
```


and outcome taxonomy as [Pool.map](Pool.map.md#pymizu.Pool.map)).

`x` replaces the staged data for this and later runs. When both the staged and the replacement x are raw-buffer eligible with the same dtype and length, the swap is an in-place memcpy over the region's x section -- the iterate-over-same-shape loop (optimizer steps, simulation sweeps) runs at memcpy cost, skipping the region create and the worker-side re-attach. Any other x restages transparently on this run.
