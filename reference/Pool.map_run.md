# Pool.map_run()


Run a map handle from [map_prepare](Pool.map_prepare.md#pymizu.Pool.map_prepare) once; return its results


Usage

``` python
Pool.map_run(
    prepared,
    x=None,
    timeout=None,
)
```


(the same shapes and outcome taxonomy as [map](Pool.map.md#pymizu.Pool.map)).

`x` replaces the staged data for this and later runs: a raw-buffer replacement of the same dtype and length swaps in place (a memcpy over the region, no restage); anything else restages transparently.
