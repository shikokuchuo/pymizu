# Pool.submit_batch()


Submit one task per zero-arg callable in `fns` in one crossing.


Usage

``` python
Pool.submit_batch(
    fns,
    *,
    timeout=None,
)
```


Ring-full past `timeout` ends the batch short -- the returned handles stay valid and collectible. Use functools.partial to bind arguments.
