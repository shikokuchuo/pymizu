# Pool.submit_batch()

Submit one task per zero-arg callable in `fns` in one

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L730-L762)

``` python
Pool.submit_batch(
    fns,
    *,
    timeout=None,
)
```

crossing.

## Parameters

`fns: _Iterable[_Callable[[], _Any]]`  
Zero-arg callables, one task each. Use `functools.partial` to bind arguments.

`timeout: float | None = None`  
Seconds to wait for injection-ring space; None waits indefinitely.

## Returns

`A list of Task handles. Ring-full past`  
`timeout` ends the batch short -- the returned handles stay valid and collectible. A mid-batch failure raises with an `accepted` attribute holding the handles already submitted, which stay valid and collectible.
