# submit_batch()

Submit one task per zero-arg callable in `fns` in one call.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L945-L979)

``` python
submit_batch(
    fns,
    *,
    timeout=None,
)
```

## Parameters

`fns: _Iterable[_Callable[[], _Any]]`  
Zero-arg callables, one task each. Use `functools.partial` to bind arguments.

`timeout: float | None = None`  
Seconds to wait for queue space; None waits indefinitely.

## Returns

`A list of Task handles. If the queue stays full`  
past `timeout` the batch ends short, and the returned handles stay valid and collectible. A mid-batch failure raises with an `accepted` attribute holding the handles already submitted, which stay valid and collectible.

## Raises

`ConcurrentUseError`  
Another submit-family verb is in flight on this handle from a second thread: a pool handle is single-producer (the class docstring).
