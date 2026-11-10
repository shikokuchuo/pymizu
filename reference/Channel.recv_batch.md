# Channel.recv_batch()

Receive up to `n` values in one call.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L488-L495)

``` python
Channel.recv_batch(
    n=256,
    timeout=None,
)
```

Returns a list (possibly short or empty), or a terminal sentinel.
