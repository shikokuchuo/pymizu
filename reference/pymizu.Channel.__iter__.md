# \_\_iter\_\_()

Iterate over received payloads until CLOSED or PEER_GONE.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L588-L599)

``` python
__iter__()
```

Blocks indefinitely between payloads ([recv()](../reference/pymizu.Channel.recv.md) with no timeout); use [`Channel.recv()`](../reference/pymizu.Channel.recv.md) directly when a bound is needed.
