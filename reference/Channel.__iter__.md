# Channel.\_\_iter\_\_()

Iterate over received payloads until CLOSED or PEER_GONE.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L373-L384)

``` python
Channel.__iter__()
```

Blocks indefinitely between payloads ([recv()](../reference/Channel.recv.md#pymizu.Channel.recv) with no timeout); use [`Channel.recv()`](../reference/Channel.recv.md#pymizu.Channel.recv) directly when a bound is needed.
