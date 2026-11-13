# destroy()

Tear down the handle immediately, without the close handshake.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L558-L571)

``` python
destroy()
```

The peer sees the channel as closed (PEER_GONE if it died hard). This is the one verb that is safe from any thread. A verb still running on another thread keeps the handle alive, and teardown completes when the last one returns; an indefinite wait (recv, recv_batch, ready_wait, close) wakes promptly so that this can happen. Pending recv futures settle with PEER_GONE. Every other verb stays confined to one thread (see the class docstring).
