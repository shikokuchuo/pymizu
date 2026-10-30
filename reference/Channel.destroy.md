# Channel.destroy()

Tear down the handle immediately, without the close

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L386-L400)

``` python
Channel.destroy()
```

handshake: the peer sees the channel as closed (PEER_GONE if it died hard). Safe from any thread -- the one verb that is. A verb still running on another thread keeps the handle alive, and teardown completes when the last one returns; an indefinite wait (recv, recv_batch, ready_wait, close) wakes promptly for this to happen. Pending recv futures settle with PEER_GONE. The any-thread license covers this verb only; every other verb stays confined to one thread (the class docstring).
