# Channel.destroy()

Tear down the handle immediately, without the close

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L381-L397)

``` python
Channel.destroy()
```

handshake: the peer observes the channel closed (PEER_GONE is the hard-death outcome). Safe from any thread -- the one verb that is: a verb in flight on another thread pins the handle, and teardown completes when the last one returns -- an indefinite wait (recv, recv_batch, ready_wait, close) breaks out within a bounded slice, so the teardown (and the peer's observation of it) follows promptly. Pending recv futures settle with PEER_GONE. The any-thread license covers this verb only; every other verb stays thread-confined (the class docstring).
