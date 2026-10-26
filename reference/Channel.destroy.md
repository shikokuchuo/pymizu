# Channel.destroy()

Tear down the handle immediately, without the close

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L342-L356)

``` python
Channel.destroy()
```

handshake: the peer observes the channel closed (PEER_GONE is the hard-death outcome). Safe from any thread: a verb in flight on another thread pins the handle, and teardown completes when the last one returns -- an indefinite wait (recv, recv_batch, ready_wait, close) breaks out within a bounded slice, so the teardown (and the peer's observation of it) follows promptly. Pending recv futures settle with PEER_GONE.
