# \_\_aiter\_\_()

Yield received values until `CLOSED` or `PEER_GONE`.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L417-L435)

``` python
__aiter__()
```

Each value is awaited through [`recv_async()`](../reference/pymizu.Channel.recv_async.md). No waiter stays armed between iterations. The next iteration drains any values that arrived in the meantime.

A [`ForeignPayloadError`](../reference/ForeignPayloadError.md#pymizu.ForeignPayloadError) ends the `async for` statement. A try/except inside the loop body cannot catch it. To skip the payload, wrap the whole loop and re-enter on the same channel. The channel remains usable, and the new iterator receives the next message.
