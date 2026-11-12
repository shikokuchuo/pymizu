# Channel.\_\_aiter\_\_()

Iterate over received payloads until CLOSED or PEER_GONE.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L422-L442)

``` python
Channel.__aiter__()
```

The async form of the sync iterator. Yields payloads as they arrive, awaiting each through [`recv_async()`](../reference/Channel.recv_async.md#pymizu.Channel.recv_async). Between iterations nothing is armed; the next iteration re-arms, and the arm's synchronous drain picks up whatever arrived meanwhile, so nothing is lost.

A declined foreign payload (see [`recv_async()`](../reference/Channel.recv_async.md#pymizu.Channel.recv_async)) propagates out of the iteration and ends the `async for` statement, so a try/except inside the loop body cannot catch it. To skip one, wrap the whole loop and re-enter on the same channel: the consumer kept flowing, and the new iterator picks up the next message.
