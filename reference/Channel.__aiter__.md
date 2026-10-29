# Channel.\_\_aiter\_\_()

Iterate over received payloads until CLOSED or PEER_GONE --

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L327-L341)

``` python
Channel.__aiter__()
```

the async form of the sync iterator.

Yields payloads as they arrive, awaiting each through [`recv_async()`](../reference/Channel.recv_async.md#pymizu.Channel.recv_async). Between iterations nothing is armed; the next iteration re-arms and the arm's synchronous drain picks up whatever arrived meanwhile (loss-free).
