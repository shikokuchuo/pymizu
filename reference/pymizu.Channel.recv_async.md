# recv_async()

Return an `asyncio.Future` that resolves with the next value.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L388-L415)

``` python
recv_async()
```

## Returns

`An asyncio.Future resolving with the same object a blocking`  
[`recv()`](../reference/pymizu.Channel.recv.md) would return, including zero-copy views. The receive runs on the event-loop thread. Terminal states stay values: the Future resolves with the `CLOSED` / `PEER_GONE` sentinel.

## Details

Outstanding calls on one channel resolve in call order, one message each. Values already in the ring resolve immediately. Values not yet sent wait for the wake-up. Cancelling the Future detaches the waiter, and the next message goes to the next waiter. There is no `timeout` argument. Use `asyncio.timeout` or `asyncio.wait_for` instead. A channel created with `spin=True` raises instead of arming. While waiters are pending, a blocking [`recv()`](../reference/pymizu.Channel.recv.md) on the same side raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError).

A language-private message rejects only the waiter that receives it, with [`ForeignPayloadError`](../reference/ForeignPayloadError.md#pymizu.ForeignPayloadError). The message is consumed. Later waiters are unaffected.
