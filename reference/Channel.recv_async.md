# Channel.recv_async()

Receive the next value as an `asyncio.Future`.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L382-L414)

``` python
Channel.recv_async()
```

The event-driven receive: the Future resolves when the value arrives, without blocking or polling.

## Returns

`An asyncio.Future resolving with the same object a blocking`  
[`recv()`](../reference/Channel.recv.md#pymizu.Channel.recv) would have returned, zero-copy views included (the receive itself runs on the event-loop thread). Terminal states stay values: the Future resolves with the `CLOSED` / `PEER_GONE` sentinel.

## Details

Several outstanding calls on one channel resolve in call order, one message each. Nothing is lost while arming: values already in the ring resolve immediately, and only values not yet sent wait for the wake-up. Cancelling the Future detaches the waiter, and the next message goes to the next waiter. There is no `timeout` argument: use `asyncio.timeout` or `asyncio.wait_for`. A channel created with `spin=True` cannot arm and raises. The async consumer is the side's one consumer: a blocking [`recv()`](../reference/Channel.recv.md#pymizu.Channel.recv) on the same side while waiters are pending raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError).

A message the peer sent outside the portable interchange subset (a language-private stream) rejects only the waiter that receives it, with [`ForeignPayloadError`](../reference/ForeignPayloadError.md#pymizu.ForeignPayloadError). The message is consumed and the async consumer keeps flowing, so the other outstanding calls are unaffected.
