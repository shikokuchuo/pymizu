# Channel.recv_async()

The event-driven receive: an `asyncio.Future` for the next

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L294-L323)

``` python
Channel.recv_async()
```

value, resolved when it arrives -- never blocks, never polls.

## Returns

`An asyncio.Future resolving with the same object a`  
blocking [`recv()`](../reference/Channel.recv.md#pymizu.Channel.recv) would have returned -- the receive itself runs on the event-loop thread -- zero-copy views included. Terminal states stay values: the Future resolves with the `CLOSED` / `PEER_GONE` sentinel.

## Details

Several outstanding calls on one channel resolve in call order, one message each. Nothing is lost while arming: values already in the ring resolve immediately, and only values not yet sent wait for the wake-up. Cancelling the Future detaches the waiter -- the next message goes to the next waiter. There is no `timeout` argument: use `asyncio.timeout` or `asyncio.wait_for`. A channel created with `spin=True` cannot arm and raises. The async consumer is the side's one consumer: a blocking [`recv()`](../reference/Channel.recv.md#pymizu.Channel.recv) on the same side while waiters are pending raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError).
