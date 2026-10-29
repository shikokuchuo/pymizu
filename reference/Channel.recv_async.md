# Channel.recv_async()

The event-driven receive: an `asyncio.Future` for the next

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L295-L325)

``` python
Channel.recv_async()
```

payload, driven by the channel's notify endpoint -- never blocks, never polls.

## Returns

`An asyncio.Future resolving with the identical object a`  
blocking [`recv()`](../reference/Channel.recv.md#pymizu.Channel.recv) would have returned (the recv runs at drain time, on the loop thread), zero-copy views included. Terminal states stay values: the Future resolves with the `CLOSED` / `PEER_GONE` sentinel.

## Details

Several outstanding calls on one channel resolve in FIFO call order, one message each, from the per-handle pending queue. Arming is loss-free: data already in the ring resolves from the arm's synchronous drain; only genuinely pending entries ride the watch. Cancelling the Future detaches the waiter -- the next message goes to the next waiter. There is no `timeout` argument: `asyncio.timeout` / `asyncio.wait_for` are the idiom. A spin channel cannot arm (its publish path skips the wake checkpoint by design) and raises. The async consumer is the side's one consumer: a blocking [`recv()`](../reference/Channel.recv.md#pymizu.Channel.recv) on the same side while waiters are pending raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError).
