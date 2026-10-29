# Channel.recv()

Receive one payload.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L271-L293)

``` python
Channel.recv(timeout=None)
```

## Parameters

`timeout: float | None = None`  
Seconds to wait; None waits indefinitely.

## Returns

`The payload, or the TIMEOUT / CLOSED / PEER_GONE`  
sentinel on the non-payload outcomes.

## Raises

`ConcurrentUseError`  
Another verb is in flight on this side from a second thread, or a [`recv_async()`](../reference/Channel.recv_async.md#pymizu.Channel.recv_async) consumer is armed and this is not its event-loop thread: a side is confined to one thread and one consumer form at a time.
