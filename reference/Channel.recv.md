# Channel.recv()

Receive one payload.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L251-L264)

``` python
Channel.recv(timeout=None)
```

## Parameters

`timeout: float | None = None`  
Seconds to wait; None waits indefinitely.

## Returns

`The payload, or the TIMEOUT / CLOSED / PEER_GONE`  
sentinel on the non-payload outcomes.
