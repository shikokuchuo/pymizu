# Channel.send()

Send one payload.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L220-L235)

``` python
Channel.send(x)
```

## Parameters

`x: _Any`  
The payload. `None` itself is a valid payload; bytes and numpy arrays travel raw, everything else rides pickle protocol 4.

## Returns

`None, or the FULL / CLOSED / PEER_GONE sentinel`  
(identity-tested).
