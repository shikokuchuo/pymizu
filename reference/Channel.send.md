# Channel.send()

Send one value.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L234-L255)

``` python
Channel.send(x)
```

## Parameters

`x: _Any`  
The value to send. `None` is a valid value; bytes and numpy arrays cross without serialization, everything else is pickled (protocol 4).

## Returns

`None, or the FULL / CLOSED / PEER_GONE sentinel`  
(identity-tested).

## Raises

`ConcurrentUseError`  
Another verb is in flight on this side from a second thread: a side is confined to one thread at a time.
