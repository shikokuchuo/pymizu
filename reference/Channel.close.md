# Channel.close()

Close the channel in an orderly way.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L566-L590)

``` python
Channel.close(timeout=5.0)
```

Tells the peer to close and waits up to `timeout` seconds for it to finish. Returns True on a clean close, or False (with a warning) on timeout, in which case the resources are released when the handle is garbage collected.

## Raises

`ConcurrentUseError`  
A verb is in flight on this side from a second thread: the side is confined to one thread (the class docstring).
