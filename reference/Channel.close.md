# Channel.close()

Orderly close: tell the peer to close and wait up to

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L348-L372)

``` python
Channel.close(timeout=5.0)
```

`timeout` seconds for it to finish. True on a clean close; False (with a warning) on timeout -- resources are then released when the handle is garbage collected.

## Raises

`ConcurrentUseError`  
A verb is in flight on this side from a second thread: the side is confined to one thread (the class docstring).
