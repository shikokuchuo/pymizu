# Channel.close()

Orderly close: signal the peer and wait up to `timeout`

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L273-L285)

``` python
Channel.close(timeout=5.0)
```

seconds for it to observe the close. True on a clean handshake; False (with a warning) on timeout, in which case resources release when the handle is garbage collected.
