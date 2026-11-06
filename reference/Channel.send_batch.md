# Channel.send_batch()

Send several values in one call.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L356-L362)

``` python
Channel.send_batch(xs)
```

Returns the number accepted, which is fewer than `len(xs)` when the ring fills or a terminal state is reached.
