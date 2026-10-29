# Pool.stop()

Orderly shutdown (controller only).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1350-L1375)

``` python
Pool.stop(timeout=5.0)
```

Broadcast shutdown, cancel pending tasks, wait up to `timeout` seconds for clean worker exits, and unlink. Idempotent.

## Returns

`True on clean worker exits within timeout.`  

## Raises

`ConcurrentUseError`  
A blocking collect or a submit is in flight on this handle from a second thread: each half is confined to one thread (the class docstring).
