# Pool.stop()

Orderly shutdown (controller only).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1137-L1155)

``` python
Pool.stop(timeout=5.0)
```

Broadcast shutdown, cancel pending tasks, wait up to `timeout` seconds for clean worker exits, and unlink. Idempotent.

## Returns

`True on clean worker exits within timeout.`  
