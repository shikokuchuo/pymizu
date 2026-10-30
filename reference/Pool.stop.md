# Pool.stop()

Orderly shutdown (controller only).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1352-L1377)

``` python
Pool.stop(timeout=5.0)
```

Tell the workers to exit, cancel pending tasks, wait up to `timeout` seconds for clean exits, and release the shared memory. Safe to call more than once.

## Returns

`True on clean worker exits within timeout.`  

## Raises

`ConcurrentUseError`  
A blocking collect or a submit is in flight on this handle from a second thread: each half is confined to one thread (the class docstring).
