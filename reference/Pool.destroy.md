# Pool.destroy()

Tear down the pool handle immediately, without waiting.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1618-L1638)

``` python
Pool.destroy()
```

No shutdown is signalled to the workers; they exit once they detect that the controller is gone. This is the one verb that is safe from any thread. A verb still running on another thread keeps the handle alive, and teardown completes when the last one returns; an indefinite wait (submit, collect, collect_any, collect_all, ready_wait) wakes promptly, while stop() waits out its own deadline and a worker's idle run() can delay teardown until it returns. Pending async consumers fail with the closed-handle error. Every other verb stays confined to one thread (see the class docstring).
