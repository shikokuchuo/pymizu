# Pool.destroy()

Tear down the pool handle immediately, without telling the

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1379-L1399)

``` python
Pool.destroy()
```

workers or waiting. Workers exit once they see the controller is gone. Safe from any thread -- the one verb that is. A verb still running on another thread keeps the handle alive, and teardown completes when the last one returns; an indefinite wait (submit, collect, collect_any, collect_all, ready_wait) wakes promptly, while stop() waits out its own deadline and a worker's idle run() can delay teardown until it returns. Pending async consumers fail with the closed-handle error. The any-thread license covers this verb only; every other verb stays confined to one thread (the class docstring).
