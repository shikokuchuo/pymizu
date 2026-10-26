# Pool.destroy()

Tear down the pool handle immediately, without the shutdown

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1291-L1310)

``` python
Pool.destroy()
```

broadcast or the wait. Workers exit once they observe the controller gone. Safe from any thread: a verb in flight on another thread pins the handle, and teardown completes when the last one returns -- an indefinite wait (submit, collect, collect_any, collect_all, ready_wait) breaks out within a bounded slice; stop() instead waits out its own deadline, and a worker's idle run() can delay the teardown (and the shutdown observation) until it returns. Pending async consumers fail with the closed-handle error.
