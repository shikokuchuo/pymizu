# Pool.trace()

Register a hook called as `fn(event, id)` at each task

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1415-L1424)

``` python
Pool.trace(fn)
```

event this process observes: `"submit"` on the submitting thread; `"start"`, `"done"`, `"error"`, `"drop"`, `"rehome"` on worker handles. `id` is `"<submitter slot>:<counter>"`, stable across processes. Registration is per-handle and per-process; `None` removes the hook. An exception in the hook is reported as unraisable, never raised into the pool.
