# Pool.trace()

Register a hook called as `fn(event, id)` at each task event.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1656-L1667)

``` python
Pool.trace(fn)
```

The events this process observes are `"submit"` on the submitting thread, and `"start"`, `"done"`, `"error"`, `"drop"`, and `"rehome"` on worker handles. `id` is `"<submitter slot>:<counter>"`, stable across processes. Registration is per-handle and per-process; `None` removes the hook. An exception in the hook is reported as unraisable, never raised into the pool.
