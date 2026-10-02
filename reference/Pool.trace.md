# Pool.trace()


Register a hook called as `fn(event, id)` at each task


Usage

``` python
Pool.trace(fn)
```


lifecycle event this process observes: `"submit"` on the submitting thread; `"start"`, `"done"`, `"error"`, `"drop"`, `"rehome"` on worker handles. `id` is `"<submitter slot>:<counter>"`, stable across processes. Registration is per-handle and per-process; `None` removes the hook. A hook exception is written as unraisable, never propagated into the pool.
