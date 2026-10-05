# set_default_pool()


Set the process-wide default pool; None clears it.


Usage

``` python
set_default_pool(pool)
```


Returns the previous default (a Pool or None), so callers can save and restore. The registry anchors the handle: a pool set as the default stays alive even after its variable is deleted, until the default is cleared or replaced. See :func:[default_pool](default_pool.md#pymizu.default_pool) for the full semantics.
