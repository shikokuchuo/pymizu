# set_default_pool()

Set the process-wide default pool; None clears it.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1362-L1382)

``` python
set_default_pool(pool)
```

## Returns

`The previous default (a Pool or None), so callers can save and`  
restore.

## Details

The registry anchors the handle: a pool set as the default stays alive even after its variable is deleted, until the default is cleared or replaced. See [`default_pool()`](../reference/default_pool.md#pymizu.default_pool) for the full semantics.
