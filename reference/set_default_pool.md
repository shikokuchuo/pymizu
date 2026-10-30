# set_default_pool()

Set the process-wide default pool; None clears it.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1571-L1590)

``` python
set_default_pool(pool)
```

## Returns

`The previous default (a Pool or None), so callers can save and`  
restore.

## Details

Setting a default keeps the pool alive: it survives even after its variable is deleted, until the default is cleared or replaced. See [`default_pool()`](../reference/default_pool.md#pymizu.default_pool) for the full semantics.
