# Pool.attach()

Attach to a live pool as a submitter, by its join token.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L616-L632)

``` python
Pool.attach(token)
```

## Parameters

`token: str`  
The join token; it travels out of band and is `pool.token` on the creator.

## Returns

`The submitter-side pool handle.`  
