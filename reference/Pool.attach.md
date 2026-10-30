# Pool.attach()

Attach to a live pool as a submitter, by its join token.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L737-L758)

``` python
Pool.attach(token)
```

## Parameters

`token: str`  
The join token, `pool.token` on the creator.

## Returns

`The submitter-side pool handle.`  

## Details

Each attached handle is its own producer with its own submission queue: attaching one handle per thread (or per process) is how several submitters drive one pool at once.
