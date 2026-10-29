# Pool.attach()

Attach to a live pool as a submitter, by its join token.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L737-L760)

``` python
Pool.attach(token)
```

## Parameters

`token: str`  
The join token; it travels out of band and is `pool.token` on the creator.

## Returns

`The submitter-side pool handle.`  

## Details

Each attached handle is its own producer with its own injection ring: attaching one handle per thread (or per process) is how several submitters drive one pool without serializing.
