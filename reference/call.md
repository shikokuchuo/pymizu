# call

A task specification for a pool of another language's workers.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L609-L688)

``` python
call(
    name=None,
    /,
    *args,
    source=None,
    **kwargs,
)
```

Describes a call for [`Pool.submit()`](../reference/pymizu.Pool.submit.md): a qualified name (`"mod.fn"` for Python workers, `"pkg::fn"` for R workers) or a `source=` string in the workers' language, plus the constant arguments. The spec describes a call, not a value. The language is not named at the call site, because [Pool.submit](../reference/pymizu.Pool.submit.md) reads the workers' language from the pool itself, and a bare unqualified name errors at submit, not here.

## Parameters

`name: str | None = None`  
A qualified name: `"mod.fn"` for Python workers, `"pkg::fn"` for R workers.

`args: _Any = ()`  
Constant positional arguments of the call.

`source: str | None = None`  
A source string in the workers' language. Exactly one of `name` or `source` must be given.

`kwargs: _Any = {}`  
Constant keyword arguments of the call.

## Details

Unnamed arguments map to the positional list and keyword arguments to the named dict, matching R's calling convention. Arguments must be portable values (the portable subset documented under [`Channel.send()`](../reference/pymizu.Channel.send.md)). A non-portable argument raises [`DeclinedError`](../reference/DeclinedError.md#pymizu.DeclinedError) at submit.

A `source=` task evaluates in a fresh namespace with the keyword arguments bound as names and positional arguments bound as `_1`, `_2`, … The result is the trailing expression's value, or None when the source ends with a statement.

Large arguments cross to foreign workers by reference rather than by copy. A fresh buffer argument past the zero-copy floor is written to shared memory once, and the worker reads a view over it. An argument that is already a shared view crosses as its name alone, so no bytes move. On a pool whose workers predate this mechanism, such a task fails at submit with an error naming the remedy.

## Examples

A spec task also runs on a normal (Python-worker) pool:

``` python
>>> import pymizu
>>> with pymizu.Pool.create(2) as pool:
...     task = pool.submit(pymizu.call("math.sqrt", 2.0))
...     results = pool.collect_all([task], timeout=10)
>>> results
[1.4142135623730951]
```
