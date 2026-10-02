# call


A task specification for a pool of another language's workers.


Usage

``` python
call(
    name=None,
    /,
    *args,
    source=None,
    **kwargs,
)
```


Describes a call for :meth:[Pool.submit](Pool.submit.md#pymizu.Pool.submit): a qualified name (`"mod.fn"` for Python workers, `"pkg::fn"` for R workers) or a `source=` string in the workers' language, plus the constant arguments. The spec describes a call; it is not a value. The language never appears at the call site -- Pool.submit resolves the workers' language from the pool itself -- and a bare (unqualified) name errors at submit, not here.

Unnamed arguments map to the positional list and keyword arguments to the named dict, matching R's mixed-call convention. Arguments must be portable values (the interchange subset documented under :meth:[Channel.send](Channel.send.md#pymizu.Channel.send)): a non-portable argument raises :class:[DeclinedError](DeclinedError.md#pymizu.DeclinedError) at submit, never a fallback.

A `source=` task evaluates in a fresh namespace with the keyword arguments bound as names and positional arguments bound as `_1`, `_2`, … The result is the trailing expression's value, or None when the source ends with a statement.

Large arguments cross to foreign workers by reference rather than by copy: one fresh buffer argument past the zero-copy floor stages a single layout write into a shared region (the worker reads a view over it), and an argument that is already a shared view crosses as its identifier alone -- zero payload bytes. On a pool whose workers predate the ref reader, such a task declines locally at submit naming the remedy.
