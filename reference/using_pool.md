# using_pool()


Use `pool` as the default for the with block, then restore.


Usage

``` python
using_pool(pool)
```


## Parameters


`pool: Pool | None`  
The pool to make the default; `None` scopes a cleared default.


## Returns


`A context manager yielding ``pool``. The previous default`  
returns on exit, including on exception.
