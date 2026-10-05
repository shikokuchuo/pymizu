# using_pool()


Use `pool` as the default for the with block, then restore.


Usage

``` python
using_pool(pool)
```


Yields `pool`. The previous default returns on exit, including on exception; `None` scopes a cleared default.
