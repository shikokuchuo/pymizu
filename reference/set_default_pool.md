# set_default_pool()


Set the process-wide default pool; None clears it.


Usage

``` python
set_default_pool(pool)
```


## Returns


`The previous default (a Pool or None), so callers can save and`  
restore.


## Details

The registry anchors the handle: a pool set as the default stays alive even after its variable is deleted, until the default is cleared or replaced. See <a href="../reference/default_pool.html#pymizu.default_pool" class="gdls-link"><code>default_pool()</code></a> for the full semantics.
