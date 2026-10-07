# r_pool_launcher()


Return a <a href="../reference/Pool.create.html#pymizu.Pool.create" class="gdls-link"><code>Pool.create()</code></a> launcher


Usage

``` python
r_pool_launcher(
    *,
    rscript=None,
    stdout=None,
    stderr=None,
)
```


spawning R workers.

Each worker runs the R package `mizu`: the returned `callable(token, slot)` spawns `rscript` on the package's static child runner with `mizu:::worker_main(<token>, <slot>)` as the entry expression and the probed library paths propagated in argv (without them the workers cannot `library(mizu)` from the host's libraries). The mirror of the R package's `mizu_py_pool_launcher()`.


## Parameters


`rscript: str | None = None`  
Path to Rscript; the default searches the PATH.

`stdout: _Any = None`  
Forwarded to `subprocess.Popen`.

`stderr: _Any = None`  
Forwarded to `subprocess.Popen`.


## Returns


`A ``callable(token, slot)`` launcher for`  
<a href="../reference/Pool.create.html#pymizu.Pool.create" class="gdls-link"><code>Pool.create()</code></a>.


## Raises


`MizuError`  
Raised here, before any pool exists, when Rscript is not found or no installed `mizu` with source string support is available.


## Notes

The first worker's join records the workers' language in the pool, so the launcher carries no language attribute: a pool of R workers takes <a href="../reference/call.html#pymizu.call" class="gdls-link"><code>pymizu.call()</code></a> specifications through <a href="../reference/Pool.submit.html#pymizu.Pool.submit" class="gdls-link"><code>Pool.submit()</code></a>, and a plain callable errors locally naming the spec verb. A launcher that spawns the wrong language fails at join, not at the first task.


## Examples

``` python
import pymizu

with pymizu.Pool.create(4, launcher=pymizu.r_pool_launcher()) as pool:
    spec = pymizu.call(source="summary(cars$speed)")
    print(pool.submit(spec).collect())
```
