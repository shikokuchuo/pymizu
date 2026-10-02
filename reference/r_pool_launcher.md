# r_pool_launcher()


Return a [Pool.create](Pool.create.md#pymizu.Pool.create) launcher spawning R workers.


Usage

``` python
r_pool_launcher(
    *,
    rscript=None,
    stdout=None,
    stderr=None,
)
```


Each worker runs the R package `mizu`: the returned `callable(token, slot)` spawns `rscript` on the package's static child runner with `mizu:::worker_main(<token>, <slot>)` as the entry expression and the probed library paths propagated in argv (without them the workers cannot `library(mizu)` from the host's libraries). Requires Rscript on the PATH (or passed as `rscript`) and an installed `mizu` with source string support -- MizuError is raised here, before any pool exists, otherwise. The mirror of the R package's `mizu_py_pool_launcher()`.

The first worker's join records the workers' language in the pool, so the launcher carries no language attribute: a pool of R workers takes :class:[pymizu.call](call.md#pymizu.call) specifications through [Pool.submit()](Pool.submit.md#pymizu.Pool.submit), and a plain callable errors locally naming the spec verb. A launcher that spawns the wrong language fails at join, not at the first task.
