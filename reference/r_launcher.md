# r_launcher()

Return a launcher spawning R processes running the `mizu` package.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_r.py#L134-L203)

``` python
r_launcher(
    *,
    rscript=None,
    stdout=None,
    stderr=None,
)
```

[`Channel.create()`](../reference/pymizu.Channel.create.md) calls it with `token` alone to spawn the peer; [`Pool.create()`](../reference/pymizu.Pool.create.md) calls it with `token` and `slot` to spawn each worker. The process runs `rscript` on the package's static child runner with the entry expression (`mizu:::peer_main(<token>)` or `mizu:::worker_main(<token>, <slot>)`) and the detected library paths passed along in argv (without them it cannot `library(mizu)` from the host's libraries). It is the counterpart of the R package's `mizu_py_launcher()`.

## Parameters

`rscript: str | None = None`  
Path to Rscript; the default searches the PATH.

`stdout: _Any = None`  
Forwarded to `subprocess.Popen`; the default inherits the console, where the peer's error messages land.

`stderr: _Any = None`  
Forwarded to `subprocess.Popen`.

## Returns

`A callable(token, slot=None) launcher for`  
[`Channel.create()`](../reference/pymizu.Channel.create.md) and [`Pool.create()`](../reference/pymizu.Pool.create.md).

## Raises

`MizuError`  
Raised here, before any channel or pool is created, when Rscript is not found or no installed `mizu` with source string support is available.

## Notes

The first worker to join records the workers' language in the pool, so the launcher carries no language attribute. A pool of R workers takes [`pymizu.call()`](../reference/call.md#pymizu.call) specifications through [`Pool.submit()`](../reference/pymizu.Pool.submit.md), and a plain callable raises an error naming [call](../reference/call.md#pymizu.call). A launcher that spawns the wrong language fails at join, not at the first task.

## Examples

``` python
import pymizu

with pymizu.Pool.create(4, launcher=pymizu.r_launcher()) as pool:
    spec = pymizu.call(source="summary(cars$speed)")
    print(pool.submit(spec).collect())
```
