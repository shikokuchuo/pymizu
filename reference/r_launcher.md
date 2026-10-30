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

[`Channel.create()`](../reference/Channel.create.md#pymizu.Channel.create) calls it with `token` alone to spawn the peer; [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create) calls it with `token` and `slot` to spawn each worker. The process runs `rscript` on the package's static child runner with the entry expression (`mizu:::peer_main(<token>)` or `mizu:::worker_main(<token>, <slot>)`) and the detected library paths passed along in argv (without them it cannot `library(mizu)` from the host's libraries). The mirror of the R package's `mizu_py_launcher()`.

## Parameters

`rscript: str | None = None`  
Path to Rscript; the default searches the PATH.

`stdout: _Any = None`  
Forwarded to `subprocess.Popen`; the default inherits the console, where the peer's error messages land.

`stderr: _Any = None`  
Forwarded to `subprocess.Popen`.

## Returns

`A callable(token, slot=None) launcher for`  
[`Channel.create()`](../reference/Channel.create.md#pymizu.Channel.create) and [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create).

## Raises

`MizuError`  
Raised here, before anything exists, when Rscript is not found or no installed `mizu` with source string support is available.

## Notes

The first worker's join records the workers' language in the pool, so the launcher carries no language attribute: a pool of R workers takes [`pymizu.call()`](../reference/call.md#pymizu.call) specifications through [`Pool.submit()`](../reference/Pool.submit.md#pymizu.Pool.submit), and a plain callable raises an error naming [call](../reference/call.md#pymizu.call). A launcher that spawns the wrong language fails at join, not at the first task.

## Examples

``` python
import pymizu

with pymizu.Pool.create(4, launcher=pymizu.r_launcher()) as pool:
    spec = pymizu.call(source="summary(cars$speed)")
    print(pool.submit(spec).collect())
```
