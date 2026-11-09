# launcher()

Return a launcher spawning Python processes running pymizu.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L82-L135)

``` python
launcher(
    *,
    python=None,
    stdout=None,
    stderr=None,
)
```

[`Channel.create()`](../reference/Channel.create.md#pymizu.Channel.create) calls it with `token` alone to spawn the peer; [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create) calls it with `token` and `slot` to spawn each worker. The process runs `python -m pymizu.child <token>` or `python -m pymizu.worker <token> <slot>`. This is the default of every `launcher=` argument, and the counterpart of the R package's `mizu_launcher()`.

## Parameters

`python: str | None = None`  
Path to the Python interpreter; the default is `sys.executable`.

`stdout: _Any = None`  
Forwarded to `subprocess.Popen`; the default inherits the console, where the peer's error messages land.

`stderr: _Any = None`  
Forwarded to `subprocess.Popen`.

## Returns

`A callable(token, slot=None) launcher for`  
[`Channel.create()`](../reference/Channel.create.md#pymizu.Channel.create) and [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create).

## Examples

``` python
import pymizu

with pymizu.Pool.create(
    4, launcher=pymizu.launcher(stderr="worker.log")
) as pool:
    print(pool.submit(abs, -1).collect())
```
