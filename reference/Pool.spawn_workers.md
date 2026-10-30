# Pool.spawn_workers()

Spawn `n` additional workers and wait for them to join

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1306-L1350)

``` python
Pool.spawn_workers(
    n=1,
    *,
    startup_timeout=30.0,
    launcher=None,
)
```

(controller only).

## Parameters

`n: int = 1`  
Number of workers to spawn.

`startup_timeout: float = 30.0`  
Seconds to wait for the workers to attach.

`launcher: _Callable[[str, int], _Any] | None = None`  
As for [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create); the default spawns `sys.executable` directly.

## Returns

`The slot indices spawned into.`  
