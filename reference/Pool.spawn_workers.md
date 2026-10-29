# Pool.spawn_workers()

Spawn `n` additional workers into free registry slots and

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1304-L1348)

``` python
Pool.spawn_workers(
    n=1,
    *,
    startup_timeout=30.0,
    launcher=None,
)
```

wait for them to join (controller only).

## Parameters

`n: int = 1`  
Number of workers to spawn.

`startup_timeout: float = 30.0`  
Seconds to wait for the workers to attach.

`launcher: _Callable[[str, int], _Any] | None = None`  
As for [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create); the default spawns `sys.executable` directly.

## Returns

`The slot indices spawned into.`  
