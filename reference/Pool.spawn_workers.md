# Pool.spawn_workers()


Spawn `n` additional workers into free registry slots and


Usage

``` python
Pool.spawn_workers(
    n=1,
    *,
    launcher=None,
    startup_timeout=30.0,
)
```


wait for them to join.


## Parameters


`n: int = ``1`  
Number of workers to spawn.

`launcher: _Callable[[str, int], _Any] | None = None`  
As for <a href="../reference/Pool.create.html#pymizu.Pool.create" class="gdls-link"><code>Pool.create()</code></a>; the default spawns `sys.executable` directly.

`startup_timeout: float = ``30.0`  
Seconds to wait for the workers to attach.


## Returns


`The slot indices spawned into.`
