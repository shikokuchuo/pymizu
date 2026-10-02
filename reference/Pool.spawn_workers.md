# Pool.spawn_workers()


Spawn `n` additional workers into free registry slots and wait


Usage

``` python
Pool.spawn_workers(
    n=1,
    *,
    launcher=None,
    startup_timeout=30.0,
)
```


for them to join. Returns the slot indices spawned into.
