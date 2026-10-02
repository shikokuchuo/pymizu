# Channel.create()


Create a channel and spawn its peer.


Usage

``` python
Channel.create(
    peer,
    *,
    capacity=16384,
    slot_size=256,
    arena_size=4 * 1024 * 1024,
    spin=False,
    startup_timeout=30.0,
    launcher=None,
)
```


`peer` is a Python source string, evaluated in the peer process with `ch` bound to the peer-side handle. `capacity` and `slot_size` are powers of two; the inline payload budget is `slot_size - 16`. `launcher` is a `callable(token)` arranging for a Python process to run `python -m pymizu.child <token>`; the default spawns `sys.executable` directly. [pymizu.r_launcher()](r_launcher.md#pymizu.r_launcher) returns one spawning an R peer (the R package `mizu`).
