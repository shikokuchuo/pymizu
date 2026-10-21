# r_launcher()

Return a [`Channel.create()`](../reference/Channel.create.md#pymizu.Channel.create) launcher

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_r.py#L119-L168)

``` python
r_launcher(
    *,
    rscript=None,
    stdout=None,
    stderr=None,
)
```

spawning an R peer.

The peer runs the R package `mizu`: the returned `callable(token)` spawns `rscript` on the package's static child runner with `mizu:::peer_main(<token>)` as the entry expression and the probed library paths propagated in argv.

## Parameters

`rscript: str | None = None`  
Path to Rscript; the default searches the PATH.

`stdout: _Any = None`  
Forwarded to `subprocess.Popen`; the default inherits the console, where the peer's error epilogue lands.

`stderr: _Any = None`  
Forwarded to `subprocess.Popen`.

## Returns

`A callable(token) launcher for`  
[`Channel.create()`](../reference/Channel.create.md#pymizu.Channel.create).

## Raises

`MizuError`  
Raised here, before the channel is created, when Rscript is not found or no installed `mizu` with source string support is available.
