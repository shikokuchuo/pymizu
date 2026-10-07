# r_launcher()


Return a <a href="../reference/Channel.create.html#pymizu.Channel.create" class="gdls-link"><code>Channel.create()</code></a> launcher


Usage

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


`A ``callable(token)`` launcher for`  
<a href="../reference/Channel.create.html#pymizu.Channel.create" class="gdls-link"><code>Channel.create()</code></a>.


## Raises


`MizuError`  
Raised here, before the channel is created, when Rscript is not found or no installed `mizu` with source string support is available.
