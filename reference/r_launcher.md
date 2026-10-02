# r_launcher()


Return a [Channel.create](Channel.create.md#pymizu.Channel.create) launcher spawning an R peer.


Usage

``` python
r_launcher(
    *,
    rscript=None,
    stdout=None,
    stderr=None,
)
```


The peer runs the R package `mizu`: the returned `callable(token)` spawns `rscript` on the package's static child runner with `mizu:::peer_main(<token>)` as the entry expression and the probed library paths propagated in argv. Requires Rscript on the PATH (or passed as `rscript`) and an installed `mizu` with source string support -- MizuError is raised here, before the channel is created, otherwise. `stdout` and `stderr` forward to subprocess.Popen; the default inherits the console, where the peer's error epilogue lands.
