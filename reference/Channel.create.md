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


## Parameters


`peer: str`  
A Python source string, evaluated in the peer process with `ch` bound to the peer-side handle.

`capacity: int = ``16384`  
Ring capacity in slots; a power of two.

`slot_size: int = ``256`  
Bytes per ring slot; a power of two. The inline payload budget is `slot_size - 16`.

`arena_size: int = 4 * 1024 * 1024`  
Bytes of shared staging arena for out-of-line payloads.

`spin: bool = ``False`  
Spin rather than park while waiting.

`startup_timeout: float = ``30.0`  
Seconds to wait for the peer to attach.

`launcher: _Callable[[str], _Any] | None = None`  
A `callable(token)` arranging for a Python process to run `python -m pymizu.child <token>`; the default spawns `sys.executable` directly. <a href="../reference/r_launcher.html#pymizu.r_launcher" class="gdls-link"><code>r_launcher()</code></a> returns one spawning an R peer (the R package `mizu`).


## Returns


`The host-side channel handle.`  


## Examples

``` python
>>> import pymizu
>>> with pymizu.Channel.create(
...     """
... import pymizu
... while True:
...     x = ch.recv()
...     if x is pymizu.CLOSED:
...         break
...     ch.send(x)
... """
... ) as ch:
...     ch.send([1, "a", None])
...     received = ch.recv(timeout=5)
>>> received
[1, 'a', None]
```
