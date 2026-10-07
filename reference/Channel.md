# Channel


A shared-memory SPSC channel handle (process-private).


Usage

``` python
Channel()
```


Create the host side with <a href="../reference/Channel.create.html#pymizu.Channel.create" class="gdls-link"><code>Channel.create()</code></a> (which spawns the peer); the peer side attaches with <a href="../reference/Channel.attach.html#pymizu.Channel.attach" class="gdls-link"><code>Channel.attach()</code></a> -- `python -m pymizu.child` does this. Handles do not survive `fork()`.
