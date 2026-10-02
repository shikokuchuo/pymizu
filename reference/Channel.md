# Channel


A shared-memory SPSC channel handle (process-private).


Usage

``` python
Channel()
```


Create the host side with :meth:`create` (which spawns the peer); the peer side attaches with :meth:`attach` -- `python -m pymizu.child` does this. Handles do not survive `fork()`.
