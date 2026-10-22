# Channel

A shared-memory SPSC channel handle (process-private).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L85-L329)

``` python
Channel()
```

Create the host side with [`Channel.create()`](../reference/Channel.create.md#pymizu.Channel.create) (which spawns the peer); the peer side attaches with [`Channel.attach()`](../reference/Channel.attach.md#pymizu.Channel.attach) -- `python -m pymizu.child` does this. Handles do not survive `fork()`.
