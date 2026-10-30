# Channel

A shared-memory channel handle (process-private).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L85-L421)

``` python
Channel()
```

Create the host side with [`Channel.create()`](../reference/Channel.create.md#pymizu.Channel.create) (which spawns the peer); the peer side attaches with [`Channel.attach()`](../reference/Channel.attach.md#pymizu.Channel.attach) -- `python -m pymizu.child` does this. Handles do not survive `fork()`.

Threading: a side belongs to one thread at a time -- call its verbs ([`send()`](../reference/Channel.send.md#pymizu.Channel.send), [`recv()`](../reference/Channel.recv.md#pymizu.Channel.recv), their batch forms) from one thread only; a second verb from another thread raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError). Blocking and async receives are likewise exclusive on a side: a blocking receive while a [`recv_async()`](../reference/Channel.recv_async.md#pymizu.Channel.recv_async) consumer is active raises it too -- from any thread but the consumer's event loop, whose own receives can never race it. Two-way traffic from two threads needs two channels. Teardown is the exception: [`destroy()`](../reference/Channel.destroy.md#pymizu.Channel.destroy) is safe from any thread.
