# Channel

A shared-memory channel handle (process-private).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L172-L599)

``` python
Channel()
```

Create the host side with [`Channel.create()`](../reference/pymizu.Channel.create.md), which spawns the peer. The peer side attaches with `Channel._attach()`, as `python -m pymizu.child` does. Handles do not survive `fork()`.

Threading: a side belongs to one thread at a time. Call its verbs ([`send()`](../reference/pymizu.Channel.send.md), [`recv()`](../reference/pymizu.Channel.recv.md), their batch forms) from one thread only; a second verb from another thread raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError). Blocking and async receives are likewise exclusive on a side: a blocking receive while a [`recv_async()`](../reference/pymizu.Channel.recv_async.md) consumer is active raises it too, from any thread but the consumer's event loop, whose own receives can never race it. Two-way traffic from two threads needs two channels. Teardown is the exception: [`destroy()`](../reference/pymizu.Channel.destroy.md) is safe from any thread.

## Attributes

| Name                             | Description                           |
|----------------------------------|---------------------------------------|
| [token](pymizu.Channel.token.md) | The join token for the peer's attach. |

## Functions

| Name | Description |
|----|----|
| [\_\_aiter\_\_()](pymizu.Channel.__aiter__.md) | Iterate over received payloads until CLOSED or PEER_GONE. |
| [\_\_iter\_\_()](pymizu.Channel.__iter__.md) | Iterate over received payloads until CLOSED or PEER_GONE. |
| [alive()](pymizu.Channel.alive.md) | True while the peer process is alive. |
| [close()](pymizu.Channel.close.md) | Close the channel in an orderly way. |
| [create()](pymizu.Channel.create.md) | Create a channel and spawn its peer. |
| [destroy()](pymizu.Channel.destroy.md) | Tear down the handle immediately, without the close handshake. |
| [info()](pymizu.Channel.info.md) | A read-only snapshot of the channel's state (dict). |
| [recv()](pymizu.Channel.recv.md) | Receive one value. |
| [recv_async()](pymizu.Channel.recv_async.md) | Receive the next value as an `asyncio.Future`. |
| [recv_batch()](pymizu.Channel.recv_batch.md) | Receive up to `n` values in one call. |
| [recv_batch_async()](pymizu.Channel.recv_batch_async.md) | Receive up to `n` values as one list on an `asyncio.Future`. |
| [send()](pymizu.Channel.send.md) | Send one value. |
| [send_batch()](pymizu.Channel.send_batch.md) | Send several values in one call. |
