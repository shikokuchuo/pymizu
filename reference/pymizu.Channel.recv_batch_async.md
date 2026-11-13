# recv_batch_async()

Receive up to `n` values as one list on an `asyncio.Future`.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L453-L525)

``` python
recv_batch_async(n=256)
```

The burst form of [`recv_async()`](../reference/pymizu.Channel.recv_async.md): one Future resolves with a whole burst of arrivals at once, the high-throughput receive form -- one resolution per burst rather than one per message.

## Parameters

`n: int = 256`  
The maximum batch size.

## Returns

`An asyncio.Future resolving with a list of up to n`  
values in arrival order -- possibly short: it resolves as soon as at least one value is available, never waiting to fill. Terminal states stay values: the Future resolves with the `CLOSED` / `PEER_GONE` sentinel. An `n` past the drain quantum (2048) resolves with at most one quantum per pass.

## Raises

`ValueError`  
`n` is less than 1.

`ConcurrentUseError`  
A blocking receive on this side from another thread races the armed async consumer: a side is confined to one thread and one consumer form at a time.

## Details

Outstanding calls on one channel -- [`recv_async()`](../reference/pymizu.Channel.recv_async.md) singles and batch waiters mixed -- resolve in call order, each batch waiter taking up to its `n` from the burst (at least one when it is reached, so a batch reached late in a busy pass can resolve short). Cancelling the Future detaches the waiter, and the next message goes to the next waiter. There is no `timeout` argument: use `asyncio.timeout` or `asyncio.wait_for`. A channel created with `spin=True` cannot arm and raises. The async consumer is the side's one consumer: a blocking [`recv()`](../reference/pymizu.Channel.recv.md) on the same side while waiters are pending raises [`ConcurrentUseError`](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError).

A declined foreign payload (see [`recv_async()`](../reference/pymizu.Channel.recv_async.md)) ahead of a batch resolves it short with the values before it; the waiter that reaches the declined message rejects with [`ForeignPayloadError`](../reference/ForeignPayloadError.md#pymizu.ForeignPayloadError), and the consumer keeps flowing.

## Examples

``` python
import pymizu

with pymizu.Channel.create('''
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED:
        break
    ch.send(x)
''') as ch:
    ch.send_batch([1, 2, 3])
    batch = await ch.recv_batch_async()
batch
```

    [1, 2, 3]
