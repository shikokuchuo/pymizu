# recv_batch_async()

Return an `asyncio.Future` for a list of received values.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L446-L505)

``` python
recv_batch_async(n=256)
```

## Parameters

`n: int = 256`  
The maximum batch size.

## Returns

`An asyncio.Future resolving with a list of up to n values in`  
arrival order. The list can be short: the Future resolves as soon as one value is available and does not wait for a full batch. Terminal states stay values: the Future resolves with the `CLOSED` / `PEER_GONE` sentinel. An `n` past the drain quantum (2048) resolves with at most one quantum per pass.

## Raises

`ValueError`  
`n` is less than 1.

`ConcurrentUseError`  
A blocking receive on this side from another thread races the armed async consumer: a side is confined to one thread and one consumer form at a time.

## Details

Single and batch waiters on one channel resolve in call order. A batch waiter takes up to its `n` values from the available burst. It takes at least one value when reached, so a batch reached late in a busy pass can resolve short.

Cancellation, timeouts, `spin=True`, and consumer exclusion follow [`recv_async()`](../reference/pymizu.Channel.recv_async.md).

A declined foreign payload ahead of a batch resolves it short with the preceding values. The waiter that reaches the declined message rejects with [`ForeignPayloadError`](../reference/ForeignPayloadError.md#pymizu.ForeignPayloadError). Later waiters are unaffected.

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
