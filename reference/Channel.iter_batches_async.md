# Channel.iter_batches_async()

Iterate over received batches until CLOSED or PEER_GONE.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L444-L487)

``` python
Channel.iter_batches_async(n=256)
```

The batch form of the async channel iterator: each yield is a list of up to `n` payloads, awaited through [`recv_batch_async()`](../reference/Channel.recv_batch_async.md#pymizu.Channel.recv_batch_async) -- the high-throughput receive form, one Future per burst. Between iterations nothing is armed; the next iteration re-arms, and the arm's synchronous drain picks up whatever arrived meanwhile, so nothing is lost.

## Parameters

`n: int = 256`  
The maximum batch size; must be at least 1.

`A`  

`the`  

`cannot`  

`channel`  

## Examples

``` python
import pymizu

with pymizu.Channel.create('''
import pymizu
for _ in range(3):
    ch.send(ch.recv())
''') as ch:
    ch.send_batch([1, 2, 3])
    batches = []
    async for batch in ch.iter_batches_async():
        batches.append(batch)
[x for batch in batches for x in batch]
```

    [1, 2, 3]
