# Channel.iter_batches()

Iterate over received batches until CLOSED or PEER_GONE.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L646-L684)

``` python
Channel.iter_batches(n=256)
```

The batch form of the channel iterator: each yield is a list of up to `n` payloads -- the high-throughput receive form. Blocks indefinitely between batches ([recv_batch()](../reference/Channel.recv_batch.md#pymizu.Channel.recv_batch) with no timeout); use [`recv_batch()`](../reference/Channel.recv_batch.md#pymizu.Channel.recv_batch) directly when a bound is needed.

## Parameters

`n: int = 256`  
The maximum batch size; must be at least 1.

`A`  

`the`  

`the`  

## Examples

``` python
import pymizu

with pymizu.Channel.create('''
import pymizu
for _ in range(3):
    ch.send(ch.recv())
''') as ch:
    ch.send_batch([1, 2, 3])
    batches = list(ch.iter_batches())
[x for batch in batches for x in batch]
```

    [1, 2, 3]
