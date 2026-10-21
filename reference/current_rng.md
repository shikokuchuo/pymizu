# current_rng()

The running element's own numpy Generator, inside a seeded map.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1285-L1318)

``` python
current_rng()
```

## Returns

`The element's memoized numpy.random.Generator inside a`  
seeded map element; None outside one.

## Raises

`TypeError`  
When numpy is not installed.

## Details

[`Pool.map()`](../reference/Pool.map.md#pymizu.Pool.map) `seed=` seeds the stdlib `random` module per element; a task drawing from numpy calls this instead: the element's memoized `numpy.random.Generator`, derived from the same seed material on first call in the element (domain-separated from the stdlib derivation, so those streams are unchanged). Two calls in one element continue one stream; distinct elements get distinct streams -- results identical for any chunking, worker count, or steal order.

Outside a seeded map element (an unseeded map, an ordinary task, the submitter process) the return is None. A seeded map element must not nested-submit and collect: worker helping can run another map's batches mid-element, wiping this element's stash -- a later call rebuilds from the digest, restarting the stream instead of continuing it.
