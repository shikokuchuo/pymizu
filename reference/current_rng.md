# current_rng()

Return the current element's numpy Generator inside a seeded map.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1629-L1650)

``` python
current_rng()
```

## Returns

`The element's own numpy.random.Generator inside a seeded map`  
element. None outside one.

## Raises

`TypeError`  
When numpy is not installed.

## Details

The first call in an element creates the generator from the map seed. Later calls in that element continue the same stream. This derivation is separate from the stdlib `random` stream. See [`Pool.map()`](../reference/pymizu.Pool.map.md) for the seed format, reproducibility rules, and nested-task restriction.
