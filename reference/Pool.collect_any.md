# Pool.collect_any()


Wait on several tasks; return `(index, value)` of the first


Usage

``` python
Pool.collect_any(
    tasks,
    timeout=None,
)
```


terminal one, or the TIMEOUT sentinel. A non-OK outcome raises with an `index` attribute (0-based).
