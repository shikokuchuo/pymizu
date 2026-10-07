# Pool.submit_batch()


Submit one task per zero-arg callable in `fns` in one


Usage

``` python
Pool.submit_batch(
    fns,
    *,
    timeout=None,
)
```


crossing.


## Parameters


`fns: _Iterable[_Callable[[], _Any]]`  
Zero-arg callables, one task each. Use `functools.partial` to bind arguments.

`timeout: float | None = None`  
Seconds to wait for injection-ring space; None waits indefinitely.


## Returns


`A list of `<a href="../reference/Task.html#pymizu.Task" class="gdls-link"><code>Task</code></a>` handles. Ring-full past`  
`timeout` ends the batch short -- the returned handles stay valid and collectible.
