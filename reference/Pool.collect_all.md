# Pool.collect_all()


Wait until every task is terminal.


Usage

``` python
Pool.collect_all(
    tasks,
    timeout=None,
)
```


## Parameters


`tasks: _Iterable[Task]`  
The <a href="../reference/Task.html#pymizu.Task" class="gdls-link"><code>Task</code></a> handles to wait on.

`timeout: float | None = None`  
Seconds to wait; None waits indefinitely.


## Returns


`All values in input order. On the first non-OK outcome by`  
position, raises with an `index` attribute (0-based) -- only the reported handle is consumed; every other, the results ahead of it included, stays collectible. A result that fails to read raises with `index` and a `values` attribute carrying the consumed prefix. The `TIMEOUT` sentinel consumes nothing.
