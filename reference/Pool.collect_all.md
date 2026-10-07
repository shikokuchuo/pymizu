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
position, raises with an `index` attribute (0-based) -- handles up to it inclusive are consumed, the rest stay collectible. The `TIMEOUT` sentinel consumes nothing.
