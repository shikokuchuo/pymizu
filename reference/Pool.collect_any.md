# Pool.collect_any()


Wait on several tasks.


Usage

``` python
Pool.collect_any(
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


`(index, value)`` of the first terminal task, or the`  
`TIMEOUT` sentinel. A non-OK outcome raises with an `index` attribute (0-based).
