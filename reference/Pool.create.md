# Pool.create()


Create a pool and spawn its worker processes.


Usage

``` python
Pool.create(
    workers=1,
    *,
    max_workers=None,
    max_submitters=8,
    injection_cap=1024,
    per_worker_cap=1024,
    result_slots=4096,
    slot_size=512,
    launcher=None,
    startup_timeout=30.0,
)
```


## Parameters


`workers: int = ``1`  
Number of worker processes to spawn; they join the pool's registry (capacity `max_workers`).

`max_workers: int | None = None`  
Registry capacity; defaults to `workers`.

`max_submitters: int = ``8`  
Maximum number of submitter processes.

`injection_cap: int = ``1024`  
Capacity of the injection ring.

`per_worker_cap: int = ``1024`  
Capacity of each worker's work-stealing deque.

`result_slots: int = ``4096`  
Bounds each submitter's outstanding (uncollected) tasks.

`slot_size: int = ``512`  
Bytes per queue entry and result slot -- a payload past the inline budget travels in a fresh region per payload.

`launcher: _Callable[[str, int], _Any] | None = None`  
A `callable(token, slot)` arranging for a Python process to run `python -m pymizu.worker <token> <slot>`; the default spawns `sys.executable` directly.

`startup_timeout: float = ``30.0`  
Seconds to wait for the workers to attach.


## Returns


`The controller-side pool handle.`  


## Examples

``` python
>>> import pymizu
>>> with pymizu.Pool.create(2) as pool:
...     task = pool.submit(pow, 2, 16)
...     results = pool.collect_all([task], timeout=10)
>>> results
[65536]
```
