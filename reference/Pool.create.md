# Pool.create()

Create a pool and spawn its worker processes.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L655-L735)

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
    startup_timeout=30.0,
    launcher=None,
)
```

## Parameters

`workers: int = 1`  
Number of worker processes to spawn.

`max_workers: int | None = None`  
The worker limit; defaults to `workers`.

`max_submitters: int = 8`  
Maximum number of submitter processes.

`injection_cap: int = 1024`  
Capacity of each submitter's queue.

`per_worker_cap: int = 1024`  
Capacity of each worker's queue.

`result_slots: int = 4096`  
Bounds each submitter's outstanding (uncollected) tasks.

`slot_size: int = 512`  
Bytes per queue entry and result slot; a larger payload travels in its own shared-memory region.

`startup_timeout: float = 30.0`  
Seconds to wait for the workers to attach.

`launcher: _Callable[[str, int], _Any] | None = None`  
A `callable(token, slot)` arranging for a Python process to run `python -m pymizu.worker <token> <slot>`; the default spawns `sys.executable` directly.

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
