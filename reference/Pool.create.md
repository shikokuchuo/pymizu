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


`workers` worker processes join the pool's registry (capacity `max_workers`). `result_slots` bounds each submitter's outstanding (uncollected) tasks; `slot_size` is the bytes per queue entry and result slot -- a payload past the inline budget travels in a fresh region per payload. `launcher` is a `callable(token, slot)` arranging for a Python process to run `python -m pymizu.worker <token> <slot>`; the default spawns `sys.executable` directly.
