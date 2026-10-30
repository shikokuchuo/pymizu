# Task.cancel()

cancel() -\> bool

Usage

``` python
Task.cancel()
```

Advisory only, never interrupts: a queued task is skipped; a running one completes and its result is dropped. True when this call cancelled the task; False in every other case.
