# Task.cancel()

cancel() -\> bool

Usage

``` python
Task.cancel()
```

Advisory and discard-only, never preemptive: a still-queued task is skipped; an executing one runs to completion and its result is dropped. True when this call cancelled the task; every other edge folds to False.
