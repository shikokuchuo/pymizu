# Task


A task handle from \_Pool.submit(). Collected exactly once; an uncollected handle's finalizer releases its slot.


Usage


``` python
Task()
```


## Methods

| Name | Description |
|----|----|
| [cancel()](#cancel) | cancel() -\> bool |
| [collect()](#collect) | collect(timeout=None) -\> value \| sentinel |

------------------------------------------------------------------------


### cancel()


cancel() -\> bool


Usage


``` python
cancel()
```


Advisory and discard-only, never preemptive: a still-queued task is skipped; an executing one runs to completion and its result is dropped. True when this call cancelled the task; every other edge folds to False.


------------------------------------------------------------------------


### collect()


collect(timeout=None) -\> value \| sentinel


Usage


``` python
collect(timeout=None)
```


Wait up to `timeout` seconds (None indefinitely, 0 polls) for the task's terminal state and return its result. A task error re-raises as pymizu.TaskError (carrying remote_type / remote_traceback), a cancellation as pymizu.CancelledError, a dead worker as pymizu.WorkerDiedError. pymizu.TIMEOUT on expiry; a task is collected exactly once.
