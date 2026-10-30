# Task.collect()

collect(timeout=None) -\> value \| sentinel

Usage

``` python
Task.collect(timeout=None)
```

Wait up to `timeout` seconds (None indefinitely, 0 polls once) for the task to finish and return its result. A task error re-raises as pymizu.TaskError (carrying remote_type / remote_traceback), a cancellation as pymizu.CancelledError, a dead worker as pymizu.WorkerDiedError, a lost result as pymizu.TaskLostError. pymizu.TIMEOUT on expiry; a task is collected exactly once.
