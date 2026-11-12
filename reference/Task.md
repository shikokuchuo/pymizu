# Task

A task handle from Pool.submit(). It is collected exactly once, and an uncollected handle releases its slot when garbage collected.

Usage

``` python
Task()
```

## Methods

| Name | Description |
|----|----|
| [\_\_await\_\_()](#__await__) | Return an iterator to be used in await expression. |
| [cancel()](#cancel) | cancel() -\> bool |
| [collect()](#collect) | collect(timeout=None) -\> value \| sentinel |
| [collect_async()](#collect_async) | collect_async() -\> asyncio.Future |

### \_\_await\_\_()

Return an iterator to be used in await expression.

Usage

``` python
__await__()
```

### cancel()

cancel() -\> bool

Usage

``` python
cancel()
```

Advisory only, and never interrupts: a queued task is skipped, and a running one completes with its result dropped. Returns True when this call cancelled the task, False in every other case.

### collect()

collect(timeout=None) -\> value \| sentinel

Usage

``` python
collect(timeout=None)
```

Wait up to `timeout` seconds (None indefinitely, 0 polls once) for the task to finish and return its result. A task error re-raises as pymizu.TaskError (carrying remote_type and detail), a cancellation as pymizu.CancelledError, a dead worker as pymizu.WorkerDiedError, and a lost result as pymizu.TaskLostError. Returns pymizu.TIMEOUT on expiry. A task is collected exactly once.

### collect_async()

collect_async() -\> asyncio.Future

Usage

``` python
collect_async()
```

The event-driven collect: an asyncio.Future that resolves with the task's result (or raises the error collect() raises) when the result arrives, without blocking or polling. Cancelling the Future detaches the waiter, so a later blocking collect() works again, and never cancels the task (only cancel() does).
