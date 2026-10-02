---
name: pymizu
description: >
  Lock-free shared-memory parallelism for Python. Use when writing Python code that uses the pymizu package.
license: MIT
compatibility: Requires Python >=3.10.
---

# pymizu 水

Lock-free shared-memory parallelism for Python

## Installation

```bash
pip install pymizu
```

## API overview

### Channels

A two-way message link between a Python process and a helper process that it spawns.

- `Channel`: A shared-memory SPSC channel handle (process-private)

### Channel Methods

Methods for the Channel class

- `Channel.create`
- `Channel.attach`
- `Channel.token`
- `Channel.send`
- `Channel.send_batch`
- `Channel.recv`
- `Channel.recv_batch`
- `Channel.close`
- `Channel.close_signal`
- `Channel.destroy`
- `Channel.alive`
- `Channel.info`
- `Channel.__enter__`
- `Channel.__exit__`

### Task pools

A set of worker processes that divide submitted tasks among themselves.

- `Pool`: A shared-memory work-stealing task pool handle (process-private)
- `Task`: A task handle from _Pool.submit(). Collected exactly once; an uncollected handle's finalizer releases its slot

### Pool Methods

Methods for the Pool class

- `Pool.create`
- `Pool.attach`
- `Pool.token`
- `Pool.submit`
- `Pool.submit_batch`
- `Pool.collect_any`
- `Pool.collect_all`
- `Pool.map`
- `Pool.map_prepare`
- `Pool.map_run`
- `Pool.retire`
- `Pool.spawn_workers`
- `Pool.stop`
- `Pool.destroy`
- `Pool.status`
- `Pool.dump`
- `Pool.stats`
- `Pool.trace`
- `Pool.__enter__`
- `Pool.__exit__`

### Cross-language interop

R peers and workers (the mizu R package), neutral task specifications, and the data.frame's Python home.

- `call`: A task specification for a pool of another language's workers
- `r_launcher`: Return a ``Channel.create`` launcher spawning an R peer
- `r_pool_launcher`: Return a ``Pool.create`` launcher spawning R workers
- `Frame`: A data.frame's Python home: named columns with a row count

### Frame Methods

Methods for the Frame class

- `Frame.names`
- `Frame.row_names`
- `Frame.to_dict`
- `Frame.__arrow_c_stream__`
- `Frame.__len__`
- `Frame.__reduce__`

### Outcomes and errors

Terminal transport states return as the sentinel singletons FULL / TIMEOUT / CLOSED / PEER_GONE (identity-tested, never raised); real failures raise from the exception hierarchy.

- `is_sentinel`: is_sentinel(x) -> bool
- `is_remote_error`: Test whether a received channel value is a remote error
- `MizuError`: Base class for all pymizu errors
- `ShmError`: A shared-memory region operation failed
- `StartupError`: A channel peer or pool worker failed to attach within the startup timeout
- `SubmitTimeoutError`: Pool.submit() timed out waiting for injection-ring space
- `SlotsExhaustedError`: Pool.submit() found no free result slot: too many outstanding (uncollected) tasks
- `StoppedError`: The pool is stopped; no further submission is possible
- `CancelledError`: The task was cancelled before it ran
- `TaskError`: The task callable raised. Carries 'remote_type' and 'remote_traceback' attributes describing the worker-side exception
- `WorkerDiedError`: The executing worker died mid-task. Carries 'slot' and 'pid' attributes identifying the worker
- `DeclinedError`: A send on a foreign-language channel of a value outside the portable interchange subset. Carries 'path' and 'reason' attributes

### Housekeeping

Pool context from inside a task, and orphan reaping.

- `current_pool`: The evaluating worker's own pool handle, inside a task
- `prune`: Remove orphaned shared memory regions

## Resources

- [Full documentation](https://shikokuchuo.net/pymizu/)
- [llms.txt](llms.txt) — Indexed API reference for LLMs
- [llms-full.txt](llms-full.txt) — Comprehensive documentation for LLMs
