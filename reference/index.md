# API Reference

## Channels

A two-way message link between a Python process and a helper process that it spawns.

[Channel](Channel.md#pymizu.Channel)  
A shared-memory channel handle (process-private).

## Task pools

A set of worker processes that divide submitted tasks among themselves.

[Pool](Pool.md#pymizu.Pool)  
A shared-memory work-stealing task pool handle (process-private).

[Task](Task.md#pymizu.Task)  
A task handle from Pool.submit(). It is collected exactly once, and an uncollected handle releases its slot when garbage collected.

[Map](Map.md#pymizu.Map)  
A staged, submitted map; collects exactly once, through this handle.

[PreparedMap](PreparedMap.md#pymizu.PreparedMap)  
A map set up once in shared memory, run many times.

[PoolExecutor](PoolExecutor.md#pymizu.PoolExecutor)  
A `concurrent.futures.Executor` over a pymizu pool.

## Cross-language interop

R peers and workers (the mizu R package), neutral task specifications, and the data.frame's Python home.

[call](call.md#pymizu.call)  
A task specification for a pool of another language's workers.

[launcher()](launcher.md#pymizu.launcher)  
Return a launcher spawning Python processes running pymizu.

[r_launcher()](r_launcher.md#pymizu.r_launcher)  
Return a launcher spawning R processes running the `mizu` package.

[Frame](Frame.md#pymizu.Frame)  
A data.frame's Python home: named columns with a row count.

[DimArray](DimArray.md#pymizu.DimArray)  
An `ndarray` carrying R labels for faithful round trips with R.

## Outcomes and errors

A full ring, a timeout, a close, or a dead peer returns as a sentinel singleton -- FULL / TIMEOUT / CLOSED / PEER_GONE -- tested by identity, never raised. Real failures raise exceptions.

[Sentinel](Sentinel.md#pymizu.Sentinel)  
A terminal-state sentinel returned by pymizu verbs. Test by identity against pymizu.FULL / TIMEOUT / CLOSED / PEER_GONE.

[is_sentinel()](is_sentinel.md#pymizu.is_sentinel)  
is_sentinel(x) -\> bool

[is_remote_error()](is_remote_error.md#pymizu.is_remote_error)  
Test whether a received channel value is a remote error.

[MizuError](MizuError.md#pymizu.MizuError)  
Base class for all pymizu errors.

[ShmError](ShmError.md#pymizu.ShmError)  
A shared-memory region operation failed. Also an OSError: the failure is at the OS layer. Carries a 'bytes' attribute: the requested size of a region that was not created, None when a region was not opened.

[StartupError](StartupError.md#pymizu.StartupError)  
A channel peer or pool worker failed to attach within the startup timeout.

[SubmitTimeoutError](SubmitTimeoutError.md#pymizu.SubmitTimeoutError)  
Pool.submit() timed out waiting for injection-ring space. Also a TimeoutError.

[SlotsExhaustedError](SlotsExhaustedError.md#pymizu.SlotsExhaustedError)  
Pool.submit() found no free result slot: too many outstanding (uncollected) tasks.

[StoppedError](StoppedError.md#pymizu.StoppedError)  
The pool is stopped; no further submission is possible.

[CancelledError](CancelledError.md#pymizu.CancelledError)  
The task was cancelled before it ran.

[TaskError](TaskError.md#pymizu.TaskError)  
The task callable raised. Carries 'remote_type' and 'detail' attributes describing the worker-side exception.

[WorkerDiedError](WorkerDiedError.md#pymizu.WorkerDiedError)  
The executing worker died mid-task. Carries 'slot' and 'pid' attributes identifying the worker.

[TaskLostError](TaskLostError.md#pymizu.TaskLostError)  
The task's payload region vanished or could not be read before any worker ran it. No worker died.

[DeclinedError](DeclinedError.md#pymizu.DeclinedError)  
A send on a foreign-language channel of a value outside the portable interchange subset. Carries 'path' and 'reason' attributes ('reason' includes the one-line rewrite guidance for the value).

[ForeignPayloadError](ForeignPayloadError.md#pymizu.ForeignPayloadError)  
A received payload in a language-private or unrecognized format: the peer sent a value outside the portable interchange subset. The message is consumed, so the ring is never blocked, and under recv_async() only the Future waiting on that message rejects.

[ConcurrentUseError](ConcurrentUseError.md#pymizu.ConcurrentUseError)  
A verb ran concurrently with another verb in the same handle role: each handle role is confined to one thread at a time (libmizu 'Handle threading').

## Housekeeping

The process-wide default pool, the current worker's own pool handle, and cleaning up shared memory left by dead processes.

[default_pool()](default_pool.md#pymizu.default_pool)  
The process-wide default pool, or None when none is set.

[set_default_pool()](set_default_pool.md#pymizu.set_default_pool)  
Set the process-wide default pool; None clears it.

[using_pool()](using_pool.md#pymizu.using_pool)  
Use `pool` as the default for the with block, then restore.

[current_pool()](current_pool.md#pymizu.current_pool)  
The pool handle of the worker running the task.

[current_rng()](current_rng.md#pymizu.current_rng)  
The current element's own numpy Generator, inside a seeded map.

[prune()](prune.md#pymizu.prune)  
Remove orphaned shared memory regions.
