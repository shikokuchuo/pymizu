# API Reference


## Channels


A two-way message link between a Python process and a helper process that it spawns.


[Channel](Channel.md#pymizu.Channel)  
A shared-memory SPSC channel handle (process-private).


## Channel Methods


Methods for the Channel class


[Channel.create()](Channel.create.md#pymizu.Channel.create)  
Create a channel and spawn its peer.

[Channel.attach()](Channel.attach.md#pymizu.Channel.attach)  
Attach to the channel named by a join token (the peer side).

[Channel.token](Channel.token.md#pymizu.Channel.token)  
The join token for the peer's attach.

[Channel.send()](Channel.send.md#pymizu.Channel.send)  
Send one payload; return None, or the FULL / CLOSED /

[Channel.send_batch()](Channel.send_batch.md#pymizu.Channel.send_batch)  
Send several payloads in one crossing; return the number

[Channel.recv()](Channel.recv.md#pymizu.Channel.recv)  
Receive one payload, waiting up to `timeout` seconds

[Channel.recv_batch()](Channel.recv_batch.md#pymizu.Channel.recv_batch)  
Receive up to `n` payloads in one crossing; a list

[Channel.close()](Channel.close.md#pymizu.Channel.close)  
Orderly close: signal the peer and wait up to `timeout`

[Channel.close_signal()](Channel.close_signal.md#pymizu.Channel.close_signal)  
Signal close without waiting (the peer's recv side sees

[Channel.destroy()](Channel.destroy.md#pymizu.Channel.destroy)  
Tear down the handle immediately, without the close

[Channel.alive()](Channel.alive.md#pymizu.Channel.alive)  
True while the peer process is alive.

[Channel.info()](Channel.info.md#pymizu.Channel.info)  
A read-only wire-state snapshot of the channel (dict).

[Channel.__enter__()](Channel.__enter__.md#pymizu.Channel.__enter__)  

[Channel.__exit__()](Channel.__exit__.md#pymizu.Channel.__exit__)  


## Task pools


A set of worker processes that divide submitted tasks among themselves.


[Pool](Pool.md#pymizu.Pool)  
A shared-memory work-stealing task pool handle (process-private).

[Task](Task.md#pymizu.Task)  
A task handle from \_Pool.submit(). Collected exactly once; an uncollected handle's finalizer releases its slot.


## Pool Methods


Methods for the Pool class


[Pool.create()](Pool.create.md#pymizu.Pool.create)  
Create a pool and spawn its worker processes.

[Pool.attach()](Pool.attach.md#pymizu.Pool.attach)  
Attach to a live pool as a submitter, by its join token.

[Pool.token](Pool.token.md#pymizu.Pool.token)  
The join token for worker/submitter attach.

[Pool.submit()](Pool.submit.md#pymizu.Pool.submit)  
Submit `fn(*args, **kwargs)` as a task; return a Task handle.

[Pool.submit_batch()](Pool.submit_batch.md#pymizu.Pool.submit_batch)  
Submit one task per zero-arg callable in `fns` in one crossing.

[Pool.collect_any()](Pool.collect_any.md#pymizu.Pool.collect_any)  
Wait on several tasks; return `(index, value)` of the first

[Pool.collect_all()](Pool.collect_all.md#pymizu.Pool.collect_all)  
Wait until every task is terminal; return all values in input

[Pool.map()](Pool.map.md#pymizu.Pool.map)  
Map `fn` over the elements of `x` on the pool; return the

[Pool.map_prepare()](Pool.map_prepare.md#pymizu.Pool.map_prepare)  
Stage a map once for repeated runs; return a map handle.

[Pool.map_run()](Pool.map_run.md#pymizu.Pool.map_run)  
Run a map handle from [map_prepare](Pool.map_prepare.md#pymizu.Pool.map_prepare) once; return its results

[Pool.retire()](Pool.retire.md#pymizu.Pool.retire)  
Ask the worker in `slot` to exit cleanly (non-blocking).

[Pool.spawn_workers()](Pool.spawn_workers.md#pymizu.Pool.spawn_workers)  
Spawn `n` additional workers into free registry slots and wait

[Pool.stop()](Pool.stop.md#pymizu.Pool.stop)  
Orderly shutdown (controller only): broadcast shutdown, cancel

[Pool.destroy()](Pool.destroy.md#pymizu.Pool.destroy)  
Tear down the pool handle immediately, without the shutdown

[Pool.status()](Pool.status.md#pymizu.Pool.status)  
A read-only wire-state snapshot of the pool (dict).

[Pool.dump()](Pool.dump.md#pymizu.Pool.dump)  
A read-only debugging snapshot of the whole pool region (dict).

[Pool.stats()](Pool.stats.md#pymizu.Pool.stats)  
Cumulative per-worker and per-submitter counters since each

[Pool.trace()](Pool.trace.md#pymizu.Pool.trace)  
Register a hook called as `fn(event, id)` at each task

[Pool.__enter__()](Pool.__enter__.md#pymizu.Pool.__enter__)  

[Pool.__exit__()](Pool.__exit__.md#pymizu.Pool.__exit__)  


## Cross-language interop


R peers and workers (the mizu R package), neutral task specifications, and the data.frame's Python home.


[call](call.md#pymizu.call)  
A task specification for a pool of another language's workers.

[r_launcher()](r_launcher.md#pymizu.r_launcher)  
Return a [Channel.create](Channel.create.md#pymizu.Channel.create) launcher spawning an R peer.

[r_pool_launcher()](r_pool_launcher.md#pymizu.r_pool_launcher)  
Return a [Pool.create](Pool.create.md#pymizu.Pool.create) launcher spawning R workers.

[Frame](Frame.md#pymizu.Frame)  
A data.frame's Python home: named columns with a row count.


## Frame Methods


Methods for the Frame class


[Frame.names](Frame.names.md#pymizu.Frame.names)  

[Frame.row_names](Frame.row_names.md#pymizu.Frame.row_names)  

[Frame.to_dict()](Frame.to_dict.md#pymizu.Frame.to_dict)  
to_dict() -\> dict

[Frame.__arrow_c_stream__()](Frame.__arrow_c_stream__.md#pymizu.Frame.__arrow_c_stream__)  
**arrow_c_stream**(requested_schema=None) -\> capsule

[Frame.__len__()](Frame.__len__.md#pymizu.Frame.__len__)  
Return len(self).

[Frame.__reduce__()](Frame.__reduce__.md#pymizu.Frame.__reduce__)  


## Outcomes and errors


Terminal transport states return as the sentinel singletons FULL / TIMEOUT / CLOSED / PEER_GONE (identity-tested, never raised); real failures raise from the exception hierarchy.


[is_sentinel()](is_sentinel.md#pymizu.is_sentinel)  
is_sentinel(x) -\> bool

[is_remote_error()](is_remote_error.md#pymizu.is_remote_error)  
Test whether a received channel value is a remote error.

[MizuError](MizuError.md#pymizu.MizuError)  
Base class for all pymizu errors.

[ShmError](ShmError.md#pymizu.ShmError)  
A shared-memory region operation failed.

[StartupError](StartupError.md#pymizu.StartupError)  
A channel peer or pool worker failed to attach within the startup timeout.

[SubmitTimeoutError](SubmitTimeoutError.md#pymizu.SubmitTimeoutError)  
Pool.submit() timed out waiting for injection-ring space.

[SlotsExhaustedError](SlotsExhaustedError.md#pymizu.SlotsExhaustedError)  
Pool.submit() found no free result slot: too many outstanding (uncollected) tasks.

[StoppedError](StoppedError.md#pymizu.StoppedError)  
The pool is stopped; no further submission is possible.

[CancelledError](CancelledError.md#pymizu.CancelledError)  
The task was cancelled before it ran.

[TaskError](TaskError.md#pymizu.TaskError)  
The task callable raised. Carries 'remote_type' and 'remote_traceback' attributes describing the worker-side exception.

[WorkerDiedError](WorkerDiedError.md#pymizu.WorkerDiedError)  
The executing worker died mid-task. Carries 'slot' and 'pid' attributes identifying the worker.

[DeclinedError](DeclinedError.md#pymizu.DeclinedError)  
A send on a foreign-language channel of a value outside the portable interchange subset. Carries 'path' and 'reason' attributes.


## Housekeeping


The process-wide default pool, the evaluating worker's own pool handle, and orphan reaping.


[default_pool()](default_pool.md#pymizu.default_pool)  
The process-wide default pool, or None when none is set.

[set_default_pool()](set_default_pool.md#pymizu.set_default_pool)  
Set the process-wide default pool; None clears it.

[using_pool()](using_pool.md#pymizu.using_pool)  
Use `pool` as the default for the with block, then restore.

[current_pool()](current_pool.md#pymizu.current_pool)  
The evaluating worker's own pool handle, inside a task.

[prune()](prune.md#pymizu.prune)  
Remove orphaned shared memory regions.
