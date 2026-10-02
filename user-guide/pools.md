# Task pools

A pool is a set of worker processes that divide submitted tasks among themselves. [Pool.create()](../reference/Pool.create.md#pymizu.Pool.create) spawns them (`python -m pymizu.worker <token> <slot>`); workers claim tasks from per-submitter injection rings and steal work from each other. A submission is one shared-memory write plus at most one directed wake: no dispatcher process is in the loop.


``` python
import pymizu

with pymizu.Pool.create(4) as pool:
    task = pool.submit(pow, 2, 16)
    result = task.collect(timeout=5)

result
```


    65536


`pool.submit()` returns a [Task](../reference/Task.md#pymizu.Task) handle immediately; `task.collect()` waits for the result. The pool is a context manager: leaving the block stops it. `pool.stop()` cancels the pending tasks, waits for the workers to exit cleanly, and releases the shared region. The pool's lifetime is bound to the creating process -- dropping the handle shuts the pool down without the wait.


# Task callables

A task callable rides pickle: under stock pickle it must be an importable reference (the multiprocessing constraint); installing cloudpickle lifts that transparently. Task arguments cross by the same rules as [channel payloads](channels.md#what-crosses-a-channel): a buffer-protocol argument past the zero-copy floor arrives as a read-only view over shared pages, not a writable copy.


# Outcomes

| Outcome | On collect |
|----|----|
| The task raised | [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError), with `remote_type` and `remote_traceback` |
| The task was cancelled before it ran | [pymizu.CancelledError](../reference/CancelledError.md#pymizu.CancelledError) |
| The executing worker died | [pymizu.WorkerDiedError](../reference/WorkerDiedError.md#pymizu.WorkerDiedError), with `slot` and `pid` |
| The collect timeout expired | the `pymizu.TIMEOUT` sentinel |

A task error crosses as a constructed, bounded envelope -- never a pickled exception instance. A worker's death is detected at OS notification latency, with no heartbeats or polling; it fails exactly the tasks the worker had claimed, and the surviving workers consume the rest of its queue.

`task.cancel()` withdraws a task: still queued, it is skipped; already running, it completes and its result is discarded. Cancellation never interrupts executing code.

Submission itself can fail: [SubmitTimeoutError](../reference/SubmitTimeoutError.md#pymizu.SubmitTimeoutError) when the injection ring stays full past `timeout`, [SlotsExhaustedError](../reference/SlotsExhaustedError.md#pymizu.SlotsExhaustedError) when the submitter's result slots are all outstanding, [StoppedError](../reference/StoppedError.md#pymizu.StoppedError) on a stopped pool.


# Waiting on several tasks

`pool.collect_any()` waits on a list of task handles and returns `(index, value)` for the first to reach a terminal state -- the other handles stay collectible. `pool.collect_all()` returns every value in input order once all are terminal. Both take one overall `timeout` and return the `TIMEOUT` sentinel on expiry, consuming nothing:


``` python
import time

with pymizu.Pool.create(2) as pool:
    tasks = [pool.submit(time.sleep, 0.5), pool.submit(len, "pymizu")]
    first = pool.collect_any(tasks, timeout=5)

first
```


    (1, 6)


``` python
with pymizu.Pool.create(4) as pool:
    tasks = [pool.submit(pow, 2, i) for i in range(4)]
    rest = pool.collect_all(tasks, timeout=5)

rest
```


    [1, 2, 4, 8]


`pool.submit_batch()` submits one task per zero-arg callable in a single crossing -- bind arguments with `functools.partial`.


# Nested tasks

Inside a task, [pymizu.current_pool()](../reference/current_pool.md#pymizu.current_pool) returns the evaluating worker's own handle. A nested submit pushes straight onto the worker's own work-stealing deque -- no ring, no wake. A worker waiting on a nested result executes other work instead of sleeping, so divide-and-conquer runs at fork/join cost and never deadlocks the pool:


``` python
def fan_out(parts):
    pool = pymizu.current_pool()
    subs = [pool.submit(sum, part) for part in parts]
    return sum(t.collect() for t in subs)

parts = [range(0, 250), range(250, 500), range(500, 750), range(750, 1000)]

with pymizu.Pool.create(4) as pool:
    total = pool.submit(fan_out, parts).collect()

total
```


    499500


(This example rides cloudpickle -- `fan_out` is not an importable reference.)


# Growing, shrinking, and joining a pool

A pool can grow and shrink while it runs. `pool.spawn_workers(n)` adds workers into free registry slots (size the ceiling with `max_workers` at create). `pool.retire(slot)` asks a worker to exit cleanly: it finishes its current task, and the remaining workers consume anything still queued to it.

Other processes join a running pool as submitters with `Pool.attach(token)` -- the token is `pool.token` on the creator -- and submit and collect exactly as the creator does.


# Observing a pool

Four read-only tools watch a running pool without disturbing it:

``` python
pool.status()   # snapshot: worker states, queued tasks, result slots
pool.stats()    # cumulative counters since each participant joined
pool.dump()     # every slot in full detail -- the first tool when a pool hangs
pool.trace(fn)  # fn(event, id) at each task lifecycle event; None removes it
```


# Sizing a pool

[Pool.create()](../reference/Pool.create.md#pymizu.Pool.create) knobs: `max_workers` (the registry ceiling), `max_submitters`, `injection_cap` (each submitter's ring), `per_worker_cap` (each worker's deque), `result_slots` (outstanding uncollected tasks per submitter), and `slot_size` (bytes per queue entry and result slot -- a payload past the inline budget travels in a fresh region).
