# Task pools

A pool is a set of worker processes that divide submitted tasks among themselves. [Pool.create()](../reference/Pool.create.md#pymizu.Pool.create) spawns them. Workers claim tasks from per-submitter injection rings and steal work from each other, so the load balances itself. A submission is one shared-memory write plus at most one directed wake: no dispatcher process is in the loop.


``` python
import pymizu

with pymizu.Pool.create(4) as pool:
    task = pool.submit(pow, 2, 16)
    result = task.collect(timeout=5)

result
```


    65536


`pool.submit()` returns a [Task](../reference/Task.md#pymizu.Task) handle immediately; `task.collect()` waits for the result. The pool is a context manager: leaving the block stops it. `pool.stop()` cancels the pending tasks, waits for the workers to exit cleanly, and releases the shared memory. The pool's lifetime is bound to the creating process -- dropping the handle shuts the pool down without the wait.


# Task callables

A task callable travels by pickle. With the standard pickle it must be an importable reference, as with `multiprocessing`. Installing cloudpickle lifts that restriction. Task arguments follow the same rules as [channel values](channels.md#what-crosses-a-channel). A buffer-protocol argument past the zero-copy floor (a fixed size threshold) arrives as a read-only view over the shared pages, not a writable copy.


# Frames as task arguments

A <a href="../reference/Frame.html#pymizu.Frame" class="gdls-link"><code>Frame</code></a> argument follows the same rule: past the zero-copy floor it crosses as one shared-memory region, and the worker's callable receives a region-backed frame, exactly as on a channel. Below the floor the argument pickles (the worker gets a copy-backed frame), as it does with a complex column (which has no Arrow type). One region crosses per task: a second region-sized argument -- another frame or a large buffer -- sends the whole task down the pickle path, values exact. Raw pyarrow and polars frames keep the pickle path (they round-trip container-exact); construct the [Frame](../reference/Frame.md#pymizu.Frame) first to opt in.


# Outcomes

| Outcome | On collect |
|----|----|
| The task raised | [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError), with `remote_type` and `remote_traceback` |
| The task was cancelled before it ran | [pymizu.CancelledError](../reference/CancelledError.md#pymizu.CancelledError) |
| The executing worker died | [pymizu.WorkerDiedError](../reference/WorkerDiedError.md#pymizu.WorkerDiedError), with `slot` and `pid` |
| The collect timeout expired | the `pymizu.TIMEOUT` sentinel |

A task error crosses as a plain description of type, message, and traceback -- never a pickled exception object. A worker's death is detected at OS notification latency, with no heartbeats or polling. A dead worker fails exactly the tasks it had claimed. The surviving workers take over the rest of its queue.

`task.cancel()` withdraws a task. A task still queued is skipped. A task already running completes, and its result is discarded. Cancellation never interrupts executing code.

Submission itself can fail: [SubmitTimeoutError](../reference/SubmitTimeoutError.md#pymizu.SubmitTimeoutError) when the submit queue stays full past `timeout`, [SlotsExhaustedError](../reference/SlotsExhaustedError.md#pymizu.SlotsExhaustedError) when too many tasks are outstanding and uncollected, [StoppedError](../reference/StoppedError.md#pymizu.StoppedError) on a stopped pool.


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


`pool.submit_batch()` submits one task per zero-argument callable in a single call -- bind arguments with `functools.partial`.


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


(This example needs cloudpickle: `fan_out` is not an importable reference.)

[Pool.map()](../reference/Pool.map.md#pymizu.Pool.map) nests the same way: a task can map over its own pool -- see [Nested maps](map.md#nested-maps).


# A default pool for package code

Package code can read a process-wide default pool with [pymizu.default_pool()](../reference/default_pool.md#pymizu.default_pool). `pymizu.set_default_pool(pool)` sets it -- `None` clears it -- and returns the previous default, so callers can save and restore. The registry anchors the handle: a pool set as the default stays alive even after its variable is deleted. `pymizu.using_pool(pool)` scopes the default to a with block, restoring the previous default on exit, including on exception.

Resolve an optional pool in this order: an explicit `pool` argument, [current_pool()](../reference/current_pool.md#pymizu.current_pool) inside a task, [default_pool()](../reference/default_pool.md#pymizu.default_pool), then your own fallback -- sequential execution or an error. The default never overrides [current_pool()](../reference/current_pool.md#pymizu.current_pool): inside a task the worker's own handle wins, so nested submission is never shadowed. Setting a default checks the type only -- a stopped pool is accepted and fails at use time -- and handles from [Pool.attach()](../reference/Pool.attach.md#pymizu.Pool.attach) are valid defaults.


``` python
def run(x, pool=None):
    pool = pool or pymizu.current_pool() or pymizu.default_pool()
    if pool is None:
        raise RuntimeError("no pool: pass one, or set a default")
    return pool.submit(pow, x, 2).collect(timeout=30)

with pymizu.Pool.create(2) as pool:
    old = pymizu.set_default_pool(pool)
    squared = run(21)
    pymizu.set_default_pool(old)

squared
```


    441


# Growing, shrinking, and joining a pool

A pool can grow and shrink while it runs. `pool.spawn_workers(n)` adds workers, up to the `max_workers` limit set at create. `pool.retire(slot)` asks a worker to exit cleanly: it finishes its current task, and the remaining workers take over anything still queued to it.

Other processes join a running pool as submitters with `Pool.attach(token)` -- the token is `pool.token` on the creator -- and submit and collect exactly as the creator does.


# Observing a pool

Four read-only tools watch a running pool without disturbing it:

``` python
pool.status()   # snapshot: worker states, queued tasks, result slots
pool.stats()    # cumulative counters since each participant joined
pool.dump()     # every slot in full detail -- the first tool when a pool hangs
pool.trace(fn)  # fn(event, id) at each task lifecycle event; None removes it
```


# concurrent.futures interop

[pymizu.PoolExecutor](../reference/PoolExecutor.md#pymizu.PoolExecutor) adapts a pool to the stdlib `concurrent.futures.Executor` interface, so code written against `ProcessPoolExecutor` drops in: `submit` returns real `concurrent.futures.Future` objects, and `wait` / `as_completed` / `asyncio.wrap_future` / the base `Executor.map` all work unchanged.


``` python
with pymizu.PoolExecutor.create(4) as ex:
    futures = [ex.submit(pow, 2, i) for i in range(4)]
    result = [f.result() for f in futures]

result
```


    [1, 2, 4, 8]


Wrap an existing pool with `PoolExecutor(pool)` -- its lifetime then stays with its owner; `PoolExecutor.create(workers, **pool_kwargs)` spawns an owned pool that `shutdown` stops. A task's failure surfaces as [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError), and cancellation is advisory, mirroring [Task.cancel](../reference/Task.md#pymizu.Task.cancel). For bulk maps, [Pool.map](../reference/Pool.map.md#pymizu.Pool.map) remains the faster path -- the executor's [map](../reference/Pool.map.md#pymizu.Pool.map) runs one task per element.


# Sizing a pool

[Pool.create()](../reference/Pool.create.md#pymizu.Pool.create) sizing options: `max_workers` (the worker limit), `max_submitters`, `injection_cap` (each submitter's injection ring), `per_worker_cap` (each worker's deque), `result_slots` (outstanding uncollected tasks per submitter), and `slot_size` (bytes per queue entry and result slot -- a payload past the inline budget travels in a fresh shared-memory region).
