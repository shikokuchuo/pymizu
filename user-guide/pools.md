# Task pools

A pool is a set of worker processes that share out submitted tasks. [Pool.create()](../reference/pymizu.Pool.create.md) spawns them. Workers pull tasks from per-submitter queues and steal work from each other, so the load balances itself. A submission costs one shared-memory write plus at most one wake-up -- there is no dispatcher process.

``` python
import pymizu

with pymizu.Pool.create(4) as pool:
    task = pool.submit(pow, 2, 16)
    result = task.collect(timeout=5)

result
```

    65536

`pool.submit()` returns a [Task](../reference/Task.md#pymizu.Task) handle immediately, and `task.collect()` waits for the result. The pool is a context manager: leaving the block stops it. `pool.stop()` cancels the pending tasks, waits for the workers to exit cleanly, and releases the shared memory. The pool lives only as long as its creating process; dropping the handle shuts it down without waiting for the workers.

## Task callables

A task callable travels by pickle, so under the standard pickle it must be an importable reference, as with `multiprocessing`; installing cloudpickle lifts that restriction. Task arguments follow the same rules as [channel values](channels.md#what-crosses-a-channel): a buffer-protocol argument larger than 32 KB arrives as a read-only view over the shared pages, not a writable copy.

## Frames as task arguments

A [`Frame`](../reference/Frame.md#pymizu.Frame) argument follows the same rule: past the size floor it crosses as one shared-memory region, and the worker's callable reads it in place, as on a channel. Below the floor it pickles (the worker gets a copy), as it does with a complex column, which has no Arrow type. Only one region crosses per task: a second region-sized argument (another frame or a large buffer) moves the whole task to pickle, values exact. Raw pyarrow and polars frames stay on the pickle path and round-trip with their exact types -- construct the [Frame](../reference/Frame.md#pymizu.Frame) first to use the shared region instead.

## Outcomes

| Outcome | On collect |
|----|----|
| The task raised | [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError), with `remote_type` and `detail` |
| The task was cancelled before it ran | [pymizu.CancelledError](../reference/CancelledError.md#pymizu.CancelledError) |
| The executing worker died | [pymizu.WorkerDiedError](../reference/WorkerDiedError.md#pymizu.WorkerDiedError), with `slot` and `pid` |
| The task's payload region vanished before any worker ran it | [pymizu.TaskLostError](../reference/TaskLostError.md#pymizu.TaskLostError) |
| The collect timeout expired | the `pymizu.TIMEOUT` sentinel |

A task error crosses as a plain description (type, message, traceback), not a pickled exception object. The OS reports a worker's death immediately -- no heartbeats, no polling. Only the tasks the dead worker had claimed fail; the surviving workers take over the rest of its queue.

`task.cancel()` withdraws a task: a queued task is skipped, a running one completes and its result is discarded. Cancellation never interrupts executing code.

Submission itself can fail: [SubmitTimeoutError](../reference/SubmitTimeoutError.md#pymizu.SubmitTimeoutError) when the submit queue stays full past `timeout`, [SlotsExhaustedError](../reference/SlotsExhaustedError.md#pymizu.SlotsExhaustedError) when too many tasks are outstanding and uncollected, [StoppedError](../reference/StoppedError.md#pymizu.StoppedError) on a stopped pool.

## Waiting on several tasks

`pool.collect_any()` waits on a list of task handles and returns `(index, value)` for the first to finish; the other handles stay collectible. `pool.collect_all()` returns every value in input order once all have finished. Both take one overall `timeout` and return the `TIMEOUT` sentinel on expiry, consuming nothing:

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

`pool.submit_batch()` submits one task per zero-argument callable in a single call. Bind arguments with `functools.partial`.

## asyncio

For event-driven collects, `await task` (or `task.collect_async()`) returns an `asyncio.Future` that resolves with the result -- or raises the task's error -- when it arrives, with no blocking and no polling. Cancelling the Future only detaches the waiter: a later blocking `collect()` works again, and the task itself is never cancelled (only `task.cancel()` does that). Each result has one consumer: a task with a pending async collect refuses blocking collects.

`pool.collect_any_async(tasks)` and `pool.collect_all_async(tasks)` are the batch forms: one Future per collection, re-checked whenever a task finishes, otherwise behaving like the sync verbs. Collect-all resolves in input order and rejects the first error by position with its `index`; collect-any resolves `(index, value)` for the first task to finish. Neither takes a timeout -- use `asyncio.timeout` or `asyncio.wait_for`.

``` python
with pymizu.Pool.create(2) as pool:
    tasks = [pool.submit(pow, 2, i) for i in range(4)]
    rest = await pool.collect_all_async(tasks)

rest
```

    [1, 2, 4, 8]

`await pool.map(fn, x)` resolves a map handle the same way (see [Parallel map](map.md)).

## Nested tasks

Inside a task, [pymizu.current_pool()](../reference/current_pool.md#pymizu.current_pool) returns the pool handle of the worker running it. A nested submit lands on that worker's own queue -- no ring, no wake-up -- and a worker waiting on a nested result runs other work instead of sleeping. Recursive fan-out is cheap and cannot deadlock the pool:

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

[Pool.map()](../reference/pymizu.Pool.map.md) nests the same way: a task can map over its own pool. See [Nested maps](map.md#nested-maps).

## A default pool for package code

Package code can read a process-wide default pool with [pymizu.default_pool()](../reference/default_pool.md#pymizu.default_pool). `pymizu.set_default_pool(pool)` sets it (`None` clears it) and returns the previous default, so you can save and restore. A pool set as the default stays alive after its variable is deleted. `pymizu.using_pool(pool)` scopes the default to a `with` block, restoring the previous default on exit, exception included.

Resolve an optional pool in this order: an explicit `pool` argument, [current_pool()](../reference/current_pool.md#pymizu.current_pool) inside a task, [default_pool()](../reference/default_pool.md#pymizu.default_pool), then your own fallback (sequential execution or an error). Inside a task, [current_pool()](../reference/current_pool.md#pymizu.current_pool) takes precedence over the default, so nested submission always goes to the worker's own pool. Setting a default checks only the type: a stopped pool is accepted and fails when used. Handles from [Pool.attach()](../reference/pymizu.Pool.attach.md) are valid defaults.

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

## Growing, shrinking, and joining a pool

A pool can grow and shrink while it runs. `pool.spawn_workers(n)` adds workers, up to the `max_workers` limit set at create. `pool.retire(slot)` asks a worker to exit cleanly: it finishes its current task, and the remaining workers take over anything still queued to it. Only the creator resizes a pool; an attached submitter gets [MizuError](../reference/MizuError.md#pymizu.MizuError).

Other processes join a running pool as submitters with `Pool.attach(token)`, where the token is the creator's `pool.token`. Attached submitters submit and collect exactly as the creator does.

## Observing a pool

Four read-only tools watch a running pool without disturbing it:

``` python
pool.info()   # snapshot: worker states, queued tasks, result slots
pool.stats()    # cumulative counters since each participant joined
pool.dump()     # every slot in full detail; start here when a pool hangs
pool.trace(fn)  # fn(event, id) at each task lifecycle event; None removes it
```

## Threading

A pool handle is single-producer: call its submit family (`submit()`, [submit_batch()](../reference/pymizu.Pool.submit_batch.md), `map()`) from one thread only. A second concurrent producer raises [ConcurrentUseError](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError) instead of corrupting the submission queue. The guard is a check, not a lock: it never waits, it reports the mistake. The waiting half (the blocking collects, `ready_wait()`, [stop()](../reference/pymizu.Pool.stop.md)) likewise belongs to one thread -- which may differ from the producer's -- and a concurrent second wait raises the same error. This is libmizu's threading contract: the shared structures carry no locks by design.

Three ways to produce from several threads, in rough order of preference:

1.  [PoolExecutor](../reference/PoolExecutor.md#pymizu.PoolExecutor): its `submit()` is thread-safe through an internal lock, and it resolves futures as results arrive (see the next section).
2.  Funnel submissions through one thread, for example `run_in_executor` with a single-worker `ThreadPoolExecutor`, while collects stay on their own thread.
3.  `Pool.attach(token)` one handle per thread: each attached handle is its own producer with its own submission queue.

Avoid `asyncio.to_thread(pool.submit, ...)` from concurrent coroutines: the default executor is multi-threaded, so the submits race and raise [ConcurrentUseError](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError). `destroy()` is the one verb that is safe from any thread, as with channels.

## concurrent.futures interop

[pymizu.PoolExecutor](../reference/PoolExecutor.md#pymizu.PoolExecutor) adapts a pool to the stdlib `concurrent.futures.Executor` interface, so code written against `ProcessPoolExecutor` drops in: `submit` returns real `concurrent.futures.Future` objects, and `wait`, `as_completed`, `asyncio.wrap_future`, and chunked `map` all work unchanged. Futures resolve as results arrive, with no background thread and no polling.

``` python
with pymizu.PoolExecutor.create(4) as ex:
    futures = [ex.submit(pow, 2, i) for i in range(4)]
    result = [f.result() for f in futures]

result
```

    [1, 2, 4, 8]

Wrap an existing pool with `PoolExecutor(pool)`; its lifetime stays with its owner. `PoolExecutor.create(workers, **pool_kwargs)` instead spawns an owned pool that [shutdown](../reference/PoolExecutor.md#pymizu.PoolExecutor.shutdown) stops. A task's failure surfaces as [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError), and cancellation behaves like [Task.cancel](../reference/Task.md#pymizu.Task.cancel). If the result table is full, `submit` waits for the executor to collect a result instead of raising [SlotsExhaustedError](../reference/SlotsExhaustedError.md#pymizu.SlotsExhaustedError); a submit from a listener-thread done callback still raises rather than deadlocking the drain. The executor's `map` honors `chunksize` and `buffersize` like `ProcessPoolExecutor`; its default remains one task per element, and [Pool.map](../reference/pymizu.Pool.map.md) remains the faster bulk-map path.

## Sizing a pool

[Pool.create()](../reference/pymizu.Pool.create.md) sizing options: `max_workers` (the worker limit), `max_submitters`, `injection_cap` (each submitter's queue size), `per_worker_cap` (each worker's queue size), `result_slots` (outstanding uncollected tasks per submitter), and `slot_size` (bytes per queue entry and result slot; a larger payload travels in its own shared-memory region).
