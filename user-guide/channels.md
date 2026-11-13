# Channels

A channel is a two-way message link between your Python process and a helper process it spawns: one lock-free ring per direction, over shared memory. One side writes into shared memory and the other reads it in place -- no copies through sockets, pipes, or files.

## Creating a channel

[Channel.create()](../reference/pymizu.Channel.create.md) spawns the peer process and connects both ends. Pass the peer program as a Python source string; the peer evaluates it with `ch` bound to its side of the channel:

``` python
import pymizu

ch = pymizu.Channel.create("""
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED:
        break
    ch.send(x)
""")
```

The peer is a full Python process: it can import anything your interpreter can. [Channel.create()](../reference/pymizu.Channel.create.md) waits up to `startup_timeout` seconds for the peer to connect and raises [StartupError](../reference/StartupError.md#pymizu.StartupError) on expiry.

A [launcher](../reference/launcher.md#pymizu.launcher) changes how the peer spawns: one callable that takes the channel's token and starts the peer process. The built-in [pymizu.r_launcher()](../reference/r_launcher.md#pymizu.r_launcher) spawns an R peer (see [R interop](interop.md)); write your own to spawn peers any other way.

## Sending and receiving

`ch.send()` publishes a value, and the peer can see it as soon as the call returns. Sends never block for ring space. `ch.recv()` returns the next value, waiting up to `timeout` seconds (indefinitely when omitted):

``` python
ch.send([1, "a", None])
ch.recv(timeout=5)
```

    [1, 'a', None]

Outcomes that end a conversation come back as sentinel singletons, not exceptions:

| Sentinel           | Returned when                                     |
|--------------------|---------------------------------------------------|
| `pymizu.FULL`      | a send finds the ring full                        |
| `pymizu.TIMEOUT`   | a receive's timeout expires                       |
| `pymizu.CLOSED`    | the peer has closed and the ring is drained       |
| `pymizu.PEER_GONE` | the peer process has died and the ring is drained |

Test for them by identity (`x is pymizu.TIMEOUT`) or with `pymizu.is_sentinel(x)` -- no call needs an error handler. If the peer dies, receives drain what it already sent, then report `PEER_GONE`.

When per-call overhead starts to matter, [send_batch()](../reference/pymizu.Channel.send_batch.md) and [recv_batch()](../reference/pymizu.Channel.recv_batch.md) move several values in one call:

``` python
ch.send_batch(range(4))
ch.recv_batch(4, timeout=5)
```

    [0, 1, 2, 3]

`ch.alive()` reports whether the peer still runs, without touching the rings. `ch.info()` returns a read-only snapshot of the channel's state.

## asyncio

For event-driven receives, `ch.recv_async()` returns an `asyncio.Future` that resolves with the next value when it arrives -- no blocking, no polling. It takes no timeout; use `asyncio.timeout` or `asyncio.wait_for`. Terminal states stay values: the Future resolves with `CLOSED` or `PEER_GONE`. Outstanding calls resolve in call order, one message each. Cancelling a Future detaches its waiter and passes the next message to the one behind it. When arrivals come in bursts, `ch.recv_batch_async(n)` resolves one Future with up to `n` values as one list -- short when fewer have arrived, never waiting to fill -- the high-throughput receive form.

``` python
ch.send(1)
assert await ch.recv_async() == 1
ch.send(2)
await ch.recv_async()
```

    2

``` python
ch.send_batch([3, 4])
await ch.recv_batch_async()
```

    [3, 4]

`async for x in ch` is the stream form: it awaits each value through [recv_async()](../reference/pymizu.Channel.recv_async.md) and ends on `CLOSED` or `PEER_GONE`, as the sync iterator does.

A message outside the portable interchange subset (a language-private stream from the peer) is declined at read time with [ForeignPayloadError](../reference/ForeignPayloadError.md#pymizu.ForeignPayloadError); the message is consumed and the channel keeps flowing. Under [recv_async()](../reference/pymizu.Channel.recv_async.md), only that message's waiter rejects. The rejection also ends an `async for` loop, and a try/except inside the loop body cannot catch it -- wrap the whole loop and re-enter on the same channel to skip the message.

## Closing

`ch.close()` is the orderly shutdown: it tells the peer to close and waits for it to finish reading before releasing the shared memory:

``` python
ch.close()
```

    True

A channel is also a context manager: the `with` block closes it on exit. `destroy()` skips the wait and releases the handle immediately; the peer sees the channel as closed (`PEER_GONE` if it died hard). Destroy is safe from any thread: a verb still running on another thread keeps the handle alive until it returns, and a waiting verb ([recv()](../reference/pymizu.Channel.recv.md), [recv_batch()](../reference/pymizu.Channel.recv_batch.md), `ready_wait()`, `close()`) wakes promptly so teardown can finish.

## Threading

Each channel side belongs to one thread at a time -- libmizu's threading contract, since the shared structures carry no locks by design. Call a side's verbs ([send()](../reference/pymizu.Channel.send.md), [recv()](../reference/pymizu.Channel.recv.md), their batch forms, `ready_wait()`) from one thread only; a second verb from another thread raises [ConcurrentUseError](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError) instead of corrupting the ring. The guard is a check, not a lock: it never waits, it reports the mistake. Blocking receives and [recv_async()](../reference/pymizu.Channel.recv_async.md) are likewise exclusive on a side: a blocking receive while async receives are pending raises [ConcurrentUseError](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError) at the call site, except on the async consumer's own event-loop thread, whose receives can never race it. For two-way traffic from two threads, use two channels (one per direction, each on its own thread) or route every call through a single thread. `destroy()` is the exception: it is safe from any thread, as described in Closing.

## What crosses a channel

pymizu chooses the cheapest transport for each value:

1.  `None` crosses as a marker: no data moves.
2.  Buffer-protocol objects (`bytes`, 1-D C-contiguous numpy arrays) cross without serialization. float64, int32, int64, complex128, and uint8 keep their exact values and arrive as arrays (`bytes` arrives as a uint8 array). numpy scalars and 0-d arrays keep their exact type and shape. A large buffer arrives as a read-only zero-copy view over the shared pages, with no copy and no parse. A contiguous buffer below 256 KB instead crosses as one copy into shared memory, which is cheaper than a separate region at those sizes. From 256 KB the receive is a view.
3.  A `str` that fits in a slot crosses as raw UTF-8.
4.  Booleans, numbers, and nested lists, tuples, and dicts of them cross in a compact binary form. A payload that references the same list or dict more than once crosses as a pickle instead, which keeps the sharing.
5.  Everything else crosses as a pickle (protocol 4).

Each end of a channel knows the peer's language. Between two Python processes it changes nothing: arrays of the listed dtypes cross unchanged, and everything else pickles. An R peer restricts values to a portable subset both languages share; a value outside it raises [pymizu.DeclinedError](../reference/DeclinedError.md#pymizu.DeclinedError) at send time, naming the value and the reason, and the channel stays usable. See [R interop](interop.md).

An uncaught error in the peer crosses as a value, not a raised exception: a [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError) carrying the original class name and traceback text. Test a received value with `pymizu.is_remote_error(x)`; raise it to propagate the error.

## Frames without pickling

Between two Python processes, a pyarrow `Table`, polars `DataFrame`, or duckdb relation pickles by default -- type-exact, but a full serialize-and-parse round trip. [pymizu.Frame.from_arrow()](../reference/Frame.md#pymizu.Frame.from_arrow) sends an Arrow producer as a frame instead: one shared-memory region, read in place at receive. Here the peer sums a 200,000-row column straight off the shared pages -- only the small result crosses back:

``` python
import pyarrow as pa

with pymizu.Channel.create("""
import pymizu
import pyarrow as pa
import pyarrow.compute as pc

while True:
    f = ch.recv()
    if f is pymizu.CLOSED:
        break
    t = pa.table(f)
    ch.send(pc.sum(t.column("x")).as_py())
""") as ch:
    f = pymizu.Frame.from_arrow(
        pa.table({"x": pa.array([1.5, None] * 100_000)})
    )
    ch.send(f)
    result = ch.recv(timeout=5)
result
```

    150000.0

The peer's `pa.table(f)` borrows the region's buffers, so the sum runs where the data already lives.

Construction adopts the producer's own buffers wherever the column layout is already the Arrow layout: a single-batch producer on offset-0 boundaries, nulls included (the validity bitmap is adopted with the values). It copies only what it must:

| Stage | Copies |
|----|----|
| producer → [Frame.from_arrow()](../reference/Frame.md#pymizu.Frame.from_arrow) | 0 per adopted column; a per-column copy only where its type forces it |
| `ch.send(f)` | 1 -- the layout write into the shared region (the region is the wire) |
| `ch.recv()` | 0 -- the frame reads the region in place |
| `pa.table(f)` | 0 -- the frame's [\_\_arrow_c_stream\_\_](../reference/Frame.md#pymizu.Frame.__arrow_c_stream__) exports borrowed buffers |

Values cross exactly, but types normalize. A column whose type forces a copy is copied rather than declined:

| In | Out (the received frame's Arrow export) |
|----|----|
| `string` | `large_string` |
| `string_view` | `large_string` (copies) |
| `bool` | `bool` (copies: bit-packed) |
| int8 / int16 / uint16 | int32 (copies) |
| uint32 / half_float / float32 | float64 (copies) |
| uint64 | uint64 (adopts) |
| timestamp\[s / ms / us / ns\] | timestamp\[s / ms / us / ns\] (adopts) |
| duration\[s / ms / us / ns\] | duration\[s / ms / us / ns\] (adopts) |
| dictionary\<i32, utf8\> | dictionary\<i32, string\> (adopts; other index widths copy) |
| chunked or sliced columns | concatenated or rebased (copies) |

Chunked pyarrow tables adopt after `table.combine_chunks()`. Timestamps and durations keep their counts and unit: a `timestamp[ns]` round trip is exact to the nanosecond, timezone included. Nanosecond precision needs this pymizu version on both ends; an older peer (or R) receives the microsecond form instead, values rounded to the wire format's microsecond precision, with no error.

Adopted buffers stay alive for the frame's lifetime: a frame built from a 4 GB table keeps the whole table in memory, like a numpy view. [to_dict()](../reference/Frame.md#pymizu.Frame.to_dict)'s masked reads copy where the Arrow export does not. When you want the exact producer type back, keep the default: a plain pyarrow or polars object pickles and comes back type-exact. On a pool, a [Frame](../reference/Frame.md#pymizu.Frame) task argument and a frame returned from a task both ride the shared-memory region as on a channel (see [the pools guide](pools.md#frames-as-task-arguments)). [Pool.map](../reference/pymizu.Pool.map.md) does not accept a frame as its element source.

## Sizing

[Channel.create()](../reference/pymizu.Channel.create.md) takes `capacity` (ring slots, a power of two), `slot_size` (bytes per slot, so a payload of up to `slot_size - 16` bytes fits in a slot), and `arena_size` (the channel's shared staging area). A value too large for a slot goes to the staging area or a fresh shared-memory region. `spin=True` makes receivers busy-wait instead of sleeping: the lowest latency, at full CPU while waiting.
