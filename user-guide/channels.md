# Channels

A channel is a two-way message link between a Python process and a helper process that it spawns: one lock-free ring per direction, over shared memory. One process writes data and the other reads it in place -- never copied through a socket, pipe, or file.


# Creating a channel

[Channel.create()](../reference/Channel.create.md#pymizu.Channel.create) spawns the peer process and connects both ends. The peer program is a Python source string, evaluated in the peer with `ch` bound to its side of the channel:


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


The peer is a full Python process: it imports anything importable in the spawning interpreter's environment. [Channel.create()](../reference/Channel.create.md#pymizu.Channel.create) returns once the peer has connected (up to `startup_timeout` seconds, raising [StartupError](../reference/StartupError.md#pymizu.StartupError) otherwise).

A `launcher` changes how the peer is spawned. It is one callable that takes the channel's token and arranges for the peer process to start. [pymizu.r_launcher()](../reference/r_launcher.md#pymizu.r_launcher) is the built-in example: it spawns an R peer instead (see [R interop](interop.md)). User code spawns peers through a launcher. `Channel.attach(token)` is the low-level entry for a peer that attaches itself.


# Sending and receiving

`ch.send()` publishes a value, visible to the peer the moment the call returns. Sends never block for ring space. `ch.recv()` returns the next value, waiting up to `timeout` seconds (indefinitely when omitted):


``` python
ch.send([1, "a", None])
ch.recv(timeout=5)
```


    [1, 'a', None]


Outcomes that end a conversation -- ring full, timeout, orderly close, peer death -- come back as sentinel singletons, never raised:

| Sentinel           | Returned when                                     |
|--------------------|---------------------------------------------------|
| `pymizu.FULL`      | a send finds the ring full                        |
| `pymizu.TIMEOUT`   | a receive's timeout expires                       |
| `pymizu.CLOSED`    | the peer has closed and the ring is drained       |
| `pymizu.PEER_GONE` | the peer process has died and the ring is drained |

Test them by identity -- `x is pymizu.TIMEOUT` -- or with `pymizu.is_sentinel(x)`. No call needs an error handler. If the peer dies, receives first drain what it already sent, then report `PEER_GONE`.

[send_batch()](../reference/Channel.send_batch.md#pymizu.Channel.send_batch) and [recv_batch()](../reference/Channel.recv_batch.md#pymizu.Channel.recv_batch) move several values in one call. Use them at rates where the per-call overhead starts to matter:


``` python
ch.send_batch(range(4))
ch.recv_batch(4, timeout=5)
```


    [0, 1, 2, 3]


`ch.alive()` reports whether the peer still runs, without touching the rings. `ch.info()` returns a read-only snapshot of the channel's state.


# Closing

`ch.close()` is the orderly shutdown: it signals the peer and waits for it to finish draining before releasing the shared resources:


``` python
ch.close()
```


    True


A channel is also a context manager -- the `with` block closes it on exit. [close_signal()](../reference/Channel.close_signal.md#pymizu.Channel.close_signal) signals without waiting. `destroy()` tears the handle down immediately, and the peer sees `PEER_GONE`.


# What crosses a channel

pymizu moves each value the cheapest way it can:

1.  `None` crosses as a marker -- no data moves.
2.  Buffer-protocol objects (`bytes`, 1-D C-contiguous numpy arrays) cross without serialization. float64, int32, int64, complex128, and uint8 keep their exact values and arrive as arrays (`bytes` arrives as a uint8 array). A large buffer arrives as a read-only zero-copy view over the shared pages -- no copy, no parse.
3.  A `str` within the inline budget crosses as raw UTF-8.
4.  Booleans, numbers, and flat lists or dicts of them cross in a compact binary form.
5.  Everything else crosses as a pickle (protocol 4).

Each end of a channel knows the peer's language. Between two Python processes that is the whole story: arrays of the dtypes above cross unchanged, and everything else pickles. With an R peer, values must fit a portable subset that both languages share. A value outside it raises [pymizu.DeclinedError](../reference/DeclinedError.md#pymizu.DeclinedError) at send time, naming the value and the reason -- the channel is unharmed. See [R interop](interop.md).

An uncaught error in the peer crosses as a value, not a raised exception: a [pymizu.TaskError](../reference/TaskError.md#pymizu.TaskError) carrying the original exception's class name and traceback text. `pymizu.is_remote_error(x)` tests a received value. Raise it to propagate the error.


# Sizing

[Channel.create()](../reference/Channel.create.md#pymizu.Channel.create) takes `capacity` (ring slots, a power of two), `slot_size` (bytes per slot -- the inline payload budget is `slot_size - 16`), and `arena_size` (the channel's spill arena). A value past the inline budget spills to the arena or a fresh shared-memory region. `spin=True` selects pure-spin waiting: receivers never park to the OS -- the lowest latency, at full CPU use while waiting.
