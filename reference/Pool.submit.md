# Pool.submit()

Submit `fn(*args, **kwargs)` as a task; return a Task handle.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L765-L843)

``` python
Pool.submit(
    fn,
    /,
    *args,
    timeout=None,
    **kwargs,
)
```

Blocks only for queue space, up to `timeout` seconds (None waits indefinitely).

## Parameters

`fn: _Callable[…, _Any]`  
The callable to run, or a [`call`](../reference/call.md#pymizu.call) spec.

`args: _Any = ()`  
Positional arguments for `fn`.

`timeout: float | None = None`  
Seconds to wait for queue space; None waits indefinitely.

`kwargs: _Any = {}`  
Keyword arguments for `fn`.

## Returns

`A Task handle.`  

## Raises

`SubmitTimeoutError`  
On `timeout` expiry.

`SlotsExhaustedError`  
When no result slot is free.

`StoppedError`  
When the pool is stopped.

`ConcurrentUseError`  
Another submit-family verb is in flight on this handle from a second thread: a pool handle is single-producer (the class docstring).

## Details

`timeout` belongs to the submission, not to `fn`: a callable taking its own `timeout=` keyword argument cannot receive it through `**kwargs` here -- bind it first with `functools.partial` as `functools.partial(fn, timeout=...)`.

A buffer-protocol argument (for example a numpy array) larger than 32 KB crosses as a read-only view over shared pages, not a writable copy.

With a [`call`](../reference/call.md#pymizu.call) spec as `fn` (no `*args` / `**kwargs` -- the spec carries them), the task crosses in a language-neutral format: this is how you drive a pool of another language's workers (spawn them with [`r_pool_launcher()`](../reference/r_pool_launcher.md#pymizu.r_pool_launcher)). On a foreign pool a plain callable raises an error naming [call](../reference/call.md#pymizu.call).

## Examples

See [`Pool.create()`](../reference/Pool.create.md#pymizu.Pool.create) for a submit and collect example.
