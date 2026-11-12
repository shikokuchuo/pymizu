# submit()

Submit `fn(*args, **kwargs)` as a task; return a Task handle.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L861-L943)

``` python
submit(
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

`timeout` belongs to the submission, not to `fn`: a callable taking its own `timeout=` keyword argument cannot receive it through `**kwargs` here. Bind it first with `functools.partial` as `functools.partial(fn, timeout=...)`.

A buffer-protocol argument (for example a numpy array) larger than 32 KB crosses as a read-only view over shared pages, not a writable copy.

A callable that crosses by reference is resolved per task off the worker's imported modules: replacing the function in an already-imported module is picked up on the next submission. Install cloudpickle to send callables by value.

With a [`call`](../reference/call.md#pymizu.call) spec as `fn` (no `*args` or `**kwargs`, since the spec carries them), the task crosses in a language-neutral format. This is how you drive a pool of another language's workers, spawned with [`r_launcher()`](../reference/r_launcher.md#pymizu.r_launcher). On a foreign pool a plain callable raises an error naming [call](../reference/call.md#pymizu.call).

## Examples

See [`Pool.create()`](../reference/pymizu.Pool.create.md) for a submit and collect example.
