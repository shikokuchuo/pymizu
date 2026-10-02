# Pool.submit()


Submit `fn(*args, **kwargs)` as a task; return a Task handle.


Usage

``` python
Pool.submit(
    fn,
    /,
    *args,
    timeout=None,
    **kwargs,
)
```


Blocks only for injection-ring space, up to `timeout` seconds (None waits indefinitely): SubmitTimeoutError on expiry, SlotsExhaustedError / StoppedError on the fatal outcomes.

`timeout` belongs to the submission, not to `fn`: a callable taking its own `timeout=` keyword argument cannot receive it through `**kwargs` here -- bind it first with `functools.partial(fn, timeout=...)`.

A buffer-protocol argument (e.g. a numpy array) past the zero-copy floor crosses as a read-only view over shared pages, not a writable copy.

With a :class:[pymizu.call](call.md#pymizu.call) spec as `fn` (no `*args` / `**kwargs` -- the spec carries them), the task stream crosses in the neutral interchange format: this is how a pool of another language's workers is driven (spawn them with :func:[pymizu.r_pool_launcher](r_pool_launcher.md#pymizu.r_pool_launcher)). On a foreign pool a plain callable errors locally, naming the spec verb.
