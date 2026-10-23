# is_remote_error()

Test whether a received channel value is a remote error.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1263-L1277)

``` python
is_remote_error(x)
```

An uncaught error in a channel peer crosses as a value, not a raised exception (transport states are values, payloads are values -- user code decides to raise). The value is a [`TaskError`](../reference/TaskError.md#pymizu.TaskError) carrying the original exception's class name as `remote_type` and its traceback text as `remote_traceback`; raise it to propagate.

## Returns

`True for a received remote error, False otherwise.`  
