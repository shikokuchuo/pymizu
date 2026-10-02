# Pool.stop()


Orderly shutdown (controller only): broadcast shutdown, cancel


Usage

``` python
Pool.stop(timeout=5.0)
```


pending tasks, wait up to `timeout` seconds for clean worker exits, and unlink. Idempotent.
