# Pool


A shared-memory work-stealing task pool handle (process-private).


Usage

``` python
Pool()
```


Create the controller side with <a href="../reference/Pool.create.html#pymizu.Pool.create" class="gdls-link"><code>Pool.create()</code></a> (which spawns the workers, `python -m pymizu.worker`); other processes join as submitters with <a href="../reference/Pool.attach.html#pymizu.Pool.attach" class="gdls-link"><code>Pool.attach()</code></a>. The pool's lifetime is bound to the creating process: dropping the handle shuts the pool down as <a href="../reference/Pool.stop.html#pymizu.Pool.stop" class="gdls-link"><code>Pool.stop()</code></a> does, but without the wait. Handles do not survive `fork()`.

Task callables ride pickle: under stock pickle a submitted callable must be an importable reference (the multiprocessing constraint); installing cloudpickle lifts that transparently.
