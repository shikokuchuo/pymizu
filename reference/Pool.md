# Pool


A shared-memory work-stealing task pool handle (process-private).


Usage

``` python
Pool()
```


Create the controller side with :meth:`create` (which spawns the workers, `python -m pymizu.worker`); other processes join as submitters with :meth:`attach`. The pool's lifetime is bound to the creating process: dropping the handle shuts the pool down as :meth:[stop](Pool.stop.md#pymizu.Pool.stop) does, but without the wait. Handles do not survive `fork()`.

Task callables ride pickle: under stock pickle a submitted callable must be an importable reference (the multiprocessing constraint); installing cloudpickle lifts that transparently.
