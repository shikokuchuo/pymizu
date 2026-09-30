"""Importable task callables for the pool tests.

Task callables ride pickle by reference under stock pickle, so the worker
processes must import them: they live here (``tests.helpers``) rather than
in the test module. The repo-root conftest.py puts the root on sys.path in
the test process; a spawned worker's sys.path[0] is the repo root already.
"""

import os
import random
import sys
import threading
import time

import pymizu

try:
    import numpy as np
except ImportError:
    np = None  # test_no_numpy's shim: this module must still import


def square(x):
    return x * x


def identity(x):
    return x


def busy(i):
    time.sleep(0.05)
    return i * 2


def raise_long(msg):
    raise ValueError(msg)


class Unpicklable:
    def __reduce__(self):
        raise TypeError("cannot pickle Unpicklable")


def _fail_on_unpickle():
    raise ValueError("pymizu: test unpickle failure")


class FailOnUnpickle:
    """A deterministic read decline: unpickling raises a plain Exception on
    the host — the content failure a channel read consumes. Pickler-
    agnostic: the reconstruction callable is importable, so stock pickle
    and cloudpickle both store it by reference."""

    def __reduce__(self):
        return (_fail_on_unpickle, ())


class ExitOnUnpickle:
    """A deterministic control-flow decline: unpickling calls sys.exit(0),
    raising SystemExit on the host — the BaseException tier a channel read
    must never consume (the slot is kept, so the failure reproduces)."""

    def __reduce__(self):
        return (sys.exit, (0,))


def make_unpicklable():
    return Unpicklable()


def big_array(n):
    """A large vector: crosses back as a zero-copy view (SHM_VEC)."""
    import numpy as np

    return np.arange(n, dtype=np.float64)


def ret_int64_array():
    """A numpy int64 array: an identity dtype for the raw tiers (pool
    results ride the memcpy raw tier — the conversion pass is
    channel-scoped, and int64 no longer needs it)."""
    import numpy as np

    return np.array([1, 2, 3], dtype=np.int64)


def ret_arrow_nulls():
    """A pyarrow array with a null (pool results keep the pickle path)."""
    import pyarrow as pa

    return pa.array([1, None, 3], type=pa.int32())


def array_sum(a):
    return float(a.sum())


def fanout(n):
    """Nested fan-out: submit n subtasks on the worker's own pool handle
    and collect them (nested collect helps instead of parking)."""
    pool = pymizu.current_pool()
    tasks = [pool.submit(square, i) for i in range(n)]
    return sum(t.collect() for t in tasks)


def rand_elt(i):
    """A draw from the worker's stdlib random stream (seeded maps)."""
    return random.random()


def fail_at(i, bad):
    if i == bad:
        raise ValueError(f"element {i}")
    return i


def sleep_then(i, t):
    time.sleep(t)
    return i


def kill_at(i, bad):
    """Die mid-map (the worker process exits without publishing)."""
    if i == bad:
        os._exit(1)
    return i


def nested_map(n):
    """A map inside a task, on the evaluating worker's own pool handle."""
    pool = pymizu.current_pool()
    return pool.map(square, list(range(n)))


def fail_or_sleep(i, bad, t):
    if i == bad:
        raise ValueError(f"element {i}")
    time.sleep(t)
    return i


def sleep_ident(t):
    time.sleep(t)
    return t


def takes_timeout(*, timeout=None):
    """A callable with its own ``timeout=`` kwarg: it collides with the
    submit keyword, so it must ride in via functools.partial."""
    return timeout


def warn_then(x):
    """A warning passes through: it never fails the task."""
    import warnings

    warnings.warn(f"warn for {x}", stacklevel=2)
    return x * 2


def scale_add(x, *, scale=1, add=0):
    return x * scale + add


def np_scalar_double(v):
    return float(v) * 2


def fanout_with_thread(n, interval=0.005):
    """Fan out while a background thread runs in the worker: its beats must
    keep advancing through the nested-collect waits (the park hook drops
    the GIL around each bounded sleep)."""
    beats = []
    stop = threading.Event()

    def beat():
        while not stop.is_set():
            beats.append(time.monotonic())
            time.sleep(interval)

    t = threading.Thread(target=beat)
    t.start()
    try:
        out = fanout(n)
    finally:
        stop.set()
        t.join()
    return out, len(beats)


class StrSubclass(str):
    """A str subclass: staging must keep its pickle semantics (the exact-type
    checks route it past STR1 and the codec)."""


if np is not None:

    class CarryArray(np.ndarray):
        """An ndarray subclass carrying an attribute through pickle (the
        canonical subclass recipe): the raw buffer tier would stage the
        base buffer only, losing the class itself."""

        def __new__(cls, data, tag=None):
            obj = np.asarray(data).view(cls)
            obj.tag = tag
            return obj

        def __array_finalize__(self, obj):
            self.tag = getattr(obj, "tag", None)

        def __reduce__(self):
            func, args, state = super().__reduce__()
            return func, args, state + (self.__dict__,)

        def __setstate__(self, state):
            self.__dict__.update(state[-1])
            super().__setstate__(state[:-1])


def echo(*a, **k):
    return a, k


def is_readonly(a):
    """True when the argument arrived as a read-only zero-copy view."""
    return not a.flags.writeable


def sum_mixed(xs):
    """A flat container with a buffer leaf: an array and a scalar."""
    return float(xs[0].sum()) + xs[1]


def pair_up(v):
    """A length-2 float64 result per element (template maps, m = 2)."""
    import numpy as np

    return np.array([v, v * 2], dtype=np.float64)


def pair_up_i64(v):
    """A length-2 int64 result per element (template maps, m = 2)."""
    import numpy as np

    return np.array([v, v * 2], dtype=np.int64)


def scalar_double(v):
    """A plain Python scalar per element (template maps, m = 1)."""
    return float(v) * 2


def bad_template(v):
    """A result that violates the template (wrong length)."""
    return [1.0, 2.0, 3.0]


def huge_int(v):
    """An int past INT64_MAX (int64 template mismatch)."""
    return 1 << 63


def trace_to_file(path):
    """Install a trace hook appending events to a file, on the worker's own
    pool handle (worker-side tracing must be installed from a task)."""
    import pymizu

    def hook(ev, tid):
        with open(path, "a") as f:
            f.write(f"{ev} {tid}\n")

    pymizu.current_pool().trace(hook)
    return True


def foreign_pair():
    """An in-process channel whose host end stages interop: the attach
    side reports (R, caps 0) as its identity word, so the host reads a
    foreign peer word at ready_wait. The reverse direction stays
    same-language (the attach side reads the host's Python word)."""
    from pymizu import _pymizu

    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token, _ident=(2, 0))
    p.ready_set()
    assert h.ready_wait(10)
    return h, p
