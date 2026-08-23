"""Importable task callables for the pool tests.

Task callables ride pickle by reference under stock pickle, so the worker
processes must import them: they live here (``tests.helpers``) rather than
in the test module. The repo-root conftest.py puts the root on sys.path in
the test process; a spawned worker's sys.path[0] is the repo root already.
"""

import threading
import time

import pyrei


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


def make_unpicklable():
    return Unpicklable()


def fanout(n):
    """Nested fan-out: submit n subtasks on the worker's own pool handle
    and collect them (nested collect helps instead of parking)."""
    pool = pyrei.current_pool()
    tasks = [pool.submit(square, i) for i in range(n)]
    return sum(t.collect() for t in tasks)


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
