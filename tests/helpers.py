"""Importable task callables for the pool tests.

Task callables ride pickle by reference under stock pickle, so the worker
processes must import them: they live here (``tests.helpers``) rather than
in the test module. The repo-root conftest.py puts the root on sys.path in
the test process; a spawned worker's sys.path[0] is the repo root already.
"""

import ctypes
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


def np_rand_elt(i):
    """A draw from the element's per-element numpy Generator (seeded
    maps): pymizu.current_rng() inside the task."""
    return pymizu.current_rng().random()


def np_rand_pair(i):
    """Two draws plus the memo identity: two calls in one element
    continue one stream (the same Generator object)."""
    rng = pymizu.current_rng()
    return (
        rng.random(),
        pymizu.current_rng().random(),
        rng is pymizu.current_rng(),
    )


def current_rng_or_none(*_):
    """pymizu.current_rng() outside a seeded map element: None."""
    return pymizu.current_rng()


def fail_at(i, bad):
    if i == bad:
        raise ValueError(f"element {i}")
    return i


def fail_on(i, marks):
    """Fail on every marked element (multi-error fail-fast tests)."""
    if i in marks:
        raise ValueError(f"element {i}")
    return i


def sleep_on(i, marks, t=0.3):
    """Sleep on the marked elements, so those chunks complete last."""
    if i in marks:
        time.sleep(t)
    return i


def map_ctx_names(_):
    """The worker's map-context cache keys (one attach per worker per map)."""
    from pymizu import _map

    return list(_map._ctx_cache)


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


def mul_add(a, b, c=0):
    """A multi-positional-arg callable (starmap tests)."""
    return a * b + c


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


def scalar_cplx(v):
    """A plain complex scalar per element (template maps, m = 1)."""
    return complex(v, v)


def na_int(v):
    """The int32 NA sentinel for multiples of 3 (template maps)."""
    return -(2**31) if v % 3 == 0 else v


def na_int64(v):
    """The int64 NA sentinel for multiples of 3 (template maps)."""
    return -(2**63) if v % 3 == 0 else v


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


def frame_unpickle(payload):
    """A Frame result for pool tests: Frames are not constructible
    directly, so the worker rebuilds one through its pickle form."""
    import pickle

    return pickle.loads(payload)


def foreign_pair(caps=0):
    """An in-process channel whose host end stages interop: the attach
    side reports (R, caps) as its identity word, so the host reads a
    foreign peer word at ready_wait. The reverse direction stays
    same-language (the attach side reads the host's Python word)."""
    from pymizu import _pymizu

    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token, _ident=(2, caps))
    p.ready_set()
    assert h.ready_wait(10)
    return h, p


# --- a hand-rolled Arrow capsule stream (ctypes, no pyarrow) -----------------
#
# The C Data Interface structs and callbacks in plain ctypes: builds a
# producer for Frame.from_arrow's success-path cases in test_no_numpy's
# shim, where pyarrow cannot import. Success-path use only: the release
# callbacks are plain Python, safe while no exception is in flight.

_RELEASE = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
_GET_SCHEMA = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
_GET_NEXT = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
_GET_LAST_ERROR = ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)


class _ArrowSchema(ctypes.Structure):
    pass


_ArrowSchema._fields_ = [
    ("format", ctypes.c_char_p),
    ("name", ctypes.c_char_p),
    ("metadata", ctypes.c_char_p),
    ("flags", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("children", ctypes.POINTER(ctypes.POINTER(_ArrowSchema))),
    ("dictionary", ctypes.POINTER(_ArrowSchema)),
    ("release", _RELEASE),
    ("private_data", ctypes.c_void_p),
]


class _ArrowArray(ctypes.Structure):
    pass


_ArrowArray._fields_ = [
    ("length", ctypes.c_int64),
    ("null_count", ctypes.c_int64),
    ("offset", ctypes.c_int64),
    ("n_buffers", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("buffers", ctypes.POINTER(ctypes.c_void_p)),
    ("children", ctypes.POINTER(ctypes.POINTER(_ArrowArray))),
    ("dictionary", ctypes.POINTER(_ArrowArray)),
    ("release", _RELEASE),
    ("private_data", ctypes.c_void_p),
]


class _ArrowArrayStream(ctypes.Structure):
    _fields_ = [
        ("get_schema", _GET_SCHEMA),
        ("get_next", _GET_NEXT),
        ("get_last_error", _GET_LAST_ERROR),
        ("release", _RELEASE),
        ("private_data", ctypes.c_void_p),
    ]


class RawArrowStream:
    """A minimal __arrow_c_stream__ producer over ctypes.

    columns: (format, name, batches) triples; a batch is a (values, valid)
    pair — values a list of numbers for "i" / "g" / "tss:" or an
    (offsets, bytes) pair for "u", valid a list of 0/1 per row (None: all
    valid). released_batches / released_stream count the release callbacks.
    """

    def __init__(self, columns):
        self.released_batches = 0
        self.released_stream = 0
        self._served = 0
        self._keep = []
        ncols = len(columns)
        nbatches = len(columns[0][2])
        # the schema: a struct root over one child per column
        child_schemas = []
        self._schema_children = (ctypes.POINTER(_ArrowSchema) * ncols)()
        for i, (fmt, name, _) in enumerate(columns):
            fmt = fmt.encode() if isinstance(fmt, str) else fmt
            name = name.encode() if isinstance(name, str) else name
            s = _ArrowSchema()
            s.format = fmt
            s.name = name
            s.flags = 2
            s.n_children = 0
            s.release = _RELEASE(self._on_schema_release)
            child_schemas.append(s)
            self._schema_children[i] = ctypes.pointer(s)
            self._keep.extend([fmt, name, s.release])
        self._keep.append(child_schemas)
        # the batches: a struct root per batch over pre-built column arrays
        self._batches = []
        for b in range(nbatches):
            children = []
            child_ptrs = (ctypes.POINTER(_ArrowArray) * ncols)()
            for i, (fmt, _, col_batches) in enumerate(columns):
                values, valid = col_batches[b]
                a = _ArrowArray()
                if fmt == "u":
                    offsets, data = values
                    n = len(offsets) - 1
                    offs = (ctypes.c_int32 * len(offsets))(*offsets)
                    dat = (ctypes.c_char * len(data)).from_buffer_copy(data)
                    vaddr = self._bitmap(n, valid)
                    bufs = (ctypes.c_void_p * 3)(
                        vaddr, ctypes.addressof(offs), ctypes.addressof(dat)
                    )
                    a.length = n
                    a.n_buffers = 3
                    self._keep.extend([offs, dat])
                else:
                    ctype = {
                        "i": ctypes.c_int32,
                        "l": ctypes.c_int64,
                        "g": ctypes.c_double,
                        "tss:": ctypes.c_int64,
                    }[fmt]
                    n = len(values)
                    vals = (ctype * n)(*values)
                    vaddr = self._bitmap(n, valid)
                    bufs = (ctypes.c_void_p * 2)(vaddr, ctypes.addressof(vals))
                    a.length = n
                    a.n_buffers = 2
                    self._keep.append(vals)
                a.null_count = 0 if valid is None else valid.count(0)
                a.offset = 0
                a.buffers = bufs
                a.n_children = 0
                a.release = _RELEASE(lambda ptr: None)
                children.append(a)
                child_ptrs[i] = ctypes.pointer(a)
                self._keep.extend([bufs, a.release])
            root = _ArrowArray()
            root.length = children[0].length
            root.null_count = 0
            root.offset = 0
            root.n_buffers = 1
            root_buf = (ctypes.c_void_p * 1)(None)
            root.buffers = root_buf
            root.n_children = ncols
            root.children = child_ptrs
            root_cb = _RELEASE(self._on_batch_release)
            root.release = root_cb
            self._batches.append(
                (root, children, child_ptrs, root_buf, root_cb)
            )
            self._keep.extend(children)
            self._keep.extend([child_ptrs, root_buf, root_cb])
        # the stream struct and its capsule
        self._schema_cb = _GET_SCHEMA(self._get_schema)
        self._next_cb = _GET_NEXT(self._get_next)
        self._error_cb = _GET_LAST_ERROR(self._get_last_error)
        self._release_cb = _RELEASE(self._on_stream_release)
        self._stream = _ArrowArrayStream()
        self._stream.get_schema = self._schema_cb
        self._stream.get_next = self._next_cb
        self._stream.get_last_error = self._error_cb
        self._stream.release = self._release_cb
        new_capsule = ctypes.pythonapi.PyCapsule_New
        new_capsule.restype = ctypes.py_object
        new_capsule.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_void_p,
        ]
        self._capsule = new_capsule(
            ctypes.addressof(self._stream), b"arrow_array_stream", None
        )

    def _bitmap(self, n, valid):
        """A validity bitmap buffer's address (None when all valid)."""
        if valid is None:
            return None
        bm = (ctypes.c_uint8 * ((n + 7) // 8))()
        for k in range(n):
            if valid[k]:
                bm[k >> 3] |= 1 << (k & 7)
        self._keep.append(bm)
        return ctypes.addressof(bm)

    def _on_schema_release(self, ptr):
        pass

    def _on_batch_release(self, ptr):
        self.released_batches += 1

    def _on_stream_release(self, ptr):
        self.released_stream += 1

    def _get_schema(self, st, out):
        s = _ArrowSchema.from_address(out)
        s.format = b"+s"
        s.name = b""
        s.flags = 0
        s.n_children = len(self._schema_children)
        s.children = self._schema_children
        s.release = _RELEASE(self._on_schema_release)
        self._keep.append(s.release)
        return 0

    def _get_next(self, st, out):
        idx = self._served
        self._served += 1
        a = _ArrowArray.from_address(out)
        if idx >= len(self._batches):
            a.release = _RELEASE()
            return 0
        root = self._batches[idx][0]
        for field, _ in _ArrowArray._fields_:
            setattr(a, field, getattr(root, field))
        return 0

    def _get_last_error(self, st):
        return b""

    def __arrow_c_stream__(self):
        return self._capsule
