"""Importable task callables for the pool tests.

Task callables ride pickle by reference under stock pickle, so the worker
processes must import them: they live here (``tests.helpers``) rather than
in the test module. The repo-root conftest.py puts the root on sys.path in
the test process; a spawned worker's sys.path[0] is the repo root already.
"""

import array
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


def slow(i):
    time.sleep(0.3)
    return i


def touch_then_sleep(marker, t):
    """Signal to the submitter that execution started, then sleep: the
    deterministic stand-in for a sleep before pool.stop(), whose budget a
    worker's claim latency can exceed on a loaded runner. The marker
    carries the start on the monotonic clock (system-wide on every
    platform), so a test can measure from the task's own start rather
    than from the moment its GIL-contended main thread noticed it."""
    with open(marker, "w") as f:
        f.write(repr(time.monotonic()))
    time.sleep(t)
    return 0


def raise_long(msg):
    raise ValueError(msg)


class UnprintableError(Exception):
    def __str__(self):
        raise RuntimeError("cannot print")


def raise_unprintable():
    raise UnprintableError("never seen")


def raise_deep(n, msg):
    if n <= 0:
        raise ValueError(msg)
    return raise_deep(n - 1, msg)


class ALongExceptionNameForEnvelopeSizingPastTheBudgetBoundary(Exception):
    pass


def raise_long_named(n):
    raise ALongExceptionNameForEnvelopeSizingPastTheBudgetBoundary("y" * n)


LongNamedError = type("A" + "x" * 249, (Exception,), {})


def raise_long_named_short_message():
    # the truncation ladder reaches the message cut with the message
    # exactly inside the half budget (236 at the default slot): the
    # truncate is a no-op (the type name alone carries the envelope
    # past the budget, so the fixed fallback still lands)
    raise LongNamedError("y" * 236)


def raise_with_surrogate_message():
    # a lone surrogate in the message: the private envelope's truncate
    # blanks rather than escapes (the 'I' err stream's dual)
    raise ValueError("x" * 600 + "\udcff")


def raise_with_nonint_map_index():
    # a _pymizu_map_index that is not an int: the error publish drops the
    # annotation silently rather than carrying it as the runner's index
    e = ValueError("boom")
    e._pymizu_map_index = 1.5
    raise e


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


def make_fail_on_unpickle():
    return FailOnUnpickle()


class BadCallable:
    """A callable whose pickle fails: a submit_batch item that cannot
    stage (P-05)."""

    def __call__(self):
        return 1

    def __reduce_ex__(self, proto):
        raise TypeError("unpicklable on purpose")


class BufferFailAfterFirst:
    """A PEP 688 exporter whose __buffer__ fails past a call budget. The
    task frame negotiates once per leaf (the size pass's open view is
    carried into the write pass), so fail_after=0 declines at the size
    pass and any larger budget stages. The layout tree's walk probes
    twice per pass, so its write-pass failure wants fail_after=3."""

    def __init__(self, a, fail_after=1):
        self.a = a
        self.calls = 0
        self.fail_after = fail_after

    def __buffer__(self, flags):
        self.calls += 1
        if self.calls > self.fail_after:
            raise BufferError("export refused")
        return memoryview(self.a)

    def __release_buffer__(self, view):
        view.release()


def two_buffers(a, b):
    """Two buffer arguments' arrival forms: read-only flags and sums."""
    return (
        not a.flags.writeable,
        not b.flags.writeable,
        float(a.sum()),
        float(b.sum()),
    )


def sum_two(a, b):
    """b is a BufferFailAfterFirst: its .a rides the pickle."""
    return float(a.sum() + b.a.sum())


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


def rand_shift(v, k):
    """A draw from the worker's stdlib random stream plus k (seeded maps
    with args)."""
    return random.random() + k


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


def raise_system_exit():
    """A task's BaseException escapes the exec hook (hard crash)."""
    raise SystemExit(3)


def raise_keyboard_interrupt():
    """The KeyboardInterrupt tier of a task's BaseException escape."""
    raise KeyboardInterrupt


def fail_with_map_index(i, idx):
    """An error annotated with an in-flight element index, as a map
    runner's batch loop does: the error envelope carries it as the
    fourth tuple element."""
    try:
        raise ValueError(f"element {i}")
    except ValueError as e:
        e._pymizu_map_index = idx
        raise


def nested_map(n):
    """A map inside a task, on the evaluating worker's own pool handle."""
    pool = pymizu.current_pool()
    return pool.map(square, list(range(n))).collect()


def fail_or_sleep(i, bad, t):
    if i == bad:
        raise ValueError(f"element {i}")
    time.sleep(t)
    return i


def sleep_ident(t):
    time.sleep(t)
    return t


ECHO_PEER = """
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(x)
"""


def parked_thread(fn):
    """Spawn a daemon thread calling ``fn()`` and return once the thread
    is at the verb's door — the deterministic stand-in for ``start()``
    plus a sleep, whose budget thread-start latency can exceed on a
    loaded runner. Returns ``(thread, results)``: the call's return
    value or raised exception, appended when it returns."""
    results = []
    at_door = threading.Event()

    def run():
        at_door.set()
        try:
            results.append(fn())
        except Exception as e:
            results.append(e)

    th = threading.Thread(target=run, daemon=True)
    th.start()
    assert at_door.wait(5)
    return th, results


def wait_guard_held(poll, timeout=5.0):
    """Poll ``poll()`` until it raises pymizu.ConcurrentUseError: proof a
    verb on another thread has entered and holds the side's guard — the
    deterministic follow-up to ``parked_thread()`` where a sleep would
    only guess that the verb had started. ``poll`` must be a guarded
    verb with no side effects while the guard is free (a zero-timeout
    poll such as ``h.recv(0)``)."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            poll()
        except pymizu.ConcurrentUseError:
            return
        assert time.monotonic() < deadline, (
            "the parked verb never took the guard"
        )
        time.sleep(0.001)


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


def np_f64(v):
    """An np.float64 scalar result (a typed map-result leaf, np 1)."""
    return np.float64(v) * 2


def np_i64(v):
    return np.int64(v) * 2


def np_i32(v):
    return np.int32(v) * 2


def np_u8(v):
    return np.uint8(v) * 2


def np_c128(v):
    return np.complex128(v) * 2


def np_pair(v):
    """The winsum shape: a tuple of two np.float64 scalars (shape 1)."""
    s = np.float64(v)
    return (s, s * 2)


def np_list_pair(v):
    """A list of two np.float64 scalars (shape 2)."""
    s = np.float64(v)
    return [s, s * 2]


def py_pair(v):
    """A tuple of two Python floats (np 0, shape 1)."""
    s = float(v)
    return (s, s * 2)


def mixed_type(v):
    """A type that changes mid-batch: the pickled map-result leaf."""
    return float(v) if v % 2 == 0 else str(v)


def none_result(v):
    """A None result (the pickled leaf: None has no typed class)."""
    return None


def is_even(v):
    """A bool result (the pickled leaf: bool is not in the wire subset)."""
    return v % 2 == 0


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


class BytesSubclass(bytes):
    """A bytes subclass: same pickle-semantics rule as StrSubclass."""


class ArraySubclass(array.array):
    """An array.array subclass: the raw buffer tier is exact-type only,
    so the subclass keeps its pickle semantics."""


if np is not None:

    class _UnhashableMeta(type):
        __hash__ = None

    class UnhashableScalar(np.float64, metaclass=_UnhashableMeta):
        """A numpy scalar subtype whose metaclass breaks __hash__: the
        known-scalar set probe in the buffer gate errors and declines."""


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


def large_args_integrity(b, s):
    """Large by-value arguments' arrival integrity (the task-frame spill)."""
    return len(b), sum(b), b[:8], b[-8:], len(s), s[:8], s[-8:]


def is_readonly(a):
    """True when the argument arrived as a read-only zero-copy view."""
    return not a.flags.writeable


def sum_mixed(xs):
    """A flat container with a buffer leaf: an array and a scalar."""
    return float(xs[0].sum()) + xs[1]


def sum_pair(xs):
    """Two buffer leaves in one flat container: both sums."""
    return float(xs[0].sum() + xs[1].sum())


def sum_pair_flaky(xs):
    """A buffer leaf and a BufferFailAfterFirst in one flat container."""
    return float(xs[0].sum() + xs[1].a.sum())


def sum_dict(d):
    """Two buffer leaves in a dict container: both sums."""
    return float(d["a"].sum() + d["b"].sum())


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


def frames_report(*frames):
    """Per Frame argument: the arrival verdict — the backing form
    ('_ShmView' when region-backed, 'list' for materialized strings) and
    the head values, nulls included."""
    out = []
    for f in frames:
        d = f.to_dict()
        backing = {
            k: "list" if isinstance(v, list) else type(v.base).__name__
            for k, v in d.items()
        }
        head = {
            k: v[:3] if isinstance(v, list) else v[:3].tolist()
            for k, v in d.items()
        }
        out.append((backing, head))
    return out


def frame_and_buffer(f, a):
    """A Frame and a buffer argument's arrival forms, with the buffer's
    sum (the one-checkout budget: both copy on the pickle fallback)."""
    return frames_report(f)[0], not a.flags.writeable, float(a.sum())


def chan_pair():
    """An in-process channel pair (host, peer) as facade Channels."""
    from pymizu import _pymizu

    h = _pymizu._channel_new(64, 256, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    p.ready_set()
    assert h.ready_wait(5)
    return pymizu.Channel._wrap(h), pymizu.Channel._wrap(p)


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
# c_void_p, not c_char_p: a bytes return warns "memory leak in callback
# function" (the char* points into a conversion buffer ctypes drops)
_GET_LAST_ERROR = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p)


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

    def __init__(self, columns, *, fail=None):
        """fail: None, or "schema" (get_schema errors with a message),
        "schema_silent" (get_schema errors, get_last_error NULL), or
        "bad_schema" (get_schema serves a NULL-format schema)."""
        self.released_batches = 0
        self.released_stream = 0
        self._served = 0
        self._fail = fail
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
                        "L": ctypes.c_uint64,
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
        self._err_buf = (
            None
            if fail in (None, "schema_silent")
            else ctypes.create_string_buffer(b"boom")
        )
        self._schema_cb = _GET_SCHEMA(self._get_schema)
        self._next_cb = _GET_NEXT(self._get_next)
        self._error_cb = _GET_LAST_ERROR(self._get_last_error)
        # a failing producer's releases are NULL: a Python callback would
        # run with the decline in flight (the SystemError clobbers it)
        self._release_cb = (
            _RELEASE()
            if fail is not None
            else _RELEASE(self._on_stream_release)
        )
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
        if self._fail in ("schema", "schema_silent"):
            return 1
        s = _ArrowSchema.from_address(out)
        s.format = None if self._fail == "bad_schema" else b"+s"
        s.name = b""
        s.flags = 0
        s.n_children = len(self._schema_children)
        s.children = self._schema_children
        s.release = (
            _RELEASE()
            if self._fail is not None
            else _RELEASE(self._on_schema_release)
        )
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
        if self._err_buf is None:
            return None
        return ctypes.addressof(self._err_buf)

    def __arrow_c_stream__(self):
        return self._capsule


def fail_indexed():
    """A map-style error envelope: the worker reads the element index off
    `_pymizu_map_index` as the outcome tuple's fourth element."""
    e = ValueError("indexed failure")
    e._pymizu_map_index = 7
    raise e


# One past the worker fn-resolution caches' clear-on-full cap (64), for
# the eviction tests; exec gives each a clean top-level qualname so the
# by-reference gate accepts it.
for _i in range(70):
    exec(f"def cache_evict_{_i}(): return {_i}")
del _i
