"""Task-frame codec tests: the structured (fn, args, kwargs) staging tier
— fn by reference or pickled, codec scalar / None / container / buffer
leaves, and the zero-copy BUFREF leaf — over real spawned workers, plus
parser-level coverage through the ``_read_stream`` hook."""

import os
import pickle
import subprocess
import sys
from functools import partial

import numpy as np
import pytest
from tests.helpers import array_sum, echo, is_readonly, square, sum_mixed

import pymizu
from pymizu._pymizu import _read_stream, _task_frame


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


def test_fn_by_reference(pool):
    # an importable callable crosses as (module, qualname), no pickle
    assert pool.submit(square, 7).collect(timeout=5) == 49
    assert pool.submit(pow, 2, 10).collect(timeout=5) == 1024  # a builtin


def test_fn_pickled_fallback(pool):
    # a partial is not a plain function/builtin: fn rides its own pickle
    # stream (stock pickle handles partials, no cloudpickle needed)
    assert pool.submit(partial(square, 9)).collect(timeout=5) == 81


def test_fn_main_module_falls_back(pool):
    # '__main__' is excluded from by-reference crossing (the worker's
    # __main__ is pymizu.worker, not the submitter's script); cloudpickle
    # carries it by value instead
    pytest.importorskip("cloudpickle")

    def local_fn(x):
        return x * 3

    local_fn.__module__ = "__main__"
    assert pool.submit(local_fn, 5).collect(timeout=5) == 15


def test_fn_unresolvable_on_worker(pool):
    # referenceable shape, but the module does not exist on the worker:
    # the resolution failure is the task's error
    def phantom():
        return None

    phantom.__module__ = "no_such_module_xyz"
    phantom.__qualname__ = "phantom"
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool.submit(phantom).collect(timeout=5)
    assert exc_info.value.remote_type == "ModuleNotFoundError"


def test_scalar_none_kwargs_args(pool):
    out = pool.submit(echo, 1, None, "x", 2.5, k=None, b=True).collect(
        timeout=5
    )
    assert out == ((1, None, "x", 2.5), {"k": None, "b": True})


def test_bytes_and_memoryview_args(pool):
    assert pool.submit(echo, b"\x00\xff" * 100).collect(timeout=5)[0][0] == (
        b"\x00\xff" * 100
    )
    # a memoryview is a buffer leaf: it arrives as a uint8 array
    out = pool.submit(echo, memoryview(b"abc")).collect(timeout=5)
    assert out[0][0].tolist() == [97, 98, 99]


def test_numpy_arg_zero_copy_view(pool):
    # past the zero-copy floor a buffer argument crosses as a read-only
    # view over shared pages (BUFREF), not a writable copy
    a = np.arange(1000000, dtype=np.float64)
    assert pool.submit(array_sum, a).collect(timeout=10) == a.sum()
    assert pool.submit(is_readonly, a).collect(timeout=10) is True
    # and again: the region recycled back to the free list
    assert pool.submit(array_sum, a).collect(timeout=10) == a.sum()


def test_small_numpy_arg_is_a_writable_copy(pool):
    a = np.arange(100, dtype=np.float64)
    assert pool.submit(array_sum, a).collect(timeout=5) == a.sum()
    assert pool.submit(is_readonly, a).collect(timeout=5) is False


def test_container_with_buffer_leaves(pool):
    out = pool.submit(sum_mixed, [np.arange(4.0), 5]).collect(timeout=5)
    assert out == 11.0


def test_submit_batch_frames(pool):
    tasks = pool.submit_batch([partial(square, i) for i in range(4)])
    assert [t.collect(timeout=5) for t in tasks] == [0, 1, 4, 9]


def test_vanished_bufref_region_does_not_wedge_pool(pool):
    # a submitter stages a BUFREF task and dies before the worker resolves
    # the region: the failed resolve publishes DIED to a slot nobody
    # collects, and the worker's drain continues
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import os, sys, pymizu\n"
            "import numpy as np\n"
            "from tests.helpers import array_sum\n"
            "p = pymizu.Pool.attach(sys.argv[1])\n"
            "p.submit(array_sum, np.arange(1000000, dtype=np.float64))\n"
            "os._exit(0)\n",
            pool.token,
        ],
        cwd=os.path.dirname(os.path.dirname(__file__)),
    )
    child.wait(timeout=10)
    for _ in range(3):
        assert pool.submit(square, 3).collect(timeout=5) == 9


# Parser-level coverage through the _read_stream hook -------------------


def _s(x):
    b = x.encode()
    return b"s" + len(b).to_bytes(4, "little") + b


def _task_stream(fn_kind, fn_bytes, args_bytes, kwargs_bytes):
    return b"Pk" + bytes([fn_kind]) + fn_bytes + args_bytes + kwargs_bytes


def test_read_stream_old_pickled_frame():
    # a pickled (fn, args, kwargs) frame from before the frame codec
    out = _read_stream(pickle.dumps((pow, (2, 10), {}), protocol=4))
    assert out[0] is pow
    assert out[1] == (2, 10)
    assert out[2] == {}


def test_read_stream_none_in_containers():
    assert _read_stream(b"Pl" + (2).to_bytes(4, "little") + b"nn") == [
        None,
        None,
    ]


def test_read_stream_task_frame_by_reference():
    stream = _task_stream(
        0,
        _s("builtins") + _s("pow"),
        b"t"
        + (2).to_bytes(4, "little")
        + b"i"
        + (2).to_bytes(8, "little")
        + b"i"
        + (10).to_bytes(8, "little"),
        b"d" + (0).to_bytes(4, "little"),
    )
    out = _read_stream(stream)
    assert out[0] is pow
    assert out[1] == (2, 10)
    assert out[2] == {}


def test_read_stream_corrupt_frames():
    # bad fn kind byte
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pk\x07")
    # truncated fn reference
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pk\x00" + b"s\x05\x00")
    # over-cap args tuple
    stream = (
        b"Pk\x00"
        + _s("builtins")
        + _s("pow")
        + b"t"
        + (65).to_bytes(4, "little")
    )
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(stream)
    # a BUFREF leaf with a truncated name
    stream = (
        b"Pk\x00"
        + _s("builtins")
        + _s("pow")
        + b"t"
        + (1).to_bytes(4, "little")
        + b"r"
        + bytes([1])
        + (8).to_bytes(8, "little")
        + bytes([12])
        + b"/mizu_short"
    )
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(stream)
    # a BUFREF leaf naming a region that does not exist
    stream = (
        b"Pk\x00"
        + _s("builtins")
        + _s("pow")
        + b"t"
        + (1).to_bytes(4, "little")
        + b"r"
        + bytes([1])
        + (8).to_bytes(8, "little")
        + bytes([13])
        + b"/mizu_nonexist"
    )
    with pytest.raises(pymizu.MizuError, match="gone"):
        _read_stream(stream)


def test_task_frame_factory_validation():
    with pytest.raises(TypeError):
        _task_frame(pow, [2, 10], {})  # args must be a tuple
    frame = _task_frame(pow, (2, 10), {})
    assert type(frame).__name__ == "_TaskFrame"
    assert tuple(frame) == (pow, (2, 10), {})
