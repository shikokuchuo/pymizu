"""Task-frame codec tests: the structured (fn, args, kwargs) staging tier
— fn by reference or pickled, codec scalar / None / container / buffer
leaves, and the zero-copy BUFREF / FRAMEREF leaves — over real spawned
workers, plus parser-level coverage through the ``_read_stream`` hook."""

import gc
import math
import os
import pickle
import subprocess
import sys
import threading
import types
from functools import partial

import numpy as np
import pytest
import tests.helpers as helpers
import tests.helpers_late as helpers_late
from tests.helpers import (
    BufferFailAfterFirst,
    array_sum,
    echo,
    frame_and_buffer,
    frames_report,
    is_readonly,
    square,
    sum_dict,
    sum_list,
    sum_mixed,
    sum_pair,
    sum_pair_flaky,
    two_buffers,
)

import pymizu
from pymizu import _pymizu
from pymizu._pymizu import (
    _codec_stream,
    _frame_rebuild,
    _read_stream,
    _task_frame,
)


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


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


def test_fn_without_module_or_qualname_pickles(pool):
    # a function missing __module__ / __qualname__ fails the by-reference
    # gate and rides its own pickle instead
    pytest.importorskip("cloudpickle")

    def bare(x):
        return x + 1

    del bare.__module__
    assert pool.submit(bare, 1).collect(timeout=5) == 2

    def noqual(x):
        return x + 2

    # __qualname__ itself refuses deletion: clear __dict__ but keep the
    # module, so the gate fails on the qualname fetch
    noqual.__dict__ = {"__module__": "tests.helpers"}
    assert pool.submit(noqual, 1).collect(timeout=5) == 3


def test_fn_resolve_picks_up_rebind(monkeypatch):
    # resolution is fresh per task off the worker's imported modules: a
    # mid-process rebind is picked up on the next submission
    fn = helpers.identity
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        wk = _pymizu._pool_worker_join(h.token, 0)
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        p = pymizu.Pool._wrap(h)
        assert p.submit(fn, 1).collect(timeout=10) == 1
        monkeypatch.setattr(helpers, "identity", lambda x: "rebound")
        assert p.submit(fn, 2).collect(timeout=10) == "rebound"
        p.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


def test_fn_resolve_first_import():
    # the miss tier: a callable from a module a fresh worker has not yet
    # imported resolves through the full import machinery
    with pymizu.Pool.create(1) as p:
        assert p.submit(helpers_late.forty_two).collect(timeout=10) == 42


def test_fn_resolve_miss_then_success(monkeypatch):
    # a task whose module is not yet importable errors off the miss
    # tier's ImportError, then resolves via sys.modules once it is
    mod = types.ModuleType("dynmod_phase3")

    def late():
        return 42

    late.__module__ = "dynmod_phase3"
    late.__qualname__ = "late"
    mod.late = late
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        wk = _pymizu._pool_worker_join(h.token, 0)
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        p = pymizu.Pool._wrap(h)
        with pytest.raises(pymizu.TaskError):
            p.submit(late).collect(timeout=10)
        monkeypatch.setitem(sys.modules, "dynmod_phase3", mod)
        assert p.submit(late).collect(timeout=10) == 42
        p.stop()
        loop.join(timeout=10)
        wk.destroy()
    finally:
        h.destroy()


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


@pytest.mark.skipif(
    sys.version_info < (3, 12), reason="PEP 688 __buffer__ is CPython 3.12+"
)
def test_size_pass_view_carried_into_write(pool):
    # the write pass consumes the size pass's open buffer view — one
    # negotiation per leaf, so an exporter refusing a second export still
    # stages as buffer leaves: big by reference (a read-only view), small
    # a writable copy. The P-17 write-pass decline this replaces is gone
    # by construction: the write never re-exports.
    big = np.full(16000, 1.0)  # 125 KiB: past the zero-copy floor
    flaky = BufferFailAfterFirst(np.full(10, 2.0))
    out = pool.submit(two_buffers, big, flaky).collect(timeout=15)
    assert out == (True, False, 16000.0, 20.0)
    assert flaky.calls == 1


def test_container_with_buffer_leaves(pool):
    out = pool.submit(sum_mixed, [np.arange(4.0), 5]).collect(timeout=5)
    assert out == 11.0


def test_many_buffer_leaves(pool):
    # six buffer leaves: past the stack-seeded view array (4), the frame
    # plan grows to the heap — values stay exact
    xs = [np.full(4, float(i)) for i in range(1, 7)]
    assert pool.submit(sum_list, xs).collect(timeout=5) == 84.0


def test_dict_arg_non_str_key_pickles(pool):
    # a task frame's dict keys must be codec strings: anything else
    # declines to the whole-tuple pickle
    assert pool.submit(len, {1: "x", 2: "y"}).collect(timeout=5) == 2


def _wait_child(child, timeout=30):
    try:
        child.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
        raise


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
    _wait_child(child)
    for _ in range(3):
        assert pool.submit(square, 3).collect(timeout=5) == 9


# The FRAMEREF leaf: a Frame argument past the zero-copy floor --------------


def big_frame(n=20000, with_nulls=True):
    """A Frame past the 32 KiB zero-copy floor: int32 / float64 / string
    columns, every tenth value null."""

    def maybe(vals, k):
        return None if with_nulls and k % 10 == 0 else vals

    return _frame_rebuild(
        ["i", "x", "s"],
        [
            ("i", [maybe(k, k) for k in range(n)]),
            ("g", [maybe(k * 0.5, k) for k in range(n)]),
            ("s", [maybe(f"s{k % 100}", k) for k in range(n)]),
        ],
        None,
    )


def dict_frame(n=24000):
    """A Frame with a factor (dict) column, past the zero-copy floor."""
    codes = [None if k % 10 == 0 else k % 2 for k in range(n)]
    return _frame_rebuild(["d"], [("d", codes, ["x", "y"])], None)


def test_frame_rebuild_complex_na():
    # a complex payload's None rebuilds as the NA pair, never a
    # malformed-state error (P-15)
    f = _frame_rebuild(["z"], [("Z", [1 + 2j, None, 3 - 1j])], None)
    out = f.to_dict()["z"]
    assert out[0] == 1 + 2j and out[2] == 3 - 1j
    assert out[1].real != out[1].real and out[1].imag != out[1].imag
    # the pickle form round-trips (the reduce emits None at the NA)
    g = pickle.loads(pickle.dumps(f))
    got = g.to_dict()["z"]
    assert got[0] == 1 + 2j and got[2] == 3 - 1j
    assert got[1].real != got[1].real and got[1].imag != got[1].imag


def test_frame_arg_zero_copy_view(pool):
    # a Frame argument past the zero-copy floor crosses as one MIZL region
    # (FRAMEREF): the worker's callable receives a region-backed Frame,
    # values and nulls exact
    report = pool.submit(frames_report, big_frame()).collect(timeout=10)
    backing, head = report[0]
    assert backing == {"i": "_ShmView", "x": "_ShmView", "s": "list"}
    assert head["i"] == [-(2**31), 1, 2]
    assert math.isnan(head["x"][0]) and head["x"][1:] == [0.5, 1.0]
    assert head["s"] == [None, "s1", "s2"]
    # and again: the region recycled back to the free list
    report = pool.submit(frames_report, big_frame()).collect(timeout=10)
    assert report[0][0]["i"] == "_ShmView"


def test_small_frame_arg_pickles(pool):
    # below the floor the whole task tuple pickles: a copy-backed Frame,
    # values exact
    report = pool.submit(frames_report, big_frame(100)).collect(timeout=5)
    backing, head = report[0]
    assert backing["i"] != "_ShmView" and backing["x"] != "_ShmView"
    assert math.isnan(head["i"][0]) and head["i"][1:] == [1.0, 2.0]
    assert math.isnan(head["x"][0]) and head["x"][1:] == [0.5, 1.0]
    assert head["s"] == [None, "s1", "s2"]


def test_frame_args_one_region_budget(pool):
    # one staging checkout per task: a second region-sized argument (a
    # second frame, or a big buffer) sends the whole task down the pickle
    # path — every argument a copy, values exact
    r1, r2 = pool.submit(frames_report, big_frame(), big_frame()).collect(
        timeout=10
    )
    assert r1[0]["i"] != "_ShmView" and r2[0]["i"] != "_ShmView"
    assert math.isnan(r1[1]["i"][0]) and r1[1]["i"][1:] == [1.0, 2.0]
    assert r2[1]["s"] == [None, "s1", "s2"]


def test_dict_frame_arg(pool):
    # a factor column walks the size gate's per-kind sums, then crosses
    report = pool.submit(frames_report, dict_frame()).collect(timeout=10)
    _, head = report[0]
    assert head["d"] == [None, "y", "x"]


def test_large_string_frame_arg(pool):
    # a large_utf8 column builds the 64-bit string block: the size gate's
    # STR64 row, then the crossing itself
    pa = pytest.importorskip("pyarrow")
    t = pa.table({"s": pa.array(["x" * 100] * 70000, type=pa.large_string())})
    report = pool.submit(frames_report, pymizu.Frame.from_arrow(t)).collect(
        timeout=10
    )
    backing, head = report[0]
    assert head["s"] == ["x" * 100] * 3


def test_two_buffer_leaves_one_region_budget(pool):
    # a second region-sized buffer leaf inside a flat container declines:
    # the whole task tuple pickles, values exact on the copies
    a = np.arange(20000, dtype=np.float64)
    b = np.arange(20000, dtype=np.float64) * 2
    out = pool.submit(sum_pair, [a, b]).collect(timeout=10)
    assert out == float(a.sum() + b.sum())


def test_two_buffer_leaves_dict_one_region_budget(pool):
    # the dict container arm of the same size-pass decline
    a = np.arange(20000, dtype=np.float64)
    b = np.arange(20000, dtype=np.float64) * 2
    out = pool.submit(sum_dict, {"a": a, "b": b}).collect(timeout=10)
    assert out == float(a.sum() + b.sum())


def test_container_leaf_buffer_decline_pickles(pool):
    # a container element whose buffer export fails at the size pass:
    # the whole task tuple pickles, values exact (buffer declines are
    # size-pass only — the write pass consumes the open views)
    a = np.full(16000, 1.0)
    refusing = BufferFailAfterFirst(np.full(10, 2.0), fail_after=0)
    out = pool.submit(sum_pair_flaky, [a, refusing]).collect(timeout=10)
    assert out == 16020.0
    a = np.arange(1000000, dtype=np.float64)
    (backing, head), writable, total = pool.submit(
        frame_and_buffer, big_frame(), a
    ).collect(timeout=10)
    assert backing["i"] != "_ShmView" and writable is False
    assert total == float(a.sum())
    assert math.isnan(head["i"][0]) and head["i"][1:] == [1.0, 2.0]


def test_frame_arg_from_arrow_adopted(pool):
    # an adopted (Frame.from_arrow) argument needs no special casing: the
    # export pins its columns through the MIZL write, so the worker's frame
    # is exact even after the producer table is gone
    pa = pytest.importorskip("pyarrow")
    n = 20000
    tbl = pa.table(
        {
            "i": pa.array(
                [None if k % 10 == 0 else k for k in range(n)],
                type=pa.int32(),
            ),
            "x": pa.array(
                [None if k % 10 == 0 else k * 0.5 for k in range(n)]
            ),
        }
    )
    f = pymizu.Frame.from_arrow(tbl)
    del tbl
    gc.collect()
    report = pool.submit(frames_report, f).collect(timeout=10)
    backing, head = report[0]
    assert backing == {"i": "_ShmView", "x": "_ShmView"}
    assert head["i"] == [-(2**31), 1, 2]
    assert math.isnan(head["x"][0]) and head["x"][1:] == [0.5, 1.0]


def test_frame_arg_complex_column_pickles(pool):
    # a complex column has no Arrow export: the FRAMEREF write declines
    # and the whole task tuple pickles — a copy-backed Frame, values exact
    n = 4096  # past the floor at the size gate (64 KiB of complex128)
    f = _frame_rebuild(
        ["z", "i"],
        [("Z", [complex(k, -k) for k in range(n)]), ("i", list(range(n)))],
        None,
    )
    report = pool.submit(frames_report, f).collect(timeout=10)
    backing, head = report[0]
    assert backing["z"] != "_ShmView"
    assert head["z"] == [complex(0, 0), complex(1, -1), complex(2, -2)]
    assert head["i"] == [0, 1, 2]


def test_submit_batch_frame_payload(pool):
    # a batch item's arguments ride the callable's own pickle: the frame
    # crosses by value, values exact
    tasks = pool.submit_batch([partial(frames_report, big_frame())])
    backing, head = tasks[0].collect(timeout=10)[0]
    assert backing["i"] != "_ShmView"
    assert math.isnan(head["i"][0]) and head["i"][1:] == [1.0, 2.0]
    assert head["s"] == [None, "s1", "s2"]


def test_vanished_frameref_region_does_not_wedge_pool(pool):
    # a submitter stages a FRAMEREF task and dies before the worker
    # resolves the region: the failed resolve publishes DIED to a slot
    # nobody collects, and the worker's drain continues
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import os, sys\n"
            "from pymizu import Pool\n"
            "from pymizu._pymizu import _frame_rebuild\n"
            "from tests.helpers import frames_report\n"
            "f = _frame_rebuild(['i'], [('i', list(range(20000)))], None)\n"
            "p = Pool.attach(sys.argv[1])\n"
            "p.submit(frames_report, f)\n"
            "os._exit(0)\n",
            pool.token,
        ],
        cwd=os.path.dirname(os.path.dirname(__file__)),
    )
    _wait_child(child)
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
    # a FRAMEREF leaf with a truncated name
    stream = (
        b"Pk\x00"
        + _s("builtins")
        + _s("pow")
        + b"t"
        + (1).to_bytes(4, "little")
        + b"F"
        + (19 | (100 << 8)).to_bytes(8, "little")
        + bytes([12])
        + b"/mizu_short"
    )
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(stream)
    # a FRAMEREF leaf naming a region that does not exist
    stream = (
        b"Pk\x00"
        + _s("builtins")
        + _s("pow")
        + b"t"
        + (1).to_bytes(4, "little")
        + b"F"
        + (19 | (100 << 8)).to_bytes(8, "little")
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


def test_task_frame_non_str_kwarg_falls_back(pool):
    # the frame writer only carries str keys: a non-str key declines the
    # frame to the whole-tuple pickle, and the call fails there instead
    frame = _task_frame(pow, (2,), {1: 2})
    with pytest.raises(pymizu.TaskError) as exc_info:
        pool._h.submit(frame).collect(timeout=5)
    assert "keywords must be strings" in str(exc_info.value)


def test_read_stream_codec_scalar_errors():
    # a scalar that fails with an error set (invalid UTF-8) propagates it
    # out of the container readers, never a bare corrupt-slot
    with pytest.raises(UnicodeDecodeError):
        _read_stream(b"Ps" + (1).to_bytes(4, "little") + b"\xff")
    with pytest.raises(UnicodeDecodeError):
        _read_stream(
            b"Pl"
            + (1).to_bytes(4, "little")
            + b"s"
            + (1).to_bytes(4, "little")
            + b"\xff"
        )
    with pytest.raises(UnicodeDecodeError):
        _read_stream(
            b"Pd"
            + (1).to_bytes(4, "little")
            + b"s"
            + (1).to_bytes(4, "little")
            + b"\xff"
            + b"n"
        )


def test_read_stream_codec_corrupt_rows():
    # an unknown tag, trailing bytes past the value, a numpy row with a
    # corrupt form byte, and one with a wire type the scalar reader
    # declines (LGL has no numpy home)
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pz")
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pi" + (5).to_bytes(8, "little") + b"x")
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pa" + bytes([14, 2]) + (0).to_bytes(8, "little"))
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pa" + bytes([10, 0]) + (0).to_bytes(4, "little"))


# -- the compact codec's writer bytes (the _codec_stream pin) ----------------


def test_codec_stream_scalars():
    # the exact byte contract: version-mixed pools parse across builds
    assert _codec_stream(None) == b"Pn"
    assert _codec_stream(True) == b"Pb\x01"
    assert _codec_stream(False) == b"Pb\x00"
    assert _codec_stream(7) == b"Pi" + (7).to_bytes(8, "little")
    assert _codec_stream(-2) == b"Pi" + (-2).to_bytes(8, "little", signed=True)
    assert _codec_stream(1.5) == b"Pf\x00\x00\x00\x00\x00\x00\xf8?"
    assert _codec_stream("hé") == (
        b"Ps" + (3).to_bytes(4, "little") + "hé".encode()
    )
    assert _codec_stream(b"\x00\xff") == (
        b"Py" + (2).to_bytes(4, "little") + b"\x00\xff"
    )


def test_codec_stream_containers():
    assert _codec_stream([]) == b"Pl" + (0).to_bytes(4, "little")
    assert _codec_stream([None, None]) == (
        b"Pl" + (2).to_bytes(4, "little") + b"nn"
    )
    assert _codec_stream((1, "a")) == (
        b"Pt"
        + (2).to_bytes(4, "little")
        + b"i"
        + (1).to_bytes(8, "little")
        + b"s"
        + (1).to_bytes(4, "little")
        + b"a"
    )
    assert _codec_stream({"k": True, "j": 2}) == (
        b"Pd"
        + (2).to_bytes(4, "little")
        + b"s"
        + (1).to_bytes(4, "little")
        + b"k"
        + b"b\x01"
        + b"s"
        + (1).to_bytes(4, "little")
        + b"j"
        + b"i"
        + (2).to_bytes(8, "little")
    )
    # the cap is inclusive: 64 elements ride, and the writer round-trips
    assert _codec_stream([0] * 64) == (
        b"Pl"
        + (64).to_bytes(4, "little")
        + (b"i" + (0).to_bytes(8, "little")) * 64
    )
    assert _read_stream(_codec_stream([1, "a", 2.5, None, True])) == (
        [1, "a", 2.5, None, True]
    )
    value = {"a": 1, "b": "x", "c": 2.5, "d": None, "e": True, "f": b"\x00"}
    assert _read_stream(_codec_stream(value)) == value


def test_codec_stream_declines():
    # a value outside the subset declines: the send path pickles
    assert _codec_stream(2**63) is None  # an int past int64
    assert _codec_stream("\ud800") is None  # a lone surrogate
    assert _codec_stream([0] * 65) is None  # past the 64-element cap
    assert _codec_stream({i: i for i in range(65)}) is None
    assert _codec_stream({1}) is None  # a set has no codec row
    assert _codec_stream(object()) is None
    assert _codec_stream([[]]) is None  # no nested containers
    assert _codec_stream(bytearray(1)) is None  # not an exact codec type


def test_codec_stream_spill():
    # past the verb's scratch budget: the exact count + write-only re-walk
    s = "x" * 5000
    assert _codec_stream([s]) == (
        b"Pl"
        + (1).to_bytes(4, "little")
        + b"s"
        + (5000).to_bytes(4, "little")
        + b"x" * 5000
    )


def test_read_stream_task_frame_bad_leaves():
    fn = _s("builtins") + _s("pow")
    no_args = b"t" + (0).to_bytes(4, "little")
    # a pickled-fn frame whose fn is not bytes
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pk\x01" + b"n")
    # a kwargs key that is not a string
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(
            b"Pk\x00"
            + fn
            + no_args
            + b"d"
            + (1).to_bytes(4, "little")
            + b"i"
            + (5).to_bytes(8, "little")
            + b"n"
        )
    # a kwargs value that does not decode (a truncated int)
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(
            b"Pk\x00"
            + fn
            + no_args
            + b"d"
            + (1).to_bytes(4, "little")
            + _s("k")
            + b"i"
            + (1).to_bytes(2, "little")
        )
    # a list argument whose element does not decode
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(
            b"Pk\x00"
            + fn
            + b"t"
            + (1).to_bytes(4, "little")
            + b"l"
            + (1).to_bytes(4, "little")
            + b"i"
            + (1).to_bytes(2, "little")
        )
    # a nested dict argument whose key is not a string
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(
            b"Pk\x00"
            + fn
            + b"t"
            + (1).to_bytes(4, "little")
            + b"d"
            + (1).to_bytes(4, "little")
            + b"i"
            + (5).to_bytes(8, "little")
            + b"n"
        )


# -- the map-result codec (tag 'm') -------------------------------------------

_REAL = 14
_CPLX = 15
_INT64 = 32


def _leaf(type_, np_, shape, m, body):
    return (
        b"\x00" + bytes([type_, np_, shape]) + m.to_bytes(4, "little") + body
    )


def test_read_stream_map_result_typed_leaves():
    import struct

    # form 0, REAL np 0 scalars
    leaf = _leaf(_REAL, 0, 0, 1, (2).to_bytes(8, "little")) + struct.pack(
        "<2d", 1.5, 2.5
    )
    assert _read_stream(b"Pm\x00" + leaf) == [1.5, 2.5]
    # form 1: the hist pairs, then one leaf per batch
    stream = (
        b"Pm\x01"
        + (1).to_bytes(4, "little")
        + (0).to_bytes(8, "little")
        + (2).to_bytes(8, "little")
        + b"\x01"
        + leaf
    )
    assert _read_stream(stream) == ([(0, 2)], [[1.5, 2.5]])
    # form 1, has_vals 0: the template publish
    stream = (
        b"Pm\x01"
        + (1).to_bytes(4, "little")
        + (3).to_bytes(8, "little")
        + (9).to_bytes(8, "little")
        + b"\x00"
    )
    assert _read_stream(stream) == ([(3, 9)], None)
    # shape 1 (tuples of m) off Python scalars
    leaf = _leaf(_REAL, 0, 1, 2, (2).to_bytes(8, "little")) + struct.pack(
        "<4d", 1, 2, 3, 4
    )
    assert _read_stream(b"Pm\x00" + leaf) == [(1.0, 2.0), (3.0, 4.0)]
    # shape 2 (lists of m) of complex scalars
    leaf = _leaf(_CPLX, 0, 2, 2, (1).to_bytes(8, "little")) + struct.pack(
        "<4d", 1, 2, 3, 4
    )
    assert _read_stream(b"Pm\x00" + leaf) == [[(1 + 2j), (3 + 4j)]]
    # INT64 scalars
    leaf = _leaf(_INT64, 0, 0, 1, (2).to_bytes(8, "little")) + struct.pack(
        "<2q", -5, 1 << 62
    )
    assert _read_stream(b"Pm\x00" + leaf) == [-5, 1 << 62]


def test_read_stream_map_result_numpy_leaf():
    import struct

    # np 1: exact numpy scalars rebuilt through frombuffer
    leaf = _leaf(_REAL, 1, 0, 1, (2).to_bytes(8, "little")) + struct.pack(
        "<2d", 1.5, 2.5
    )
    out = _read_stream(b"Pm\x00" + leaf)
    assert all(type(v) is np.float64 for v in out)
    assert out == [1.5, 2.5]
    # shape 1: tuples of the exact scalar class
    leaf = _leaf(_INT64, 1, 1, 2, (2).to_bytes(8, "little")) + struct.pack(
        "<4q", 1, 2, 3, 4
    )
    out = _read_stream(b"Pm\x00" + leaf)
    assert all(type(e) is tuple for e in out)
    assert all(type(v) is np.int64 for e in out for v in e)
    assert out == [(1, 2), (3, 4)]


def test_read_stream_map_result_pickled_leaf():
    body = pickle.dumps(["a", 1], protocol=4)
    leaf = b"\x01" + len(body).to_bytes(8, "little") + body
    assert _read_stream(b"Pm\x00" + leaf) == ["a", 1]


def test_read_stream_map_result_corrupt():
    import struct

    leaf = _leaf(_REAL, 0, 0, 1, (2).to_bytes(8, "little")) + struct.pack(
        "<2d", 1.5, 2.5
    )
    # a bad form byte
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x07")
    # a bad kind byte
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x00\x03")
    # a truncated typed leaf
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x00" + leaf[:6])
    # a truncated pickled leaf
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x00\x01\x04")
    # m == 0
    bad = _leaf(_REAL, 0, 0, 0, (2).to_bytes(8, "little")) + struct.pack(
        "<2d", 1.5, 2.5
    )
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x00" + bad)
    # a shape or np byte out of range
    bad = _leaf(_REAL, 2, 0, 1, (1).to_bytes(8, "little")) + struct.pack(
        "<d", 1.0
    )
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x00" + bad)
    # a count that overruns the span
    bad = _leaf(_REAL, 0, 0, 1, (100).to_bytes(8, "little")) + struct.pack(
        "<d", 1.0
    )
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x00" + bad)
    # a wire type with no element size (STR)
    bad = _leaf(16, 0, 0, 1, (1).to_bytes(8, "little")) + b"12345678"
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x00" + bad)
    # a truncated hist
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x01" + (1).to_bytes(4, "little") + b"\x00" * 8)
    # the hist complete but the has_vals byte missing
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x01" + (1).to_bytes(4, "little") + b"\x00" * 16)
    # a has_vals byte out of range
    stream = (
        b"Pm\x01"
        + (1).to_bytes(4, "little")
        + (0).to_bytes(8, "little")
        + (2).to_bytes(8, "little")
        + b"\x02"
    )
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(stream)
    # trailing bytes past the value
    with pytest.raises(pymizu.MizuError, match="corrupt"):
        _read_stream(b"Pm\x00" + leaf + b"x")
