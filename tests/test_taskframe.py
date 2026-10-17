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
from functools import partial

import numpy as np
import pytest
from tests.helpers import (
    BufferFailAfterFirst,
    array_sum,
    echo,
    frame_and_buffer,
    frames_report,
    is_readonly,
    square,
    sum_dict,
    sum_mixed,
    sum_pair,
    sum_pair_flaky,
    sum_two,
)

import pymizu
from pymizu._pymizu import _frame_rebuild, _read_stream, _task_frame


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
def test_abandoned_bufref_stage_rolls_back(pool):
    # a by-reference leaf's retain followed by a write-pass failure: the
    # abandoned checkout rolls back, so the pickle fallback reuses the
    # region instead of orphaning it for a fresh one (P-17)
    def reuse():
        return next(
            s["spill_reuse"]
            for s in pool.stats()["submitters"]
            if s["pid"] == os.getpid()
        )

    # seed the free list with one region (a big pickled payload: its
    # surrender at collect is deterministic)
    assert pool.submit(len, "x" * 200000).collect(timeout=15) == 200000
    before = reuse()
    big = np.full(16000, 1.0)  # 125 KiB: past the zero-copy floor
    flaky = BufferFailAfterFirst(np.full(10, 2.0))
    t = pool.submit(sum_two, big, flaky)
    # the pickle fallback pops the rolled-back BUFREF region: one
    # published spill frame, marked reused (an orphaned checkout would
    # force a fresh create: reuse delta 0)
    assert reuse() - before == 1
    assert t.collect(timeout=15) == 16020.0


def test_container_with_buffer_leaves(pool):
    out = pool.submit(sum_mixed, [np.arange(4.0), 5]).collect(timeout=5)
    assert out == 11.0


def test_dict_arg_non_str_key_pickles(pool):
    # a task frame's dict keys must be codec strings: anything else
    # declines to the whole-tuple pickle
    assert pool.submit(len, {1: "x", 2: "y"}).collect(timeout=5) == 2


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


def test_container_leaf_write_decline_pickles(pool):
    # a container element whose write-pass buffer export fails (the size
    # pass passed): the whole task tuple pickles, values exact
    a = np.full(16000, 1.0)
    flaky = BufferFailAfterFirst(np.full(10, 2.0))
    out = pool.submit(sum_pair_flaky, [a, flaky]).collect(timeout=10)
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
