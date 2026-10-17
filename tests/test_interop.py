"""The 'I' interchange codec: the golden corpus drives the reader and
writer in-process (tests/interop-corpus/ rides the libmizu pin), and the
feature coverage beyond the corpus lives here — the pinned foreign
staging order, the decline set, the Frame export machinery, and the
Arrow stream front-end."""

import datetime
import os
import pathlib
import pickle
import struct
import subprocess
import sys
import textwrap
import warnings

import pytest
from tests import ix_notation as ixn
from tests.helpers import BufferFailAfterFirst, foreign_pair

import pymizu
from pymizu import _pymizu

np = pytest.importorskip("numpy", reason="numpy not installed")
pa = pytest.importorskip("pyarrow", reason="pyarrow not installed")

CORPUS_DIR = pathlib.Path(__file__).parent / "interop-corpus"
CASES = ixn.load_cases(CORPUS_DIR / "cases.txt")
CORPUS = ixn.load_corpus(CORPUS_DIR / "corpus.txt")

WARN_ON_READ = {"i64v-na", "list-i64-na"}  # the sentinel-carrying reads
SKIP_WRITE_DECLINE = {"wd-dup-names"}  # a Python dict dedupes by
# construction; the decline is
# unreachable (R covers it)


def _read(data):
    if isinstance(data, str):
        data = bytes.fromhex(data)
    return _pymizu._read_stream(data)


def _write(x):
    return _pymizu._write_stream(x).hex()


def test_golden_corpus():
    for case in CASES:
        cid, kind, langs = case["id"], case["kind"], case["langs"]
        if langs not in ("all", "PY"):
            continue
        value = case["value"]
        if kind == "read-err":
            with pytest.raises(pymizu.MizuError):
                # err-taskdec- rows ride the task decoder, the rest the
                # value reader
                if cid.startswith("err-taskdec-"):
                    _pymizu._read_task(bytes.fromhex(CORPUS[cid]))
                else:
                    _read(CORPUS[cid])
            continue
        if kind == "task":
            home = ixn.parse(value)
            got = _pymizu._read_task(bytes.fromhex(CORPUS[cid]))
            assert tuple(got[:4]) == (
                home.target,
                home.kind,
                home.ident,
                home.code,
            ), cid
            assert ixn.ix_same(got[4], home.positional), cid
            assert ixn.ix_same(got[5], home.named), cid
            assert (
                _pymizu._write_task(
                    home.code,
                    home.kind,
                    tuple(home.positional),
                    home.named,
                    home.target,
                    home.ident,
                ).hex()
                == CORPUS[cid]
            ), cid
            continue
        if kind == "write-decline":
            if cid in SKIP_WRITE_DECLINE:
                continue
            with pytest.raises(pymizu.DeclinedError):
                _write(ixn.parse(value))
            continue
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always")
            got = _read(CORPUS[cid])
        if cid in WARN_ON_READ:
            assert any(w.category is RuntimeWarning for w in seen), cid
        else:
            assert not seen, cid
        if kind == "dec":
            home = ixn.parse(value.split(" => ", 1)[-1])
            assert ixn.ix_same(got, home), cid
        elif kind == "rt":
            home = ixn.parse(value)
            assert ixn.ix_same(got, home), cid
            if isinstance(home, ixn.ExpectedError):
                assert (
                    _pymizu._write_err(
                        home.remote_type,
                        home.message,
                        home.detail,
                        index=home.index,
                    ).hex()
                    == CORPUS[cid]
                ), cid
            else:
                assert _write(got) == CORPUS[cid], cid
        elif kind == "enc":
            home = ixn.parse(value)
            assert _write(home) == CORPUS[cid], cid
        else:
            raise AssertionError(f"unknown kind {kind}")


def test_str_list_writes_strv():
    # a list[str | None] crosses as the 0x0b string vector at every size —
    # the home the MIZS tier picks past the zc floor (tier choice by size
    # never changes the type R receives)
    assert _write(["a", "bc"]) == CORPUS["py-strv-plain"]
    assert _write(["a", None, "c"]) == CORPUS["py-strv"]
    # length 1 pins the 0x04 scalar form (R's character(1))
    assert _write(["a"]) == "4901040100000061"
    assert _write(["a"]) == _write("a")
    # an all-None list is not a string vector: the 0x0c list of nils (R's
    # list(NULL), the depth-64 pin) at every size
    assert _write([None]) == "49010c010000000000000000"
    assert _write([None, None]) == "49010c02000000000000000000"
    # an empty list and a tuple keep the 0x0c list form
    assert _write([]) == CORPUS["list-empty"]
    assert _write(("a", "b")) == (
        "49010c0200000000000000040100000061040100000062"
    )


def test_scalar_and_container_roundtrips():
    h, p = foreign_pair()
    for x in [
        None,
        True,
        1,
        -1,
        1.5,
        1 + 2j,
        "héllo ✓",
        [1, "a", [True, 2.5]],
        {"a": 1, "b": {"z": None}},
        [1, "a", [True, 2.5]],
        {"a": 1, "b": {"z": None}},
        2**53,
        -(2**53),
    ]:
        assert h.send(x) is True
        assert p.recv(5) == x
    # top-level bytes stage raw and read back as a uint8 array
    assert h.send(b"\x00\xff") is True
    assert np.array_equal(p.recv(5), [0, 255])
    # scalar NAs read as None (documented lossy)
    p.destroy()
    h.destroy()


def test_write_runner_golden():
    # the kind-2 (map runner) task stream, pinned byte-for-byte: the
    # 0x12 task tag (target, kind 2, ident), the region name, the
    # ordinal/generation word, then nil or the int64 (seed, offset) pair
    got = _pymizu._write_runner("/mizu_abc", 3 << 32 | 1, (42, 7), 2, 0x1234)
    assert got.hex() == (
        "49011202020000341200000000000004090000002f6d697a755f616263"
        "0201000000030000000e02000000000000002a000000000000000700000000000000"
    )
    got = _pymizu._write_runner("/mizu_abc", 5, None, 2, 0)
    assert got.hex() == (
        "49011202020000000000000000000004090000002f6d697a755f616263"
        "02050000000000000000"
    )
    # the generic task decode refuses kind 2 (the runner half decodes in
    # the exec hook)
    with pytest.raises(pymizu.MizuError, match="unsupported task kind"):
        _pymizu._read_task(got)


def test_write_runner_validation():
    with pytest.raises(TypeError, match="_write_runner expects"):
        _pymizu._write_runner(b"n", 0, None, 2, 0)
    with pytest.raises(ValueError, match="language byte"):
        _pymizu._write_runner("/mizu_abc", 0, None, 256, 0)
    with pytest.raises(TypeError):
        _pymizu._write_runner("/mizu_abc", "x", None, 2, 0)


def test_task_hook_validation():
    # the test-only hooks' argument gates
    with pytest.raises(TypeError, match="_runner_frame expects"):
        _pymizu._runner_frame(1, 2, None)
    with pytest.raises(TypeError, match="_call_frame expects"):
        _pymizu._call_frame()
    with pytest.raises(TypeError, match="_write_task expects"):
        _pymizu._write_task("os.getpid", 0, (), {}, 1)
    with pytest.raises(TypeError):
        _pymizu._write_task("os.getpid", "x", (), {}, 1, 0)
    with pytest.raises(ValueError, match="language byte"):
        _pymizu._write_task("os.getpid", 0, (), {}, -1, 0)
    with pytest.raises(TypeError, match="_map_desc_write expects"):
        _pymizu._map_desc_write("os.getpid", 0, (), {})
    with pytest.raises(ValueError, match="language byte"):
        _pymizu._map_desc_write("os.getpid", 0, (), {}, None, 256)
    for hook in (
        _pymizu._read_task,
        _pymizu._map_desc_read,
        _pymizu._read_stream,
    ):
        with pytest.raises(TypeError, match="expected a bytes stream"):
            hook(1)
    with pytest.raises(TypeError):
        _pymizu._write_err(1, "m", "d")
    # a budget past the stack buffer spills to the heap
    assert _pymizu._write_err("t", "m", "d", budget=4096) != b""


def _tree(n):
    return [
        {"id": i, "name": f"item-{i:04d}", "vals": [i, i * 1.5, True, None]}
        for i in range(n)
    ]


def test_spilled_streams_roundtrip_on_each_carrier():
    # the one-walk stager's carriers: the corpus pins the stream bytes
    # (_write_stream is the two-pass oracle); these rows pin each
    # carrier's delivery — inline (one walk), an arena chunk, an SHM_RAW
    # region, and the n == inline_max boundary staying INLINE
    h, p = foreign_pair()  # slot_size 1024 (inline budget 1008), 64 KB arena
    INLINE_MAX = 1024 - 16
    boundary = "x" * (INLINE_MAX - len(_pymizu._write_stream("")))
    assert len(_pymizu._write_stream(boundary)) == INLINE_MAX
    cases = [
        ({"a": [1, "x", [True, None]], "b": {"z": 2.5}}, INLINE_MAX),
        (boundary, INLINE_MAX),
        (_tree(60), 1 << 16),
        (_tree(1000), None),
    ]
    for x, band in cases:
        size = len(_pymizu._write_stream(x))
        if band is None:
            assert size > (1 << 16)
        else:
            assert size <= band
        assert h.send(x) is True
        assert p.recv(5) == x
    p.destroy()
    h.destroy()


def test_arena_pressure_forces_the_region_carrier():
    # pipelined sends past the 64 KB arena's hold: the overflow messages
    # take the SHM_RAW reservation and read back intact
    h, p = foreign_pair()
    x = {"vals": list(range(3000))}
    size = len(_pymizu._write_stream(x))
    assert size < (1 << 16)
    n = (1 << 16) // size + 4  # cumulatively past the arena
    for _ in range(n):
        assert h.send(x) is True
    for _ in range(n):
        assert p.recv(5) == x
    p.destroy()
    h.destroy()


def test_decline_mid_write_leaves_the_slot_unwedged():
    # a decline found past the inline budget: the payload writes up to it
    # are scratch — nothing commits and the channel stages on
    h, p = foreign_pair()
    bad = [{"id": i} for i in range(100)] + [object()]
    assert len(_pymizu._write_stream(bad[:-1])) > 1024 - 16
    with pytest.raises(pymizu.DeclinedError) as ei:
        h.send(bad)
    assert ei.value.path == "x[100]"
    assert h.send({"ok": 1}) is True
    assert p.recv(5) == {"ok": 1}
    p.destroy()
    h.destroy()


def test_cvt_warning_raises_after_the_spilled_write():
    # a strided uint64 array skips the raw tiers (non-contiguous) and
    # stages through the 'I' writer's conversion body: past the inline
    # budget the reservation takes the full write before the warning
    # raises — raised as an error it rolls the reservation back, never a
    # half-written chunk
    h, p = foreign_pair()
    a = (np.arange(4000, dtype=np.uint64) + 2**62)[::2]
    with pytest.warns(RuntimeWarning, match="beyond"):
        assert len(_pymizu._write_stream(a)) > 1024 - 16
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(RuntimeWarning, match="beyond"):
            h.send(a)
    assert h.send({"ok": 1}) is True
    assert p.recv(5) == {"ok": 1}
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        assert h.send(a) is True
    assert any(w.category is RuntimeWarning for w in seen)
    got = p.recv(5)
    assert isinstance(got, np.ndarray) and np.isnan(got).all()
    p.destroy()
    h.destroy()


def test_top_level_scalars_stay_scalars():
    # a Python scalar crosses as the scalar tag, a length-1 array as the
    # vector tag (the pinned split)
    h, p = foreign_pair()
    assert h.send(1.5) is True
    assert p.recv(5) == 1.5
    a = np.array([1.5])
    assert h.send(a) is True
    got = p.recv(5)
    assert isinstance(got, np.ndarray) and got.dtype == np.float64
    i = np.array([5], dtype=np.int64)
    assert h.send(i) is True
    got = p.recv(5)
    assert isinstance(got, np.ndarray) and got.dtype == np.int64
    p.destroy()
    h.destroy()


def test_numpy_scalar_conversions_decode_by_kind():
    # one converter for every row (the uint32-as-float32 misread's fix):
    # the scalar path picks only the output tag per wire type
    assert _read(_pymizu._write_stream(np.uint32(5))) == 5.0
    assert _read(_pymizu._write_stream(np.array(5, dtype=np.uint32))) == 5.0
    assert _read(_pymizu._write_stream(np.float32(1.5))) == 1.5
    for dt, val in [
        (np.bool_, True),
        (np.int8, -5),
        (np.uint8, 200),
        (np.int16, -300),
        (np.uint16, 60000),
        (np.int32, -70000),
        (np.int64, -(2**60)),
        (np.float16, 1.5),
        (np.float64, 1.5),
        (np.complex64, 1 + 2j),
        (np.complex128, 1 + 2j),
    ]:
        assert _read(_pymizu._write_stream(dt(val))) == val
        assert _read(_pymizu._write_stream(np.array(val, dtype=dt))) == val


def test_dim_shape_orders_and_strides():
    m = np.arange(6, dtype=np.float64).reshape(2, 3)
    got = _read(_pymizu._write_stream(m))
    assert got.flags.f_contiguous and np.array_equal(got, m)
    mf = np.asfortranarray(m)
    got = _read(_pymizu._write_stream(mf))
    assert got.flags.f_contiguous and np.array_equal(got, mf)
    a3 = np.arange(24, dtype=np.int32).reshape(2, 3, 4)
    got = _read(_pymizu._write_stream(a3))
    assert got.shape == (2, 3, 4) and np.array_equal(got, a3)
    # strided and negatively-strided normalize (top level)
    s = np.arange(12, dtype=np.float64).reshape(3, 4)[:, ::2]
    assert np.array_equal(_read(_pymizu._write_stream(s)), s)
    neg = np.arange(6, dtype=np.float64).reshape(2, 3)[::-1]
    assert np.array_equal(_read(_pymizu._write_stream(neg)), neg)
    # an integer64 matrix and a bool matrix
    im = np.arange(4, dtype=np.int64).reshape(2, 2)
    got = _read(_pymizu._write_stream(im))
    assert got.dtype == np.int64 and np.array_equal(got, im)
    bm = np.array([[True, False], [False, True]])
    got = _read(_pymizu._write_stream(bm))
    assert got.dtype == np.bool_ and np.array_equal(got, bm)


def test_dim_shape_tiled_gather():
    # the 2-D transpose fast path, exercised across tile boundaries (32)
    base = np.arange(70 * 41) % 251
    for dt in (np.uint8, np.int32, np.float64, np.complex128):
        m = base.astype(dt).reshape(70, 41)  # C-order
        got = _read(_pymizu._write_stream(m))
        assert got.flags.f_contiguous and np.array_equal(got, m)
    # a conversion row (float32 -> REAL) through the same loops
    m = base.astype(np.float32).reshape(70, 41)
    got = _read(_pymizu._write_stream(m))
    assert got.dtype == np.float64
    assert np.array_equal(got, m.astype(np.float64))


def test_nested_dim_leaf():
    got = _read(_pymizu._write_stream([np.arange(6).reshape(2, 3)]))
    assert len(got) == 1 and got[0].shape == (2, 3)
    assert np.array_equal(got[0], np.arange(6).reshape(2, 3))


def test_float16_widens_to_double():
    a = np.array([1.5, -2.25, 0.0, np.inf, np.nan], dtype=np.float16)
    got = _read(_pymizu._write_stream(a))
    assert got.dtype == np.float64
    assert np.array_equal(got[:4], a[:4].astype(np.float64))
    assert np.isnan(got[4])
    # subnormals and the smallest normal decode exactly
    a = np.array([6e-8, 6.1e-5, 65504.0], dtype=np.float16)
    got = _read(_pymizu._write_stream(a))
    assert np.array_equal(got, a.astype(np.float64))
    # scalar, nested leaf, and the 2-D gather all take the same row
    assert _read(_pymizu._write_stream(np.float16(1.5))) == 1.5
    got = _read(
        _pymizu._write_stream({"a": np.array([1.5], dtype=np.float16)})
    )
    assert got["a"].dtype == np.float64 and got["a"][0] == 1.5
    m = np.arange(6, dtype=np.float16).reshape(2, 3)
    got = _read(_pymizu._write_stream(m))
    assert got.dtype == np.float64
    assert np.array_equal(got, m.astype(np.float64))


def test_nested_uint64_converts_with_a_warning():
    got = _read(
        _pymizu._write_stream({"a": np.array([1, 2], dtype=np.uint64)})
    )
    assert np.array_equal(got["a"], [1.0, 2.0])
    with pytest.warns(RuntimeWarning, match="beyond"):
        got = _read(
            _pymizu._write_stream([np.array([2**62], dtype=np.uint64)])
        )
    assert np.isnan(got[0][0])


def test_buffer_conversion_rows():
    # the conversion table's prefixed-longlong and uint64 rows: ctypes
    # buffers export '<q' and array's uint64 exports 'Q', both missing the
    # identity dtype map, so the foreign buffer tier converts them
    import array
    import ctypes

    h, p = foreign_pair()
    try:
        mv = memoryview((ctypes.c_longlong * 3)(1, -2, 3))
        assert h.send(mv) is True
        got = p.recv(10)
        assert got.dtype == np.int64
        assert got.tolist() == [1, -2, 3]
        with pytest.warns(RuntimeWarning, match="beyond"):
            assert h.send(array.array("Q", [1, 2**60])) is True
        got = p.recv(10)
        assert got.dtype == np.float64
        assert got[0] == 1.0 and np.isnan(got[1])
    finally:
        p.destroy()
        h.destroy()


def test_temporal_shapes():
    # datetime64 both directions: [W] converts to days (exact), the time
    # units to epoch-seconds doubles (sub-us resolution rounds)
    a = np.array([1, 20000], dtype="datetime64[D]")
    got = _read(_pymizu._write_stream(a))
    assert np.array_equal(got.view(np.int64), a.view(np.int64))
    a = np.array([1, 20000], dtype="datetime64[W]")
    got = _read(_pymizu._write_stream(a))
    assert np.array_equal(got.view(np.int64), a.view(np.int64) * 7)
    scales = {
        "h": 3600e6,
        "m": 60e6,
        "s": 1e6,
        "ms": 1e3,
        "us": 1.0,
        "ns": 1e-3,
    }
    for unit, factor in scales.items():
        a = np.array([1, 20000], dtype=f"datetime64[{unit}]")
        got = _read(_pymizu._write_stream(a))
        expected = np.round(a.view(np.int64) * factor).astype(np.int64)
        assert got.dtype == np.dtype("datetime64[us]"), unit
        assert np.array_equal(got.view(np.int64), expected), unit
    nat = np.array([1, np.datetime64("NaT", "us")], dtype="datetime64[us]")
    got = _read(_pymizu._write_stream(nat))
    assert np.array_equal(got.view(np.int64), nat.view(np.int64))
    # stdlib date / datetime scalars home as length-1 shapes
    got = _read(_pymizu._write_stream(datetime.date(2022, 3, 21)))
    assert got == np.array(["2022-03-21"], dtype="datetime64[D]")[0]
    got = _read(
        _pymizu._write_stream(datetime.datetime(2023, 11, 14, 22, 13, 20))
    )
    assert got == np.array(["2023-11-14T22:13:20"], dtype="datetime64[us]")[0]
    # an aware datetime converts to its UTC instant
    import zoneinfo

    try:
        zi = zoneinfo.ZoneInfo("Europe/Paris")
    except zoneinfo.ZoneInfoNotFoundError:
        pytest.skip("no IANA time zone database (install tzdata)")
    got = _read(
        _pymizu._write_stream(
            datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=zi)
        )
    )
    assert got == np.array(["2023-11-14T21:13:20"], dtype="datetime64[us]")[0]


def test_temporal_declines():
    for bad in [
        np.array([1, 2], dtype="datetime64[Y]"),
        np.array([1, 2], dtype="datetime64[M]"),
        np.array([1], dtype="m8[Y]"),
        np.array([1], dtype="m8[M]"),
        np.array([1], dtype="m8[ps]"),
    ]:
        with pytest.raises(pymizu.DeclinedError):
            _pymizu._write_stream(bad)


def test_timedelta_shapes():
    # timedelta64 both directions: any non-calendar unit converts to us
    # (a double-seconds hop), NaT kept
    scales = {
        "W": 604800e6,
        "D": 86400e6,
        "h": 3600e6,
        "m": 60e6,
        "s": 1e6,
        "ms": 1e3,
        "us": 1.0,
        "ns": 1e-3,
    }
    for unit, factor in scales.items():
        a = np.array([1, 20000], dtype=f"m8[{unit}]")
        got = _read(_pymizu._write_stream(a))
        expected = np.round(a.view(np.int64) * factor).astype(np.int64)
        assert got.dtype == np.dtype("m8[us]"), unit
        assert np.array_equal(got.view(np.int64), expected), unit
    nat = np.array([1, np.timedelta64("NaT", "s")], dtype="m8[s]")
    got = _read(_pymizu._write_stream(nat))
    assert got[0] == np.timedelta64(1, "s") and np.isnat(got[1])
    # a timedelta64 scalar exports a uint8 byte view — the value crosses
    got = _read(_pymizu._write_stream(np.timedelta64(250, "ms")))
    assert got == np.timedelta64(250000, "us")
    # stdlib timedelta: the normalized (days, seconds, microseconds) form
    got = _read(
        _pymizu._write_stream(
            datetime.timedelta(days=1, seconds=30, microseconds=500)
        )
    )
    assert got == np.timedelta64(86430000500, "us")


def test_frame_scalar_int_column():
    # an 'I' frame carrying a scalar INT column: the column must adopt its
    # value buffer (a NULL there crashed every reader of the column)
    def key(s):
        return len(s).to_bytes(4, "little") + s.encode()

    def sval(s):
        return b"\x04" + key(s)

    s = bytearray(b"I\x01")
    s += b"\x0f"  # ATTR
    s += b"\x0c" + (1).to_bytes(8, "little")  # LIST n=1
    s += b"\x02" + (5).to_bytes(8, "little", signed=True)  # INT 5
    s += b"\x0d" + (3).to_bytes(8, "little")  # DICT n=3
    s += key("names") + sval("a")
    s += key("class") + sval("data.frame")
    s += key("row.names") + b"\x02" + (1).to_bytes(8, "little", signed=True)
    f = _pymizu._read_stream(bytes(s))
    d = f.to_dict()
    assert d["a"].tolist() == [5]
    assert _pymizu._write_stream(f)  # re-emits without a NULL memcpy


def test_frame_adjacent_duplicate_names_rejected():
    # no real producer sends duplicate names; a torn stream could, and an
    # adjacent pair must trip the same check as a non-adjacent one
    def key(s):
        return len(s).to_bytes(4, "little") + s.encode()

    def sval(s):
        return b"\x04" + key(s)

    s = bytearray(b"I\x01")
    s += b"\x0f"  # ATTR
    s += b"\x0c" + (2).to_bytes(8, "little")  # LIST n=2
    s += b"\x02" + (1).to_bytes(8, "little", signed=True)  # INT 1
    s += b"\x02" + (2).to_bytes(8, "little", signed=True)  # INT 2
    s += b"\x0d" + (3).to_bytes(8, "little")  # DICT n=3
    names = b"\x0b" + (2).to_bytes(8, "little") + key("a") + key("a")
    s += key("names") + names
    s += key("class") + sval("data.frame")
    s += key("row.names") + b"\x02" + (1).to_bytes(8, "little", signed=True)
    with pytest.raises(pymizu.MizuError, match="duplicate column names"):
        _pymizu._read_stream(bytes(s))


def test_arrow_temporal_columns():
    h, p = foreign_pair()
    t = pa.table(
        {
            "d": pa.array([19000, None], type=pa.date32()),
            "t": pa.array(
                [1700000000000000, None], type=pa.timestamp("us", tz="UTC")
            ),
            "z": pa.array(
                [1700000000000000, 1700000001000000],
                type=pa.timestamp("us", tz="America/New_York"),
            ),
            "u": pa.array([90, None], type=pa.duration("s")),
            "n": pa.array([250000, 1000000], type=pa.duration("ns")),
        }
    )
    assert h.send(t) is True
    f = p.recv(5)
    d = f.to_dict()
    assert d["d"].dtype == np.dtype("datetime64[D]")
    assert d["t"].dtype == np.dtype("datetime64[us]")
    assert d["z"].dtype == np.dtype("datetime64[us]")
    assert d["u"].dtype == np.dtype("timedelta64[us]")
    assert d["u"][0] == np.timedelta64(90, "s") and np.isnat(d["u"][1])
    assert d["n"][0] == np.timedelta64(250, "us")
    assert d["n"][1] == np.timedelta64(1, "ms")
    # the frame's own Arrow export speaks duration[us]
    schema = pa.table(f).schema
    assert schema.field("u").type == pa.duration("us")
    assert schema.field("n").type == pa.duration("us")
    # a named zone rides the column's metadata and the write-back
    assert _pymizu._write_stream(f)  # re-emits without loss
    p.destroy()
    h.destroy()


def test_arrow_duration_single():
    h, p = foreign_pair()
    assert (
        h.send(pa.chunked_array([[250, None], [500]], type=pa.duration("ms")))
        is True
    )
    got = p.recv(5)
    assert got.dtype == np.dtype("timedelta64[us]")
    assert got[0] == np.timedelta64(250, "ms") and np.isnat(got[1])
    assert got[2] == np.timedelta64(500, "ms")
    p.destroy()
    h.destroy()


def test_declined_error_paths():
    for bad, why in [
        ({1, 2}, "set"),
        (np.ma.MaskedArray([1.0, 2.0], mask=[True, False]), "buffer subclass"),
        ([np.ma.MaskedArray([1.0])], "buffer subclass"),
        (np.array([["a"]], dtype=object), "dtype"),
        ([np.arange(4)[::2]], "strided"),
        ({1: "x"}, "non-str dict key"),
        ("\ud800", "UTF-8"),
        (2**90, "int64"),
    ]:
        h, p = foreign_pair()
        with pytest.raises(pymizu.DeclinedError, match=why):
            h.send(bad)
        p.destroy()
        h.destroy()


def test_declined_error_record():
    h, p = foreign_pair()
    with pytest.raises(pymizu.DeclinedError) as ei:
        h.send({"ok": [1, {1, 2}]})
    assert ei.value.path == "x['ok'][1]"
    assert "set" in ei.value.reason
    p.destroy()
    h.destroy()


def test_declined_error_is_typeerror():
    assert issubclass(pymizu.DeclinedError, TypeError)


def test_foreign_staging_order_pyarrow_array_raw():
    # a numeric pyarrow Array stages on the raw tiers, not 'I' (the pinned
    # foreign order); a ChunkedArray takes the stream front-end
    h, p = foreign_pair()
    h.send(pa.array([1, 2, 3], type=pa.int32()))
    got = p.recv(5)
    assert np.array_equal(np.asarray(got), [1, 2, 3])
    assert got.dtype == np.int32
    h.send(pa.chunked_array([[1, 2], [3]]))
    assert np.array_equal(np.asarray(p.recv(5)), [1, 2, 3])
    p.destroy()
    h.destroy()


def test_polars_series_and_categorical():
    pl = pytest.importorskip("polars")
    h, p = foreign_pair()
    h.send(pl.Series(["x", "yy", None]))
    assert p.recv(5) == ["x", "yy", None]
    h.send(pl.Series("c", ["u", "v", "u"], dtype=pl.Categorical))
    assert p.recv(5) == ["u", "v", "u"]
    with pytest.raises(pymizu.DeclinedError, match="pl.Categorical"):
        h.send(pl.Series("e", ["a"], dtype=pl.Enum(["a", "b"])))
    p.destroy()
    h.destroy()


def test_polars_frame_string_view_columns():
    pl = pytest.importorskip("polars")
    h, p = foreign_pair()
    df = pl.DataFrame(
        {
            "s": ["a", "b", None],
            "x": [1, 2, 3],
            "c": pl.Series(["u", "v", "u"], dtype=pl.Categorical),
        }
    )
    h.send(df)
    f = p.recv(5)
    d = f.to_dict()
    assert d["s"] == ["a", "b", None]
    assert d["x"].tolist() == [1, 2, 3]
    assert d["c"] == ["u", "v", "u"]
    p.destroy()
    h.destroy()


def test_multibatch_frame():
    h, p = foreign_pair()
    t = pa.table(
        {
            "a": pa.chunked_array([[1, 2], [3, 4]]),
            "b": pa.chunked_array([["x", "y"], ["z", "w"]]),
            "d": pa.chunked_array(
                [
                    pa.DictionaryArray.from_arrays([0, 0], ["u", "v"]),
                    pa.DictionaryArray.from_arrays([1, 0], ["u", "v"]),
                ]
            ),
        }
    )
    h.send(t)
    f = p.recv(5)
    d = f.to_dict()
    assert d["a"].tolist() == [1, 2, 3, 4]
    assert d["b"] == ["x", "y", "z", "w"]
    assert d["d"] == ["u", "u", "v", "u"]
    p.destroy()
    h.destroy()


def test_region_frame_with_strings_resends():
    # a region-backed Frame's string column (FCOL_STR64) walks the MIZS
    # block's i64 offsets/validity through the 'I' writer — an explicit
    # arm, values and NAs crossing exact (P-02)
    s = ["x" * 10] * 8000
    s[17] = None
    tbl = pa.table({"s": s, "v": np.arange(8000.0)})
    h1, p1 = foreign_pair(7)  # MIZS | ATTRS | MIZL: the frame arrives
    h2, p2 = foreign_pair()  # region-backed; caps-0: the 'I' writer path
    try:
        assert h1.send(tbl) is True
        f = p1.recv(5)
        assert h2.send(f) is True
        d = p2.recv(5).to_dict()
        assert len(d["s"]) == 8000
        assert d["s"][0] == "x" * 10
        assert d["s"][17] is None
        assert d["s"][-1] == "x" * 10
        assert float(d["v"][-1]) == 7999.0
        # nested in a list: the same writer path even with caps
        assert h1.send([f]) is True
        [g] = p1.recv(5)
        assert len(g) == 8000
    finally:
        for x in (p1, h1, p2, h2):
            x.destroy()


def test_corrupt_and_unknown_streams():
    for hexstream in [
        "49017f",  # unknown tag
        "490200",  # a newer version
        "4901020102",  # truncated
        "49010000",  # trailing bytes
        "49010d0100000000000000ffffffff",  # a dict key of length -1
        "49010f0f",  # an attr wraps an attr
        "49010401000000ff",  # invalid UTF-8
    ]:
        with pytest.raises(pymizu.MizuError, match="pymizu"):
            _read(hexstream)


def test_read_stream_codec_corrupt():
    # the compact codec's own corrupt arms: an empty stream, an unknown
    # scalar tag inside a container, and invalid UTF-8 (a real decode
    # error, not the generic corrupt verdict)
    with pytest.raises(pymizu.MizuError, match="corrupt payload slot"):
        _pymizu._read_stream(b"")
    with pytest.raises(pymizu.MizuError, match="corrupt payload slot"):
        _pymizu._read_stream(b"P" + b"l" + (1).to_bytes(4, "little") + b"z")
    with pytest.raises(UnicodeDecodeError):
        _pymizu._read_stream(
            b"P"
            + b"l"
            + (1).to_bytes(4, "little")
            + b"s"
            + (1).to_bytes(4, "little")
            + b"\xff"
        )


def test_random_corrupt_streams():
    # the cursor declines junk cleanly (the _read_stream hook doubles as
    # the fuzz harness): never a crash, always an informative error
    rng = np.random.default_rng(42)
    for _ in range(200):
        n = int(rng.integers(0, 40))
        blob = b"\x49\x01" + bytes(rng.integers(0, 256, n, dtype=np.uint8))
        try:
            _pymizu._read_stream(blob)
        except pymizu.MizuError:
            pass
        except Exception as e:  # noqa: BLE001 — anything else is a bug
            raise AssertionError(f"unexpected {type(e).__name__}: {e}") from e
    for _ in range(50):
        n = int(rng.integers(0, 40))
        blob = bytes(rng.integers(0, 256, n, dtype=np.uint8))
        with pytest.raises(pymizu.MizuError):
            _pymizu._read_stream(blob)


def test_depth_cap():
    x = None
    for _ in range(64):
        x = [x]
    assert _read(_pymizu._write_stream(x)) == x
    x = None
    for _ in range(65):
        x = [x]
    with pytest.raises(pymizu.DeclinedError, match="depth cap"):
        _pymizu._write_stream(x)


@pytest.mark.skipif(
    sys.version_info < (3, 12), reason="PEP 688 __buffer__ is CPython 3.12+"
)
def test_a_declined_write_pass_fails_the_send():
    # an exporter whose later __buffer__ call fails declines walk 2
    # mid-write: the send must fail, never stage a partial stream (P-07)
    class Flaky:
        def __init__(self, a):
            self.a = a
            self.calls = 0

        def __buffer__(self, flags):
            self.calls += 1
            if self.calls == 4:
                raise BufferError("export refused")
            return memoryview(self.a)

        def __release_buffer__(self, view):
            view.release()

    h, p = foreign_pair()
    try:
        assert h.send([b"A" * 2000, np.full(10, 7.0), [9, 9, 9]]) is True
        p.recv(5)
        with pytest.raises(pymizu.MizuError, match="interop write mismatch"):
            h.send([b"B" * 2000, Flaky(np.full(10, 1.0)), [1, 2, 3]])
        # the ring still flows
        assert h.send([1, 2]) is True
        assert p.recv(5) == [1, 2]
    finally:
        p.destroy()
        h.destroy()


def test_attr_shapes_decline_at_the_depth_cap():
    # the attr shapes count their cursor frames: a value the reader would
    # reject declines at the send instead (P-10)
    x = np.zeros((2, 2))
    for _ in range(61):
        x = [x]
    got = _read(_write(x))
    for _ in range(61):
        assert isinstance(got, list) and len(got) == 1
        got = got[0]
    assert isinstance(got, np.ndarray)
    assert np.array_equal(got, np.zeros((2, 2)))
    for depth in (62, 63):
        y = np.zeros((2, 2))
        for _ in range(depth):
            y = [y]
        with pytest.raises(pymizu.DeclinedError, match="depth cap"):
            _pymizu._write_stream(y)


def test_declined_path_decodes_past_truncated_utf8():
    # a long non-ASCII key's repr truncates mid-sequence in the path
    # buffer: the DeclinedError still raises (P-13)
    with pytest.raises(pymizu.DeclinedError) as ei:
        _pymizu._write_stream({"€" * 100: object()})
    assert isinstance(ei.value.path, str)
    assert "€" in ei.value.path


def test_frame_export_machinery():
    pl = pytest.importorskip("polars")
    # a three-column frame: factor and int64 columns, Arrow dictionary out
    f = _read(bytes.fromhex(CORPUS["frame-3col"]))
    df = pl.DataFrame(f)
    assert df.schema["n"] == pl.Float64
    assert df.schema["f"] == pl.Categorical
    assert df.schema["i"] == pl.Int64
    assert df.to_dicts() == [
        {"n": 1.5, "f": "f", "i": 5},
        {"n": 2.5, "f": None, "i": None},
    ]
    t = pa.table(f)
    assert t.to_pydict() == {"n": [1.5, 2.5], "f": ["f", None], "i": [5, None]}
    # bitmap laziness: to_dict() alone builds none
    assert f.to_dict()["n"].tolist() == [1.5, 2.5]
    # a Frame re-sent through its own export keeps row names
    fi = _read(bytes.fromhex(CORPUS["frame-char-rownames"]))
    assert fi.row_names == ["a", "b", "c"]
    assert _write(fi) == CORPUS["frame-char-rownames"]
    # pickling on a same-language handle
    f2 = pickle.loads(pickle.dumps(fi))
    assert f2.row_names == ["a", "b", "c"]
    assert _write(f2) == CORPUS["frame-char-rownames"]


def test_frame_complex_column():
    f = _read(bytes.fromhex(CORPUS["frame-cplx"]))
    assert f.to_dict()["z"].dtype == np.complex128
    with pytest.raises(pymizu.MizuError, match="complex column"):
        f.__arrow_c_stream__()


def test_same_language_frame_pickles():
    h = _pymizu._channel_new(64, 1024, 65536, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    p.ready_set()
    assert h.ready_wait(10)
    f = _read(bytes.fromhex(CORPUS["frame-3col"]))
    h.send(f)
    got = p.recv(5)
    assert _write(got) == CORPUS["frame-3col"]
    p.destroy()
    h.destroy()


def test_pandas_frame_foreign():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    h, p = foreign_pair()
    df = pd.DataFrame(
        {
            "s": pd.array(["a", "b"], dtype="str"),
            "o": pd.Series(["x", None], dtype=object),
            "i": pd.array([1, None], dtype="Int64"),
            "c": pd.Series(["u", "v"], dtype=pd.CategoricalDtype(["u", "v"])),
        }
    )
    h.send(df)
    f = p.recv(5)
    d = f.to_dict()
    assert d["s"] == ["a", "b"]
    assert d["o"] == ["x", None]
    assert d["i"].tolist() == [1, -9223372036854775808]
    assert d["c"] == ["u", "v"]
    # a string index arrives as a column
    df2 = pd.DataFrame({"x": [1, 2]}, index=["r1", "r2"])
    h.send(df2)
    f = p.recv(5)
    assert (
        "__index_level_0__" in f.names or "index" in f.names or "x" in f.names
    )
    # a named RangeIndex is dropped
    df3 = pd.DataFrame({"x": [1, 2]})
    df3.index.name = "myidx"
    h.send(df3)
    f = p.recv(5)
    assert f.names == ("x",)
    # an ordered Categorical declines
    df4 = pd.DataFrame(
        {
            "c": pd.Series(
                ["a"], dtype=pd.CategoricalDtype(["a", "b"], ordered=True)
            )
        }
    )
    with pytest.raises(pymizu.DeclinedError, match="ordered"):
        h.send(df4)
    p.destroy()
    h.destroy()


def test_pandas_without_pyarrow_declines():
    pd = pytest.importorskip("pandas")
    import builtins
    import sys

    real_import = builtins.__import__

    def no_pyarrow(name, *args, **kwargs):
        if name == "pyarrow" or name.startswith("pyarrow."):
            raise ImportError("No module named 'pyarrow'")
        return real_import(name, *args, **kwargs)

    h, p = foreign_pair()
    try:
        builtins.__import__ = no_pyarrow
        saved = sys.modules.pop("pyarrow", None)
        with pytest.raises(pymizu.DeclinedError, match="pyarrow"):
            h.send(pd.DataFrame({"x": [1, 2]}))
    finally:
        builtins.__import__ = real_import
        if saved is not None:
            sys.modules["pyarrow"] = saved
        p.destroy()
        h.destroy()


def test_err_stream_reads_as_a_task_error_value():
    data = bytes.fromhex(
        "49011100000a00000056616c75654572726f7204000000626f6f6d00000000"
    )
    v = _read(data)
    assert isinstance(v, pymizu.TaskError)
    assert isinstance(v, pymizu.MizuError)
    assert pymizu.is_remote_error(v)
    assert not pymizu.is_remote_error(ValueError("boom"))
    assert v.remote_type == "ValueError"
    assert str(v) == "ValueError: boom"
    assert v.remote_traceback == ""
    assert not hasattr(v, "index")
    data_idx = bytes.fromhex(
        "490111010029000000000000000b000000576f726b65724572726f72"
        "0e000000656c656d656e74206661696c656400000000"
    )
    vi = _read(data_idx)
    assert vi.remote_type == "WorkerError"
    assert vi.index == 41  # the wire index, 0-based


def test_write_err_frames_bounded():
    assert _pymizu._write_err("ValueError", "boom", "").hex() == (
        "49011100000a00000056616c75654572726f7204000000626f6f6d00000000"
    )
    # the frame fits the slot by construction: strings truncate at shares
    b = _pymizu._write_err("custom_error", "m" * 500, "d" * 500, budget=48)
    assert len(b) <= 48
    v = _read(bytes(b))
    assert v.remote_type == "custom_error"
    assert str(v) == "custom_error: " + "m" * 11
    assert v.remote_traceback == ""
    # a multibyte string cuts at a character boundary
    b2 = _pymizu._write_err("custom_error", "é" * 100, "", budget=60)
    assert len(b2) <= 60
    assert str(_read(bytes(b2))) == "custom_error: " + "é" * 11


def test_err_send_crosses_foreign_and_same_language_channels():
    h, p = foreign_pair()
    try:
        raise ValueError("boom")
    except ValueError as e:
        assert p._send_error(e) is True
    v = h.recv(5)
    assert isinstance(v, pymizu.TaskError)
    assert v.remote_type == "ValueError"
    assert str(v) == "ValueError: boom"
    assert "ValueError: boom" in v.remote_traceback
    p.destroy()
    h.destroy()
    # same-language: the pointer match bypasses pickle, the same value
    hh = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    pp, _ = _pymizu._channel_attach(hh.token)
    pp.ready_set()
    assert hh.ready_wait(10)
    try:
        raise RuntimeError("same-lang")
    except RuntimeError as e:
        assert pp._send_error(e) is True
        # a second send of the same exception pickles as an ordinary value
        assert pp.send(e) is True
    v2 = hh.recv(5)
    assert isinstance(v2, pymizu.TaskError)
    assert str(v2) == "RuntimeError: same-lang"
    v3 = hh.recv(5)
    assert type(v3) is RuntimeError
    assert not pymizu.is_remote_error(v3)
    pp.destroy()
    hh.destroy()


# -- hand-built streams: the column and attribute forms --------------------


def _ix_key(k):
    b = k.encode()
    return len(b).to_bytes(4, "little") + b


def _ix_sval(v):
    b = v.encode()
    return b"\x04" + len(b).to_bytes(4, "little") + b


def _ix_strelt(v):
    if v is None:
        return (-1).to_bytes(4, "little", signed=True)
    b = v.encode()
    return len(b).to_bytes(4, "little") + b


def _ix_int(v):
    return b"\x02" + v.to_bytes(8, "little", signed=True)


def _ix_real(v):
    return b"\x03" + struct.pack("<d", v)


def _ix_vec(tag, data, count):
    return bytes([tag]) + count.to_bytes(8, "little") + data


def _ix_frame(cols, names, nrow, rownames=None):
    s = bytearray(b"\x0f")
    s += b"\x0c" + len(cols).to_bytes(8, "little")
    for c in cols:
        s += c
    s += b"\x0d" + (3).to_bytes(8, "little")
    s += _ix_key("names")
    if len(names) == 1:
        s += _ix_sval(names[0])
    else:
        s += b"\x0b" + len(names).to_bytes(8, "little")
        for nm in names:
            s += _ix_strelt(nm)
    s += _ix_key("class") + _ix_sval("data.frame")
    s += _ix_key("row.names")
    if rownames is not None:
        s += rownames
    else:
        s += _ix_vec(
            0x07,
            (-(2**31)).to_bytes(4, "little", signed=True)
            + (-nrow).to_bytes(4, "little", signed=True),
            2,
        )
    return b"I\x01" + bytes(s)


def test_frame_scalar_column_forms():
    # one scalar column per kind: the length-1 column homes (the scalar
    # tags promote to the n == 1 vector forms at read)
    cases = [
        (b"\x01\x01", "lgl", [True]),
        (b"\x01\x02", "lgl_na", [np.int32(-(2**31))]),
        (_ix_int(5), "i32", [5]),
        (_ix_int(2**40), "i64", [2**40]),
        (_ix_real(1.5), "f64", [1.5]),
        (b"\x10" + struct.pack("<dd", 1.0, 2.0), "c128", [1 + 2j]),
        (_ix_sval("x"), "str", ["x"]),
        (b"\x04" + (-1).to_bytes(4, "little", signed=True), "str_na", [None]),
    ]
    for col, name, want in cases:
        f = _read(_ix_frame([col], [name], 1))
        got = f.to_dict()[name]
        got = got.tolist() if hasattr(got, "tolist") else got
        assert got == want, (name, got)
        # the canonical fixpoint: the write-back reads and writes the same
        s = _pymizu._write_stream(f)
        assert _pymizu._write_stream(_read(s)) == s, name


def test_frame_scalar_int64_column_reemits():
    # a scalar INT past int32 adopts an i64 column; the NA scalar a masked
    # i32 one
    f = _read(_ix_frame([_ix_int(2**40)], ["a"], 1))
    assert f.to_dict()["a"].dtype == np.int64
    f = _read(_ix_frame([_ix_int(-(2**63))], ["a"], 1))
    d = f.to_dict()["a"]
    assert d.dtype == np.float64 and np.isnan(d[0])


def test_frame_vector_column_forms():
    # a vector column per wire type, CPLX included (a pyarrow producer has
    # no complex — the stream form carries it)
    i32 = struct.pack("<3i", 1, 2, -(2**31))
    f64 = struct.pack("<3d", 1.5, -2.5, 3.5)
    i64v = struct.pack("<3q", 1, -1, 2**60)
    cplx = struct.pack("<6d", 1, 1, 2, 2, 3, 3)
    raw = b"\x00\x01\xff"
    lgl = struct.pack("<3i", 0, 1, -(2**31))
    cols = [
        _ix_vec(0x08, f64, 3),
        _ix_vec(0x07, i32, 3),
        _ix_vec(0x0E, i64v, 3),
        _ix_vec(0x09, cplx, 3),
        _ix_vec(0x0A, raw, 3),
        _ix_vec(0x06, lgl, 3),
    ]
    names = ["f64", "i32", "i64", "c128", "u8", "lgl"]
    f = _read(_ix_frame(cols, names, 3))
    d = f.to_dict()
    assert d["f64"].tolist() == [1.5, -2.5, 3.5]
    assert d["i32"].tolist()[0] == 1.0 and np.isnan(d["i32"][2])
    assert d["i64"].tolist() == [1, -1, 2**60]
    assert d["c128"].tolist() == [1 + 1j, 2 + 2j, 3 + 3j]
    assert d["u8"].tolist() == [0, 1, 255]
    assert d["lgl"].tolist()[:2] == [False, True]
    s = _pymizu._write_stream(f)
    assert _pymizu._write_stream(_read(s)) == s


def test_frame_column_malformed_forms():
    # the column reader's refusals, byte-exact
    cases = [
        # a string column whose second element is not a string
        (
            b"\x0b" + (2).to_bytes(8, "little") + _ix_strelt("a") + _ix_int(1),
            None,
        ),
        # an attributed column wrapping a non-vector value
        (
            b"\x0f"
            + b"\x0c"
            + (0).to_bytes(8, "little")
            + b"\x0d"
            + (0).to_bytes(8, "little"),
            "attributed form",
        ),
        # an attributed column of an unknown class
        (
            b"\x0f"
            + _ix_real(1.5)
            + b"\x0d"
            + (1).to_bytes(8, "little")
            + _ix_key("class")
            + _ix_sval("mystery"),
            "no portable home",
        ),
        # a difftime column of an unknown unit
        (
            b"\x0f"
            + _ix_real(1.5)
            + b"\x0d"
            + (2).to_bytes(8, "little")
            + _ix_key("class")
            + _ix_sval("difftime")
            + _ix_key("units")
            + _ix_sval("fortnights"),
            "no portable home",
        ),
        # a list column
        (b"\x0c" + (0).to_bytes(8, "little"), "unexpected form"),
    ]
    for col, match in cases:
        with pytest.raises(pymizu.MizuError, match=match):
            _read(_ix_frame([col], ["a"], 1))


def test_attr_dict_malformed_forms():
    # the attribute dict's refusals (on a standalone POSIXct-shaped value)
    def attr_val(dict_bytes):
        return b"I\x01\x0f" + _ix_real(1.5) + dict_bytes

    # the attributes are not a dict
    with pytest.raises(pymizu.MizuError, match="not a dict"):
        _read(attr_val(_ix_int(1)))
    # past 16 entries
    big = b"\x0d" + (17).to_bytes(8, "little")
    for i in range(17):
        big += _ix_key(f"k{i}") + _ix_int(1)
    with pytest.raises(pymizu.MizuError, match="past 16"):
        _read(attr_val(big))
    # a duplicate key
    dup = b"\x0d" + (2).to_bytes(8, "little")
    dup += (
        _ix_key("class")
        + _ix_sval("Date")
        + _ix_key("class")
        + _ix_sval("Date")
    )
    with pytest.raises(pymizu.MizuError, match="duplicate"):
        _read(attr_val(dup))
    # an NA string value
    nastr = b"\x0d" + (1).to_bytes(8, "little")
    nastr += (
        _ix_key("class") + b"\x04" + (-1).to_bytes(4, "little", signed=True)
    )
    with pytest.raises(pymizu.MizuError, match="NA attribute string"):
        _read(attr_val(nastr))
    # a value of an unsupported kind
    badk = b"\x0d" + (1).to_bytes(8, "little")
    badk += _ix_key("class") + _ix_real(1.5)
    with pytest.raises(pymizu.MizuError, match="unsupported kind"):
        _read(attr_val(badk))
    # an attribute vector of a non-INT type
    badv = b"\x0d" + (1).to_bytes(8, "little")
    badv += _ix_key("dim") + _ix_vec(0x08, struct.pack("<d", 1.5), 1)
    with pytest.raises(pymizu.MizuError, match="unexpected type"):
        _read(attr_val(badv))


def test_factor_standalone_forms():
    # the standalone factor homes a list[str | None]; the scalar code and
    # the NA code included
    def factor(codes, levels):
        s = bytearray(b"I\x01\x0f")
        s += _ix_vec(0x07, struct.pack(f"<{len(codes)}i", *codes), len(codes))
        s += b"\x0d" + (2).to_bytes(8, "little")
        s += _ix_key("class") + _ix_sval("factor")
        s += _ix_key("levels")
        if len(levels) == 1 and levels[0] is not None:
            s += _ix_sval(levels[0])
        else:
            s += b"\x0b" + len(levels).to_bytes(8, "little")
            for lv in levels:
                s += _ix_strelt(lv)
        return bytes(s)

    got = _read(factor([1, -(2**31), 2], ["a", "b"]))
    assert got == ["a", None, "b"]
    # the scalar-code form (the 0x02 scalar, 1-based)
    s = bytearray(b"I\x01\x0f") + _ix_int(2)
    s += b"\x0d" + (2).to_bytes(8, "little")
    s += _ix_key("class") + _ix_sval("factor") + _ix_key("levels")
    s += (
        b"\x0b" + (2).to_bytes(8, "little") + _ix_strelt("a") + _ix_strelt("b")
    )
    assert _read(bytes(s)) == ["b"]
    # a code outside the levels
    with pytest.raises(pymizu.MizuError, match="outside the levels"):
        _read(factor([3], ["a", "b"]))
    # an NA level
    with pytest.raises(pymizu.MizuError, match="NA factor level"):
        _read(factor([1], [None]))


def test_dim_malformed_forms():
    def dim_val(dims_bytes, vec_tag=0x07, vec_data=None, n=2):
        if vec_data is None:
            vec_data = struct.pack("<2i", 1, 2)
        s = bytearray(b"I\x01\x0f")
        s += _ix_vec(vec_tag, vec_data, n)
        s += b"\x0d" + (1).to_bytes(8, "little")
        s += _ix_key("dim") + dims_bytes
        return bytes(s)

    # an empty dim (the zero-length intv)
    with pytest.raises(pymizu.MizuError, match="an empty dim"):
        _read(dim_val(_ix_vec(0x07, b"", 0)))
    # a negative dim
    with pytest.raises(pymizu.MizuError, match="a negative dim"):
        _read(
            dim_val(
                _ix_vec(
                    0x07,
                    (-1).to_bytes(4, "little", signed=True)
                    + (2).to_bytes(4, "little", signed=True),
                    2,
                )
            )
        )
    # a dim product overflow
    big = struct.pack("<3i", 2**31 - 1, 2**31 - 1, 2**31 - 1)
    with pytest.raises(pymizu.MizuError, match="product overflow"):
        _read(dim_val(_ix_vec(0x07, big, 3)))
    # a dim past 32 axes
    with pytest.raises(pymizu.MizuError, match="past 32"):
        _read(dim_val(_ix_vec(0x07, struct.pack("<33i", *([1] * 33)), 33)))
    # the product is not the element count
    with pytest.raises(
        pymizu.MizuError, match="not the value's element count"
    ):
        _read(dim_val(_ix_vec(0x07, struct.pack("<2i", 3, 3), 2)))
    # a scalar dim is the length-1 shift: a plain vector
    got = _read(dim_val(_ix_int(2)))
    assert got.tolist() == [1, 2]


def test_attr_value_form_errors():
    # an attributed list reads as a frame: zero columns is malformed
    s = b"I\x01\x0f" + b"\x0c" + (0).to_bytes(8, "little")
    s += b"\x0d" + (0).to_bytes(8, "little")
    with pytest.raises(pymizu.MizuError, match="without columns"):
        _read(s)
    # an attributed value of a non-vector form
    s = b"I\x01\x0f" + b"\x05" + (0).to_bytes(8, "little")
    s += b"\x0d" + (0).to_bytes(8, "little")
    with pytest.raises(pymizu.MizuError, match="of this form"):
        _read(s)
    # an unhandled class list names the classes in the error
    s = bytearray(b"I\x01\x0f")
    s += _ix_vec(0x07, struct.pack("<2i", 1, 2), 2)
    s += b"\x0d" + (1).to_bytes(8, "little")
    s += _ix_key("class")
    s += (
        b"\x0b"
        + (2).to_bytes(8, "little")
        + _ix_strelt("foo")
        + _ix_strelt("bar")
    )
    with pytest.raises(pymizu.MizuError, match='"foo", "bar"'):
        _read(bytes(s))


def test_int64_na_warning_as_error():
    # the sentinel-carrying read honors the warning filter
    s = b"I\x01" + _ix_vec(0x0E, struct.pack("<2q", 1, -(2**63)), 2)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(RuntimeWarning, match="NA_integer64_"):
            _read(s)


def test_err_stream_with_an_index():
    out = _read(_pymizu._write_err("ValueError", "msg", "tb", index=7))
    assert isinstance(out, pymizu.TaskError)
    assert out.remote_type == "ValueError"
    assert out.index == 7


def test_ref_leaf_in_a_value_stream():
    # the 0x13 leaf declines on the channel value reader
    s = b"I\x01\x13\x08/mizu_ab"
    with pytest.raises(pymizu.MizuError, match="cannot cross as a channel"):
        _read(s)


def test_dict_and_strv_malformed_forms():
    # a dict whose key is not a string
    s = b"I\x01\x0d" + (1).to_bytes(8, "little") + _ix_int(1) + _ix_int(2)
    with pytest.raises(pymizu.MizuError):
        _read(s)
    # a duplicate dict key
    s = b"I\x01\x0d" + (2).to_bytes(8, "little")
    s += _ix_key("a") + _ix_int(1) + _ix_key("a") + _ix_int(2)
    with pytest.raises(pymizu.MizuError, match="duplicate"):
        _read(s)
    # a string vector whose element is not a string
    s = b"I\x01\x0b" + (2).to_bytes(8, "little") + _ix_strelt("a") + _ix_int(1)
    with pytest.raises(pymizu.MizuError):
        _read(s)


def test_str_list_length_one_is_a_scalar():
    # the ixe_strv shape: length 1 is the 0x04 scalar both ways
    assert _read(_pymizu._write_stream(["a"])) == "a"
    assert _read(_pymizu._write_stream([["a"]])) == ["a"]


def test_scalar_dtype_u64_and_declines():
    # the uint64 scalar: within int64 it is the int tag, past it a decline;
    # a 0-d buffer of an unmapped dtype declines too
    assert _read(_pymizu._write_stream(np.uint64(5))) == 5
    assert _read(_pymizu._write_stream(np.uint64(2**63 - 1))) == 2**63 - 1
    with pytest.raises(pymizu.DeclinedError):
        _pymizu._write_stream(np.uint64(2**63))
    with pytest.raises(pymizu.DeclinedError):
        _pymizu._write_stream(np.array("ab"))


def test_numpy_temporal_2d_declines():
    with pytest.raises(pymizu.DeclinedError, match="past 1-D"):
        _pymizu._write_stream(np.zeros((2, 2), dtype="m8[ms]"))
    with pytest.raises(pymizu.DeclinedError, match="past 1-D"):
        _pymizu._write_stream(np.zeros((2, 2), dtype="M8[us]"))


def test_stdlib_datetime_zone_forms():
    # datetime.timezone.utc has no zoneinfo key: its str() is "UTC"
    got = _read(
        _pymizu._write_stream(
            datetime.datetime(2023, 11, 14, 22, tzinfo=datetime.timezone.utc)
        )
    )
    assert got == np.array(["2023-11-14T22:00:00"], dtype="datetime64[us]")[0]
    # a fixed-offset zone normalizes to the UTC instant but its str() is
    # not "UTC": no portable home
    with pytest.raises(pymizu.DeclinedError, match="no zoneinfo key"):
        _pymizu._write_stream(
            datetime.datetime(
                2023,
                11,
                14,
                tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
            )
        )

    class BadTZ(datetime.tzinfo):
        def utcoffset(self, dt):
            raise ValueError("broken")

    with pytest.raises(pymizu.DeclinedError):
        _pymizu._write_stream(datetime.datetime(2023, 11, 14, tzinfo=BadTZ()))


def test_stdlib_timedelta_range_decline():
    with pytest.raises(pymizu.DeclinedError, match="past the int64"):
        _pymizu._write_stream(datetime.timedelta(days=200000))


def test_ref_marker_forms():
    # the corpus marker: a valid identifier round-trips through the task
    # hooks; malformed identifiers decline at the write
    m = _pymizu._IxRef("/mizu_abc")
    s = _pymizu._write_task("os.getpid", 0, (m,), {}, 1, 0)
    got = _pymizu._read_task(s)
    assert type(got[4][0]).__name__ == "_IxRef"
    assert got[4][0].id == "/mizu_abc"
    for bad in ("", "x" * 256):
        with pytest.raises(pymizu.DeclinedError, match="1..255"):
            _pymizu._write_task(
                "os.getpid", 0, (_pymizu._IxRef(bad),), {}, 1, 0
            )
    with pytest.raises(pymizu.DeclinedError, match="UTF-8"):
        _pymizu._write_task(
            "os.getpid", 0, (_pymizu._IxRef("\ud800"),), {}, 1, 0
        )


def test_task_stream_read_as_a_value():
    s = _pymizu._write_task("os.getpid", 0, (), {}, 1, 0)
    with pytest.raises(pymizu.MizuError, match="not a value"):
        _read(s)


def test_write_task_hook_validation():
    with pytest.raises(TypeError, match="expects"):
        _pymizu._write_task(42, 0, (), {}, 1, 0)
    with pytest.raises(TypeError, match="expects"):
        _pymizu._write_task("os.getpid", 0, [], {}, 1, 0)
    with pytest.raises(TypeError, match="expects"):
        _pymizu._write_task("os.getpid", 0, (), [], 1, 0)
    # a lone-surrogate code string
    with pytest.raises(ValueError):
        _pymizu._write_task("\ud800", 0, (), {}, 1, 0)


def test_write_task_kwargs_forms():
    # the named-dict walk: values through the generic writer, a non-str or
    # lone-surrogate key a decline
    b = _pymizu._write_task(
        "math.sqrt", 0, (16,), {"y": 2.5, "z": None}, 2, 99
    )
    got = _pymizu._read_task(b)
    assert got[5] == {"y": 2.5, "z": None}
    with pytest.raises(pymizu.DeclinedError, match="non-str dict key"):
        _pymizu._write_task("m.f", 0, (), {1: 2}, 1, 0)
    with pytest.raises(pymizu.DeclinedError, match="not writable as UTF-8"):
        _pymizu._write_task("m.f", 0, (), {"\ud800": 1}, 1, 0)


def test_read_task_malformed_dicts():
    def task(dict_body):
        s = bytearray(b"I\x01\x12\x01\x00\x00\x00") + (0).to_bytes(8, "little")
        s += _ix_sval("m.f")
        s += b"\x0c" + (0).to_bytes(8, "little")
        s += b"\x0d" + dict_body
        return bytes(s)

    dup = (2).to_bytes(8, "little")
    dup += _ix_key("a") + _ix_int(1) + _ix_key("a") + _ix_int(2)
    with pytest.raises(pymizu.MizuError, match="a duplicate dict key"):
        _pymizu._read_task(task(dup))
    # an item where a key was expected
    nonstr = (1).to_bytes(8, "little") + _ix_int(1)
    with pytest.raises(pymizu.MizuError):
        _pymizu._read_task(task(nonstr))


def test_map_desc_hooks():
    # the descriptor's 'I' form, both x forms, and the reader's refusals
    b = _pymizu._map_desc_write("math.sqrt", 0, (), {"k": 1}, [1, 2.5, "x"], 1)
    kind, code, pos, kw, x = _pymizu._map_desc_read(b)
    assert (kind, code, pos, kw, x) == (
        0,
        "math.sqrt",
        [],
        {"k": 1},
        [1, 2.5, "x"],
    )
    b = _pymizu._map_desc_write("os.getpid", 1, (), {}, None, 2)
    assert _pymizu._map_desc_read(b) == (1, "os.getpid", [], {}, None)
    with pytest.raises(pymizu.DeclinedError):
        _pymizu._map_desc_write("m.f", 0, (), {}, [object()], 1)
    with pytest.raises(TypeError, match="expects"):
        _pymizu._map_desc_write(42, 0, (), {}, None, 1)
    # the reader's shape checks
    s = b"I\x01\x0c" + (1).to_bytes(8, "little") + b"\x00"
    with pytest.raises(pymizu.MizuError, match="two-element list"):
        _pymizu._map_desc_read(s)
    s = b"I\x01\x0c" + (2).to_bytes(8, "little") + b"\x00\x00"
    with pytest.raises(pymizu.MizuError, match="no task tag"):
        _pymizu._map_desc_read(s)
    s = b"I\x01\x0c" + (2).to_bytes(8, "little")
    s += b"\x12\x01\x02\x00\x00" + (0).to_bytes(8, "little")
    with pytest.raises(pymizu.MizuError, match="not a call spec"):
        _pymizu._map_desc_read(s)
    with pytest.raises(pymizu.MizuError, match="bad magic"):
        _pymizu._map_desc_read(b"")
    with pytest.raises(pymizu.MizuError, match="truncated"):
        _pymizu._map_desc_read(b"I\x01\x0c")


MAP_DESC_PEER = """
import time
import numpy as np
ch.send(np.arange(200000, dtype=np.float64))
time.sleep(30)
"""


@pytest.mark.skipif(
    os.name == "nt",
    reason="no unlink on Windows: the name resolves while a handle lives",
)
def test_map_desc_view_ref_resolve_and_gone():
    # a re-sendable view rides the descriptor as one REF leaf (D6): the
    # read resolves it while the region lives; once a dead producer's
    # verdict unlinked the name the resolve is the clean "not found"
    # (the descriptor reader carries no gone signal)
    ch = pymizu.Channel.create(MAP_DESC_PEER)
    view = ch.recv(30).base
    b = _pymizu._map_desc_write("os.getpid", 0, (), {}, view, 1)
    kind, code, pos, kw, x = _pymizu._map_desc_read(b)
    assert (kind, code, pos, kw) == (0, "os.getpid", [], {})
    assert pymizu._pymizu._view_check(x)
    assert np.asarray(x)[3] == 3.0
    ch._proc.kill()
    assert ch.close() is True
    ch._proc.wait()
    pymizu.prune()
    with pytest.raises(pymizu.MizuError, match="not found"):
        _pymizu._map_desc_read(b)
    ch.destroy()


def test_write_runner_hook():
    # the kind-2 stream off the frame fields: nil and (seed, offset) forms
    b = _pymizu._write_runner("/mizu_xyz", 5 << 32 | 1, None, 1, 0)
    assert b[:3] == b"I\x01\x12"
    b2 = _pymizu._write_runner("/mizu_xyz", 1, (42, 7), 1, 0)
    assert len(b2) > len(b)
    with pytest.raises(TypeError):
        _pymizu._write_runner(42, 1, None, 1, 0)


def test_container_truncated_forms():
    # the strv/dict readers' cursor-failure arms (the cursor owns UTF-8
    # validity, so malformed bytes land on the same arms)
    cases = [
        # a strv short an element
        b"I\x01\x0b" + (2).to_bytes(8, "little") + _ix_strelt("a"),
        # an invalid-UTF-8 strv element
        b"I\x01\x0b"
        + (2).to_bytes(8, "little")
        + _ix_strelt("a")
        + b"\x01\x00\x00\x00\xff",
        # a dict key short of its declared length
        b"I\x01\x0d" + (1).to_bytes(8, "little") + b"\x05\x00\x00\x00ab",
        # an invalid-UTF-8 dict key
        b"I\x01\x0d"
        + (1).to_bytes(8, "little")
        + b"\x01\x00\x00\x00\xff"
        + _ix_int(1),
        # a value missing after its key
        b"I\x01\x0d" + (1).to_bytes(8, "little") + _ix_key("a"),
        # a key item that is not a string
        b"I\x01\x0d" + (1).to_bytes(8, "little") + _ix_int(1) + _ix_int(2),
    ]
    for s in cases:
        with pytest.raises(pymizu.MizuError):
            _read(s)


def test_attr_dict_truncated_forms():
    # the attr dict reader's cursor-failure arms
    def attr_val(body):
        return b"I\x01\x0f" + _ix_real(1.5) + body

    # a key item that is not a string
    with pytest.raises(pymizu.MizuError):
        _read(attr_val(b"\x0d" + (1).to_bytes(8, "little") + _ix_int(1)))
    # a value missing after its key
    with pytest.raises(pymizu.MizuError):
        _read(attr_val(b"\x0d" + (1).to_bytes(8, "little") + _ix_key("k")))


def test_attr_strv_dim_no_home():
    # an attributed character vector declines (a character matrix's shape)
    s = bytearray(b"I\x01\x0f")
    s += _ix_vec(0x0B, _ix_strelt("a") + _ix_strelt("b"), 2)
    s += b"\x0d" + (1).to_bytes(8, "little")
    s += _ix_key("dim") + _ix_vec(0x07, struct.pack("<i", 2), 1)
    with pytest.raises(pymizu.MizuError, match="attributed character vector"):
        _read(bytes(s))


def test_difftime_out_of_range_no_home():
    # 1e13 seconds is past the timedelta64[us] span
    s = bytearray(b"I\x01\x0f")
    s += _ix_vec(0x08, struct.pack("<d", 1e13), 1)
    s += b"\x0d" + (2).to_bytes(8, "little")
    s += _ix_key("class") + _ix_sval("difftime")
    s += _ix_key("units") + _ix_sval("secs")
    with pytest.raises(
        pymizu.MizuError, match="no portable home for this difftime value"
    ):
        _read(bytes(s))


def test_frame_row_names_malformed():
    intv3 = _ix_vec(0x07, struct.pack("<3i", 1, 2, 3), 3)
    strv3 = _ix_vec(
        0x0B, _ix_strelt("a") + _ix_strelt("b") + _ix_strelt("c"), 3
    )
    cases = [
        (intv3, "not the row count"),  # 3 int row names, 2 rows
        (_ix_int(5), "not the row count"),  # a scalar row name, 2 rows
        (strv3, "not int or character"),  # 3 string row names, 2 rows
    ]
    col = _ix_vec(0x07, struct.pack("<2i", 1, 2), 2)
    for rn, match in cases:
        with pytest.raises(pymizu.MizuError, match=match):
            _read(_ix_frame([col], ["a"], 2, rownames=rn))
    # an NA in a character row.names
    s = bytearray(b"\x0f")
    s += b"\x0c" + (1).to_bytes(8, "little") + col
    s += b"\x0d" + (3).to_bytes(8, "little")
    s += _ix_key("names") + _ix_sval("a")
    s += _ix_key("class") + _ix_sval("data.frame")
    s += _ix_key("row.names")
    s += (
        b"\x0b"
        + (2).to_bytes(8, "little")
        + _ix_strelt("x")
        + _ix_strelt(None)
    )
    with pytest.raises(pymizu.MizuError, match="an NA row name"):
        _read(b"I\x01" + bytes(s))


def test_frame_attributed_list_no_home():
    # a list with names/class/row.names but not the data.frame class
    s = bytearray(b"\x0f")
    s += b"\x0c" + (1).to_bytes(8, "little")
    s += _ix_vec(0x07, struct.pack("<2i", 1, 2), 2)
    s += b"\x0d" + (3).to_bytes(8, "little")
    s += _ix_key("names") + _ix_sval("a")
    s += _ix_key("class") + _ix_sval("foo")
    s += _ix_key("row.names") + _ix_int(5)
    with pytest.raises(pymizu.MizuError, match="an attributed list"):
        _read(b"I\x01" + bytes(s))


def test_frame_dict_scalar_column_reemits():
    # a 1-row frame's factor column homes the scalar-code emit; the NA code
    # crosses as the int64 sentinel
    def factor(code):
        s = bytearray(b"\x0f")
        s += _ix_int(code)
        s += b"\x0d" + (2).to_bytes(8, "little")
        s += _ix_key("class") + _ix_sval("factor")
        s += _ix_key("levels")
        s += (
            b"\x0b"
            + (2).to_bytes(8, "little")
            + _ix_strelt("a")
            + _ix_strelt("b")
        )
        return bytes(s)

    f = _read(_ix_frame([factor(2), factor(-(2**63))], ["a", "b"], 1))
    assert f.to_dict() == {"a": ["b"], "b": [None]}
    out = _read(_pymizu._write_stream(f))
    assert out.to_dict() == {"a": ["b"], "b": [None]}


def test_frame_u64_clean_reemit():
    # the 'I' frame emit's uint64 conversion: lanes within 2^53 convert
    # exact (past-2^53 lanes NA with a warning, covered elsewhere)
    t = pa.table({"u": pa.array([1, 2, 3], type=pa.uint64())})
    f = pymizu.Frame.from_arrow(t)
    out = _read(_pymizu._write_stream(f))
    assert out.to_dict()["u"].tolist() == [1.0, 2.0, 3.0]


def test_writer_declines_buffer_forms():
    with pytest.raises(pymizu.DeclinedError, match="past 32 dimensions"):
        _pymizu._write_stream(np.zeros([1] * 33))
    with pytest.raises(pymizu.DeclinedError, match="below microsecond"):
        _pymizu._write_stream(np.array([1], dtype="M8[ps]"))
    # a lone-surrogate dict key
    with pytest.raises(pymizu.DeclinedError, match="not writable as UTF-8"):
        _pymizu._write_stream({"\ud800": 1})
    # a strided array is the top-level F-order form only — never nested
    with pytest.raises(pymizu.DeclinedError, match="strided array nested"):
        _pymizu._write_stream([np.arange(10)[::2]])


def test_frame_row_names_reemit_forms():
    # a 1-row frame's character row name re-emits as the scalar form
    f = _pymizu._frame_rebuild(["a"], [("i", [5])], ["x"])
    assert f.row_names == ["x"]
    out = _read(_pymizu._write_stream(f))
    assert out.row_names == ["x"] and out.to_dict()["a"].tolist() == [5]
    # a lone-surrogate row name declines the emit (the row_names getter
    # hands out the live list)
    f.row_names[0] = "\ud800"
    with pytest.raises(pymizu.DeclinedError, match="not writable as UTF-8"):
        _pymizu._write_stream(f)
    g = _pymizu._frame_rebuild(["a"], [("i", [1, 2])], ["x", "y"])
    g.row_names[1] = "\ud800"
    with pytest.raises(pymizu.DeclinedError, match="not writable as UTF-8"):
        _pymizu._write_stream(g)


def test_frame_emit_overflow_arms():
    # the inline-budget pass of a foreign send: a column body past the slot
    # payload flips the walk to count-only (the reservation's second walk
    # writes it whole) — one frame per overflowing column kind
    h, p = foreign_pair()
    try:
        t = pa.table(
            {
                "f": pa.array(
                    [["u", "v"][k % 2] for k in range(300)]
                ).dictionary_encode()
            }
        )
        assert h.send(pymizu.Frame.from_arrow(t)) is True
        d = p.recv(5).to_dict()
        assert d["f"][:3] == ["u", "v", "u"] and len(d["f"]) == 300
        t = pa.table(
            {"d": pa.array([19000 + k for k in range(300)], type=pa.date32())}
        )
        assert h.send(pymizu.Frame.from_arrow(t)) is True
        assert p.recv(5).to_dict()["d"][0] == np.datetime64("2022-01-08")
        # int32 row names: the buffer form's vec body overflows likewise
        g = _pymizu._frame_rebuild(
            ["a"], [("i", list(range(200)))], list(range(200))
        )
        assert h.send(g) is True
        out = p.recv(5)
        assert out.to_dict()["a"].tolist() == list(range(200))
        assert list(out.row_names) == list(range(200))
    finally:
        p.destroy()
        h.destroy()


def test_datetime_probe_before_any_datetime_import():
    # the datetime module probe's miss arm: a process that never imported
    # datetime stages a foreign value — the probe finds nothing in
    # sys.modules (never imports it) and the type gates fall through
    script = """
        import sys
        from pymizu import _pymizu

        sys.modules.pop("datetime", None)  # transitively imported already
        assert "datetime" not in sys.modules
        try:
            _pymizu._write_stream(object())
            raise SystemExit("no decline")
        except Exception as e:
            assert "no portable home" in str(e), e
        assert "datetime" not in sys.modules
        """
    out = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stderr


def test_dict_stream_duplicate_key():
    # the dict reader's duplicate-key refusal
    b = bytearray(_pymizu._write_stream({"a": 1, "b": 2}))
    b[b.rfind(ord("b"))] = ord("a")
    with pytest.raises(pymizu.MizuError, match="duplicate dict key"):
        _read(bytes(b))


def test_frame_names_malformed_forms():
    # the names block's refusals: a count that is not the column count, an
    # NA name, a duplicate name
    col = _ix_int(5)
    with pytest.raises(pymizu.MizuError, match="do not match its columns"):
        _read(_ix_frame([col], ["a", "b"], 1))
    na_names = (
        b"\x0b"
        + (2).to_bytes(8, "little")
        + _ix_strelt("a")
        + (-1).to_bytes(4, "little", signed=True)
    )
    s = bytearray(b"I\x01\x0f")
    s += b"\x0c" + (2).to_bytes(8, "little") + col + col
    s += b"\x0d" + (3).to_bytes(8, "little")
    s += _ix_key("names") + na_names
    s += _ix_key("class") + _ix_sval("data.frame")
    s += _ix_key("row.names")
    s += _ix_vec(
        0x07,
        (-(2**31)).to_bytes(4, "little", signed=True)
        + (-1).to_bytes(4, "little", signed=True),
        2,
    )
    with pytest.raises(pymizu.MizuError, match="an NA column name"):
        _read(bytes(s))
    with pytest.raises(pymizu.MizuError, match="duplicate column names"):
        _read(_ix_frame([col, col], ["a", "a"], 1))


def test_overflow_temporal_realv_body():
    # a datetime64 array's 'I' stream past the slot payload: the REALV body
    # straddles the inline limit, the reservation writes it whole
    h, p = foreign_pair()
    try:
        a = np.datetime64("2024-01-01") + np.arange(
            2000, dtype="timedelta64[us]"
        )
        assert h.send(a) is True
        got = p.recv(5)
        assert got.dtype == "datetime64[us]"
        assert (got == a).all()
    finally:
        p.destroy()
        h.destroy()


def test_overflow_masked_frame_column():
    # an adopted column's nulls are bitmap-only: the masked emit walks per
    # element, the body straddling the inline limit past the slot payload —
    # the frame nested (a top-level frame takes the Arrow front-ends)
    h, p = foreign_pair()
    try:
        vals = [float(k) for k in range(300)]
        vals[7] = None
        f = pymizu.Frame.from_arrow(pa.table({"a": pa.array(vals)}))
        assert h.send([f]) is True
        d = p.recv(5)[0].to_dict()["a"]
        assert np.isnan(d[7])
        assert d[[0, 1, 2]].tolist() == [0.0, 1.0, 2.0]
    finally:
        p.destroy()
        h.destroy()


def test_overflow_dim_intv_body():
    # the dim attr's intv is the stream's last bytes: one shape in the
    # bracket straddles the inline limit with the nd body still fitting
    h, p = foreign_pair()
    try:
        for k in range(119, 124):
            a = np.arange(2 * k, dtype=np.int32).reshape(k, 2)
            assert h.send(a) is True
            got = p.recv(5)
            assert got.shape == (k, 2)
            assert (got == a).all()
    finally:
        p.destroy()
        h.destroy()


def test_write_hooks_write_pass_failure():
    # a PEP 688 exporter whose __buffer__ fails on the write pass: both
    # buffer requests decline the leaf, the deterministic second pass then
    # disagrees with the first — the write-mismatch error, never a wedge
    a = np.arange(16, dtype=np.float64)
    with pytest.raises(pymizu.MizuError, match="interop write mismatch"):
        _pymizu._write_stream(BufferFailAfterFirst(a, fail_after=1))
    with pytest.raises(pymizu.MizuError, match="interop write mismatch"):
        _pymizu._write_task(
            "f", 0, (BufferFailAfterFirst(a, fail_after=2),), {}, 3, 7
        )


def test_write_hooks_warn_as_error():
    # the conversion-warning record raises as error after the write pass,
    # on each two-pass writer
    a = np.array([2**60], dtype=np.uint64)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(RuntimeWarning, match="uint64"):
            _pymizu._write_stream(a)
        with pytest.raises(RuntimeWarning, match="uint64"):
            _pymizu._write_task("f", 0, (a,), {}, 3, 7)
        with pytest.raises(RuntimeWarning, match="uint64"):
            _pymizu._map_desc_write("f", 0, (a,), {}, None, 3)


def test_arrow_pair_bad_capsule_names():
    # __arrow_c_array__ serving capsules under the wrong names: the
    # interface claim fails standing, the channel unharmed
    import ctypes

    buf = ctypes.create_string_buffer(16)
    new_capsule = ctypes.pythonapi.PyCapsule_New
    new_capsule.restype = ctypes.py_object
    new_capsule.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]

    class BadPair:
        def __arrow_c_array__(self):
            return (
                new_capsule(ctypes.addressof(buf), b"not_arrow_schema", None),
                new_capsule(ctypes.addressof(buf), b"arrow_array", None),
            )

    class BadPair2:
        def __arrow_c_array__(self):
            return (
                new_capsule(ctypes.addressof(buf), b"arrow_schema", None),
                new_capsule(ctypes.addressof(buf), b"not_arrow_array", None),
            )

    h, p = foreign_pair()
    try:
        for bad in (BadPair(), BadPair2()):
            with pytest.raises(ValueError, match="incorrect name"):
                h.send(bad)
        assert h.send({"k": [1, 2, 3]}) is True
        assert p.recv(5) == {"k": [1, 2, 3]}
    finally:
        p.destroy()
        h.destroy()


def test_map_desc_read_malformed_forms():
    # the descriptor reader's refusals past the header checks
    base = _pymizu._map_desc_write("math.sqrt", 0, (), {"a": 1}, [1, 2], 3)
    b = bytearray(base)
    b[47] = 0x0C  # the named field as a list
    with pytest.raises(pymizu.MizuError, match="named field"):
        _pymizu._map_desc_read(bytes(b))
    with pytest.raises(pymizu.MizuError):  # ends after the first key
        _pymizu._map_desc_read(bytes(base[:61]))
    with pytest.raises(pymizu.MizuError):  # trailing garbage
        _pymizu._map_desc_read(base + b"\x00")


def test_frame_column_truncated_forms():
    # the column reader's cursor failures: a vector claiming more than
    # the stream holds, at the column's first item and inside the attr
    # wrapper's value item
    col = b"\x08" + (2**40).to_bytes(8, "little")
    with pytest.raises(pymizu.MizuError, match="truncated"):
        _read(_ix_frame([col], ["a"], 1))
    col_attr = b"\x0f" + b"\x08" + (2**40).to_bytes(8, "little")
    with pytest.raises(pymizu.MizuError, match="truncated"):
        _read(_ix_frame([col_attr], ["a"], 1))


def test_stdlib_datetime_offset_attr_failures():
    # a tzinfo returning a timedelta subclass whose shadowing properties
    # fail the offset read: cleared, the key probe declines
    class StrDays(datetime.timedelta):
        @property
        def days(self):
            return "not an int"

    class RaisingDays(datetime.timedelta):
        @property
        def days(self):
            raise AttributeError("days")

    class Tz(datetime.tzinfo):
        delta = datetime.timedelta(hours=1)

        def utcoffset(self, dt):
            return self.delta

        def dst(self, dt):
            return None

    for delta in (StrDays(hours=1), RaisingDays(hours=1)):
        Tz.delta = delta
        x = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=Tz())
        with pytest.raises(pymizu.DeclinedError, match="no zoneinfo key"):
            _write(x)
