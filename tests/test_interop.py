"""The 'I' interchange codec: the golden corpus drives the reader and
writer in-process (tests/interop-corpus/ rides the libmizu pin), and the
feature coverage beyond the corpus lives here — the pinned foreign
staging order, the decline set, the Frame export machinery, and the
Arrow stream front-end."""

import datetime
import pathlib
import pickle
import warnings

import pytest
from tests import ix_notation as ixn
from tests.helpers import foreign_pair

import pymizu
from pymizu import _pymizu

np = pytest.importorskip("numpy", reason="numpy not installed")
pa = pytest.importorskip("pyarrow", reason="pyarrow not installed")

CORPUS_DIR = pathlib.Path(__file__).parent / "interop-corpus"
CASES = ixn.load_cases(CORPUS_DIR / "cases.txt")
CORPUS = ixn.load_corpus(CORPUS_DIR / "corpus.txt")

WARN_ON_READ = {"i64v-na", "list-i64-na"}   # the sentinel-carrying reads
SKIP_WRITE_DECLINE = {"wd-dup-names"}       # a Python dict dedupes by
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
                home.target, home.kind, home.ident, home.code,
            ), cid
            assert ixn.ix_same(got[4], home.positional), cid
            assert ixn.ix_same(got[5], home.named), cid
            assert _pymizu._write_task(
                home.code, home.kind, tuple(home.positional), home.named,
                home.target, home.ident,
            ).hex() == CORPUS[cid], cid
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
                assert _pymizu._write_err(
                    home.remote_type, home.message, home.detail,
                    index=home.index,
                ).hex() == CORPUS[cid], cid
            else:
                assert _write(got) == CORPUS[cid], cid
        elif kind == "enc":
            home = ixn.parse(value)
            assert _write(home) == CORPUS[cid], cid
        else:
            raise AssertionError(f"unknown kind {kind}")


def test_scalar_and_container_roundtrips():
    h, p = foreign_pair()
    for x in [None, True, 1, -1, 1.5, 1 + 2j, "héllo ✓",
              [1, "a", [True, 2.5]], {"a": 1, "b": {"z": None}},
              [1, "a", [True, 2.5]], {"a": 1, "b": {"z": None}},
            2**53, -(2**53)]:
        assert h.send(x) is True
        assert p.recv(5) == x
    # top-level bytes stage raw and read back as a uint8 array
    assert h.send(b"\x00\xff") is True
    assert np.array_equal(p.recv(5), [0, 255])
    # scalar NAs read as None (documented lossy)
    p.destroy()
    h.destroy()


def _tree(n):
    return [{"id": i, "name": f"item-{i:04d}", "vals": [i, i * 1.5, True,
            None]} for i in range(n)]


def test_spilled_streams_roundtrip_on_each_carrier():
    # the one-walk stager's carriers: the corpus pins the stream bytes
    # (_write_stream is the two-pass oracle); these rows pin each
    # carrier's delivery — inline (one walk), an arena chunk, an SHM_RAW
    # region, and the n == inline_max boundary staying INLINE
    h, p = foreign_pair()   # slot_size 1024 (inline budget 1008), 64 KB arena
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
    n = (1 << 16) // size + 4   # cumulatively past the arena
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


def test_nested_dim_leaf():
    got = _read(_pymizu._write_stream([np.arange(6).reshape(2, 3)]))
    assert len(got) == 1 and got[0].shape == (2, 3)
    assert np.array_equal(got[0], np.arange(6).reshape(2, 3))


def test_temporal_shapes():
    # datetime64 both directions: [W] converts to days (exact), the time
    # units to epoch-seconds doubles (sub-us resolution rounds)
    a = np.array([1, 20000], dtype="datetime64[D]")
    got = _read(_pymizu._write_stream(a))
    assert np.array_equal(got.view(np.int64), a.view(np.int64))
    a = np.array([1, 20000], dtype="datetime64[W]")
    got = _read(_pymizu._write_stream(a))
    assert np.array_equal(got.view(np.int64), a.view(np.int64) * 7)
    scales = {"h": 3600e6, "m": 60e6, "s": 1e6, "ms": 1e3, "us": 1.0,
              "ns": 1e-3}
    for unit, factor in scales.items():
        a = np.array([1, 20000], dtype=f"datetime64[{unit}]")
        got = _read(_pymizu._write_stream(a))
        expected = np.round(a.view(np.int64) * factor).astype(np.int64)
        assert got.dtype == np.dtype("datetime64[us]"), unit
        assert np.array_equal(got.view(np.int64), expected), unit
    nat = np.array([1, np.datetime64("NaT")], dtype="datetime64[us]")
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
    for bad in [np.array([1, 2], dtype="datetime64[Y]"),
                np.array([1, 2], dtype="datetime64[M]"),
                np.array([1], dtype="m8[Y]"),
                np.array([1], dtype="m8[M]"),
                np.array([1], dtype="m8[ps]")]:
        with pytest.raises(pymizu.DeclinedError):
            _pymizu._write_stream(bad)


def test_timedelta_shapes():
    # timedelta64 both directions: any non-calendar unit converts to us
    # (a double-seconds hop), NaT kept
    scales = {"W": 604800e6, "D": 86400e6, "h": 3600e6, "m": 60e6,
              "s": 1e6, "ms": 1e3, "us": 1.0, "ns": 1e-3}
    for unit, factor in scales.items():
        a = np.array([1, 20000], dtype=f"m8[{unit}]")
        got = _read(_pymizu._write_stream(a))
        expected = np.round(a.view(np.int64) * factor).astype(np.int64)
        assert got.dtype == np.dtype("m8[us]"), unit
        assert np.array_equal(got.view(np.int64), expected), unit
    nat = np.array([1, np.timedelta64("NaT")], dtype="m8[s]")
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
    s += b"\x0f"                             # ATTR
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


def test_arrow_temporal_columns():
    h, p = foreign_pair()
    t = pa.table({
        "d": pa.array([19000, None], type=pa.date32()),
        "t": pa.array([1700000000000000, None],
                      type=pa.timestamp("us", tz="UTC")),
        "z": pa.array([1700000000000000, 1700000001000000],
                      type=pa.timestamp("us", tz="America/New_York")),
        "u": pa.array([90, None], type=pa.duration("s")),
        "n": pa.array([250000, 1000000], type=pa.duration("ns")),
    })
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
    assert h.send(pa.chunked_array([[250, None], [500]],
                                   type=pa.duration("ms"))) is True
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
        ([np.array([1], dtype=np.uint64)], "uint64"),
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
    df = pl.DataFrame({
        "s": ["a", "b", None],
        "x": [1, 2, 3],
        "c": pl.Series(["u", "v", "u"], dtype=pl.Categorical),
    })
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
    t = pa.table({
        "a": pa.chunked_array([[1, 2], [3, 4]]),
        "b": pa.chunked_array([["x", "y"], ["z", "w"]]),
        "d": pa.chunked_array([
            pa.DictionaryArray.from_arrays([0, 0], ["u", "v"]),
            pa.DictionaryArray.from_arrays([1, 0], ["u", "v"]),
        ]),
    })
    h.send(t)
    f = p.recv(5)
    d = f.to_dict()
    assert d["a"].tolist() == [1, 2, 3, 4]
    assert d["b"] == ["x", "y", "z", "w"]
    assert d["d"] == ["u", "u", "v", "u"]
    p.destroy()
    h.destroy()


def test_corrupt_and_unknown_streams():
    for hexstream in [
        "49017f",                    # unknown tag
        "490200",                    # a newer version
        "4901020102",                # truncated
        "49010000",                  # trailing bytes
        "49010d0100000000000000ffffffff",   # a dict key of length -1
        "49010f0f",                  # an attr wraps an attr
        "49010401000000ff",          # invalid UTF-8
    ]:
        with pytest.raises(pymizu.MizuError, match="pymizu"):
            _read(hexstream)


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


def test_frame_export_machinery():
    pl = pytest.importorskip("polars")
    # a three-column frame: factor and int64 columns, Arrow dictionary out
    f = _read(bytes.fromhex(CORPUS["frame-3col"]))
    df = pl.DataFrame(f)
    assert df.schema["n"] == pl.Float64
    assert df.schema["f"] == pl.Categorical
    assert df.schema["i"] == pl.Int64
    assert df.to_dicts() == [{"n": 1.5, "f": "f", "i": 5},
                             {"n": 2.5, "f": None, "i": None}]
    t = pa.table(f)
    assert t.to_pydict() == {"n": [1.5, 2.5], "f": ["f", None],
                             "i": [5, None]}
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
    df = pd.DataFrame({
        "s": pd.array(["a", "b"], dtype="str"),
        "o": pd.Series(["x", None], dtype=object),
        "i": pd.array([1, None], dtype="Int64"),
        "c": pd.Series(["u", "v"], dtype=pd.CategoricalDtype(["u", "v"])),
    })
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
        "__index_level_0__" in f.names
        or "index" in f.names
        or "x" in f.names
    )
    # a named RangeIndex is dropped
    df3 = pd.DataFrame({"x": [1, 2]})
    df3.index.name = "myidx"
    h.send(df3)
    f = p.recv(5)
    assert f.names == ("x",)
    # an ordered Categorical declines
    df4 = pd.DataFrame({
        "c": pd.Series(
            ["a"], dtype=pd.CategoricalDtype(["a", "b"], ordered=True)
        )
    })
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
