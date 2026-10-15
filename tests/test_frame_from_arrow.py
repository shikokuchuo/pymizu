"""Frame.from_arrow(): the opt-in zero-copy frame path for Python-to-Python
channels — buffer adoption at construction (nulls included), null fidelity
on the adopted state per reader, the copy arms (type-forced, chunk- and
slice-forced), the pickle fallback below the zero-copy floor, and the
decline surface (unsupported types name their column; malformed producer
geometry is never silently trusted)."""

import gc
import pickle
import struct
import warnings

import pytest
from tests.helpers import RawArrowStream

import pymizu
from pymizu import _pymizu

pa = pytest.importorskip("pyarrow", reason="pyarrow not installed")
np = pytest.importorskip("numpy", reason="numpy not installed")


def same_pair():
    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    p.ready_set()
    assert h.ready_wait(10)
    return h, p


def big_table(n=20000):
    """One column per adopted kind, every tenth value null, past the 32 KiB
    zero-copy floor."""

    def maybe(vals, k):
        return None if k % 10 == 0 else vals

    return pa.table(
        {
            "x": pa.array([maybe(k * 0.5, k) for k in range(n)]),
            "i": pa.array([maybe(k, k) for k in range(n)], type=pa.int32()),
            "l": pa.array([maybe(k, k) for k in range(n)], type=pa.int64()),
            # uint8 stays null-free: Arrow nulls cannot cross in a uint8
            # column (R raw vectors have no NA — the batch check declines)
            "u": pa.array([k % 251 for k in range(n)], type=pa.uint8()),
            "s": pa.array([maybe(f"s{k % 100}", k) for k in range(n)]),
            "f": pa.array(
                [maybe(["u", "v", "w"][k % 3], k) for k in range(n)]
            ).dictionary_encode(),
            "d": pa.array(
                [maybe(19000 + k, k) for k in range(n)], type=pa.date32()
            ),
            "t": pa.array(
                [maybe(1700000000000000 + k, k) for k in range(n)],
                type=pa.timestamp("us"),
            ),
            "dur": pa.array(
                [maybe(1000 + k, k) for k in range(n)],
                type=pa.duration("us"),
            ),
        }
    )


def test_from_arrow_adopts_single_batch():
    f = pymizu.Frame.from_arrow(big_table())
    dbg = _pymizu._frame_debug(f)
    # every column of a single-batch table adopts; one hold, one ref each;
    # only the null-free uint8 column sets known_free
    assert all(dbg["adopted"])
    assert dbg["known_free"] == tuple(
        c == "u" for c in big_table().column_names
    )
    assert dbg["hold_refs"] == len(big_table().column_names)


def test_from_arrow_repeated_exports():
    # repeat exports serve the recorded null counts — the producer's for
    # adopted columns, the build's for copied ones — and the cached NUL
    # name store, never re-walking a bitmap; exact every time
    f = pymizu.Frame.from_arrow(big_table())
    f2 = pymizu.Frame.from_arrow(
        pa.Table.from_batches(big_table().to_batches(max_chunksize=10000))
    )
    names = big_table().column_names
    for _ in range(3):
        for src in (f, f2):
            t = pa.table(src)
            assert t.schema.names == names
            for c in names:
                assert t.column(c).null_count == (0 if c == "u" else 2000)
            assert t.column("i").slice(0, 3).to_pylist() == [None, 1, 2]
            assert t.column("s").slice(0, 3).to_pylist() == [None, "s1", "s2"]


def test_from_arrow_null_fidelity_readers():
    f = pymizu.Frame.from_arrow(big_table())
    # to_dict: None / NaN / NaT at null slots, never garbage or a sentinel
    d = f.to_dict()
    assert np.isnan(d["x"][0]) and d["x"][1] == 0.5
    assert np.isnan(d["i"][0]) and d["i"][1] == 1
    assert d["l"][0] == -(2**63) and d["l"][1] == 1  # int64 has no NA form
    assert d["s"][0] is None and d["s"][1] == "s1"
    assert d["f"][0] is None and d["f"][1] == "v"
    assert np.isnat(d["d"][0]) and d["d"][1] == np.datetime64("2022-01-09")
    assert np.isnat(d["t"][0]) and d["t"][1] == np.datetime64(
        "2023-11-14T22:13:20.000001"
    )
    assert np.isnat(d["dur"][0]) and d["dur"][1] == np.timedelta64(1001, "us")
    # the Arrow export (pre-send): null-for-null against the source
    t = pa.table(f)
    src = big_table()
    for c in t.column_names:
        assert t.column(c).null_count == src.column(c).null_count


def test_from_arrow_pickle_preserves_nulls():
    f = pymizu.Frame.from_arrow(big_table())
    f2 = pickle.loads(pickle.dumps(f))
    d = f2.to_dict()
    assert np.isnan(d["x"][0]) and d["x"][1] == 0.5
    assert np.isnan(d["i"][0]) and d["i"][1] == 1
    assert d["s"][0] is None and d["s"][1] == "s1"
    assert d["f"][0] is None and d["f"][1] == "v"
    # the rebuilt frame is copy-backed (no adoption, no hold)
    dbg = _pymizu._frame_debug(f2)
    assert not any(dbg["adopted"]) and dbg["hold_refs"] is None


def test_from_arrow_mixed_frame():
    # adopted nullable double + copied bool + adopted utf8 with nulls
    t = pa.table(
        {
            "x": pa.array([1.5, None, 3.5]),
            "b": pa.array([True, None, False]),
            "s": pa.array(["a", None, "c"]),
        }
    )
    f = pymizu.Frame.from_arrow(t)
    dbg = _pymizu._frame_debug(f)
    assert dbg["adopted"] == (True, False, True)
    assert dbg["hold_refs"] == 2
    d = f.to_dict()
    # the LGL copy with nulls is the int32-with-sentinel form
    assert d["b"].tolist() == [1, -(2**31), 0]
    assert np.isnan(d["x"][1]) and d["s"][1] is None
    out = pa.table(f)
    assert out.schema.field("b").type == pa.bool_()
    assert [c.null_count for c in out.columns] == [1, 1, 1]


def test_from_arrow_masked_columns_foreign_send():
    """A Frame.from_arrow send on a foreign channel honors the adopted
    columns' validity bitmaps: every masked kind round-trips its nulls
    (the 'I' frame emit keyed on the data slots alone before — garbage at
    nulls)."""
    from tests.helpers import foreign_pair

    t = pa.table(
        {
            "x": pa.array([1.5, None, 3.5]),
            "i": pa.array([1, None, 3], type=pa.int32()),
            "l": pa.array([1, None, 3], type=pa.int64()),
            "s": pa.array(["a", None, "c"]),
            "f": pa.array(["u", None, "w"]).dictionary_encode(),
            "d": pa.array([19000, None, 19002], type=pa.date32()),
            "t": pa.array(
                [1700000000000000, None, 1700000000000002],
                type=pa.timestamp("us"),
            ),
            "dur": pa.array([1000, None, 1002], type=pa.duration("us")),
        }
    )
    h, p = foreign_pair(
        caps=7
    )  # MIZU_CAP_MIZS | MIZU_CAP_ATTRS | MIZU_CAP_MIZL
    assert h.send(pymizu.Frame.from_arrow(t)) is True
    d = p.recv(30).to_dict()
    h.destroy()
    p.destroy()
    assert np.isnan(d["x"][1]) and d["x"][0] == 1.5
    assert np.isnan(d["i"][1]) and d["i"][0] == 1
    assert d["l"][1] == -(2**63) and d["l"][0] == 1  # the int64 sentinel
    assert d["s"][1] is None and d["s"][0] == "a"
    assert d["f"][1] is None and d["f"][0] == "u"
    assert np.isnat(d["d"][1])
    assert np.isnat(d["t"][1])
    assert np.isnat(d["dur"][1])


def test_from_arrow_null_count_unknown_and_known_free():
    # a crafted null_count = -1 column adopts and reads by its bitmap
    buf_valid = pa.py_buffer(bytes([0b0101]))
    buf_vals = pa.py_buffer(np.array([10, 20, 30, 40], dtype=np.int32))
    arr = pa.Array.from_buffers(
        pa.int32(), 4, [buf_valid, buf_vals], null_count=-1
    )
    f = pymizu.Frame.from_arrow(pa.table({"i": arr}))
    dbg = _pymizu._frame_debug(f)
    assert dbg["adopted"] == (True,) and dbg["known_free"] == (False,)
    got = f.to_dict()["i"].tolist()
    assert got[0] == 10 and got[2] == 30
    assert np.isnan(got[1]) and np.isnan(got[3])
    assert pa.table(f).column("i").null_count == 2
    # a null-free column sets known_free (the export skips its scan)
    f2 = pymizu.Frame.from_arrow(
        pa.table({"i": pa.array([1, 2, 3], type=pa.int32())})
    )
    assert _pymizu._frame_debug(f2)["known_free"] == (True,)
    assert pa.table(f2).column("i").null_count == 0


def test_from_arrow_producer_death():
    gc.collect()
    base = pa.total_allocated_bytes()
    t = big_table()
    f = pymizu.Frame.from_arrow(t)
    del t
    gc.collect()
    # the adoption hold keeps the producer's buffers alive
    assert pa.total_allocated_bytes() > base
    d = f.to_dict()
    assert d["s"][1] == "s1" and np.isnan(d["x"][0])
    h, p = same_pair()
    h.send(f)
    g = p.recv(10)
    assert pa.table(g).column("i").null_count == 2000
    assert pa.table(f).column("s").null_count == 2000
    p.destroy()
    h.destroy()
    del f, g, d
    gc.collect()
    assert pa.total_allocated_bytes() <= base + 4096


def test_from_arrow_midstream_failure_releases():
    base = pa.total_allocated_bytes()

    def gen():
        yield pa.record_batch([pa.array([1.5] * 100000)], ["x"])
        raise RuntimeError("boom")

    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("x", pa.float64())]), gen()
    )
    with pytest.raises(pymizu.MizuError, match="the Arrow stream failed"):
        pymizu.Frame.from_arrow(reader)
    del reader
    gc.collect()
    assert pa.total_allocated_bytes() <= base + 4096


def test_from_arrow_build_failure_releases_both_arms():
    # column 0 adopts, column 1's conversion warning errors mid-build (a
    # sliced — copy-arm — masked int32 with a genuine INT32_MIN)
    base = pa.total_allocated_bytes()
    t = pa.table(
        {
            "x": pa.array([1.5] * 100000),
            "i": pa.array([0] + [-(2**31), None] * 50000, type=pa.int32())[1:],
        }
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        with pytest.raises(RuntimeWarning, match="NA_integer_ in R"):
            pymizu.Frame.from_arrow(t)
    del t
    gc.collect()
    assert pa.total_allocated_bytes() <= base + 4096


def test_from_arrow_roundtrip_all_kinds():
    f = pymizu.Frame.from_arrow(big_table())
    h, p = same_pair()
    h.send(f)
    g = p.recv(10)
    # region-backed past the zero-copy floor
    d = g.to_dict()
    assert type(d["x"].base).__name__ == "_ShmView"
    out = pa.table(g)
    src = big_table()
    for c in src.column_names:
        assert out.column(c).null_count == src.column(c).null_count
    assert out.column("i").slice(0, 3).to_pylist() == [None, 1, 2]
    assert out.column("s")[1].as_py() == "s1"
    assert out.column("f")[1].as_py() == "v"
    assert out.column("d")[1] == src.column("d")[1]
    p.destroy()
    h.destroy()


def test_from_arrow_echo_peer():
    # the spawned-peer harness: the region handoff between real processes
    peer = """
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(x)
"""
    f = pymizu.Frame.from_arrow(big_table())
    with pymizu.Channel.create(peer) as ch:
        ch.send(f)
        g = ch.recv(timeout=10)
        out = pa.table(g)
        assert out.column("i").null_count == 2000
        assert out.column("i").slice(0, 3).to_pylist() == [None, 1, 2]


def test_from_arrow_double_send():
    f = pymizu.Frame.from_arrow(big_table())
    h, p = same_pair()
    h.send(f)
    g1 = p.recv(10)
    h.send(f)
    g2 = p.recv(10)
    assert pa.table(g1).column("i").null_count == 2000
    assert pa.table(g2).column("i").null_count == 2000
    p.destroy()
    h.destroy()


def test_from_arrow_chunked_reader_copy_path():
    # a chunked RecordBatchReader: per-column concat across batch boundaries
    b1 = pa.record_batch(
        [pa.array([1, None, 3], type=pa.int32()), pa.array(["a", "b", None])],
        names=["i", "s"],
    )
    b2 = pa.record_batch(
        [pa.array([None, 5], type=pa.int32()), pa.array([None, "e"])],
        names=["i", "s"],
    )
    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("i", pa.int32()), ("s", pa.utf8())]), [b1, b2]
    )
    f = pymizu.Frame.from_arrow(reader)
    dbg = _pymizu._frame_debug(f)
    assert not any(dbg["adopted"]) and dbg["hold_refs"] is None
    d = f.to_dict()
    got = d["i"].tolist()
    assert got[0] == 1 and got[2] == 3 and got[4] == 5
    assert np.isnan(got[1]) and np.isnan(got[3])
    assert d["s"] == ["a", "b", None, None, "e"]
    out = pa.table(f)
    assert out.column("i").null_count == 2
    assert out.column("s").null_count == 2
    # combine_chunks() is the route back to the adopt path
    t = pa.table({"i": pa.chunked_array([b1.column(0), b2.column(0)])})
    f2 = pymizu.Frame.from_arrow(t.combine_chunks())
    assert _pymizu._frame_debug(f2)["adopted"] == (True,)


def test_from_arrow_polars_dataframe():
    pl = pytest.importorskip("polars")
    df = pl.DataFrame(
        {"x": [1.5, None, 3.5], "s": ["a", None, "c"], "i": [1, None, 3]}
    )
    f = pymizu.Frame.from_arrow(df)
    d = f.to_dict()
    assert np.isnan(d["x"][1]) and d["x"][0] == 1.5
    assert d["s"] == ["a", None, "c"]
    # polars ints export as Int64: the masked null is the int64 NA form
    assert d["i"][1] == -(2**63) and d["i"][0] == 1
    out = pa.table(f)
    assert [c.null_count for c in out.columns] == [1, 1, 1]


def test_from_arrow_duckdb_relation():
    duckdb = pytest.importorskip("duckdb")
    rel = duckdb.sql(
        "select * from (values (1, 'a'), (2, 'b')) as t(i, s)"
    ).arrow()
    f = pymizu.Frame.from_arrow(rel)
    assert f.to_dict()["s"] == ["a", "b"]


def test_from_arrow_edge_cases():
    # zero rows / zero-length columns
    f = pymizu.Frame.from_arrow(
        pa.table(
            {
                "i": pa.array([], type=pa.int32()),
                "s": pa.array([], type=pa.utf8()),
                "d": pa.DictionaryArray.from_arrays(
                    pa.array([], type=pa.int32()), pa.array([], type=pa.utf8())
                ),
            }
        )
    )
    assert len(f) == 0
    d = f.to_dict()
    assert len(d["i"]) == 0 and d["s"] == [] and d["d"] == []
    # an all-null column
    f = pymizu.Frame.from_arrow(
        pa.table({"i": pa.array([None, None, None], type=pa.int32())})
    )
    assert all(np.isnan(v) for v in f.to_dict()["i"].tolist())
    assert pa.table(f).column("i").null_count == 3
    # a sliced table: the per-column copy path, values still exact
    t = pa.table(
        {
            "i": pa.array(range(100), type=pa.int32()),
            "s": pa.array([f"s{k}" for k in range(100)]),
        }
    ).slice(20)
    f = pymizu.Frame.from_arrow(t)
    assert not any(_pymizu._frame_debug(f)["adopted"])
    d = f.to_dict()
    assert len(f) == 80
    assert d["i"][:3].tolist() == [20, 21, 22]
    assert d["s"][:3] == ["s20", "s21", "s22"]


def test_from_arrow_below_floor_pickles():
    # a small constructed frame takes the pickle fallback; nulls intact
    t = pa.table(
        {
            "x": pa.array([1.5, None, 3.5]),
            "i": pa.array([1, None, 3], type=pa.int32()),
            "s": pa.array(["a", None, "c"]),
        }
    )
    f = pymizu.Frame.from_arrow(t)
    assert all(_pymizu._frame_debug(f)["adopted"])
    h, p = same_pair()
    h.send(f)
    g = p.recv(10)
    d = g.to_dict()
    assert type(d["x"]).__name__ != "_ShmView"  # a pickle copy
    assert np.isnan(d["x"][1]) and d["x"][0] == 1.5
    assert np.isnan(d["i"][1]) and d["i"][0] == 1
    assert d["s"] == ["a", None, "c"]
    p.destroy()
    h.destroy()


def test_from_arrow_conversions_not_declines():
    # half_float / string_view are conversions (copies), not declines;
    # uint64 keeps its dtype verbatim (adopted like the other exact kinds)
    t = pa.table(
        {
            "u": pa.array([1, 2, 3], type=pa.uint64()),
            "e": pa.array([1.5, 2.5, 3.5], type=pa.float16()),
            "v": pa.array(["a", "b", "c"], type=pa.string_view()),
            "I": pa.array([1, None, 3], type=pa.uint32()),
            "c": pa.array([1, 2, 3], type=pa.int8()),
            "U": pa.array(["a", None, "c"], type=pa.large_utf8()),
            "n": pa.array([1000, None, 250000], type=pa.duration("ns")),
        }
    )
    f = pymizu.Frame.from_arrow(t)
    # the conversions copy; uint64 and duration[ns] adopt verbatim
    dbg = _pymizu._frame_debug(f)
    assert dbg["adopted"] == (True, False, False, False, False, False, True)
    d = f.to_dict()
    assert d["u"].dtype == np.uint64 and d["u"].tolist() == [1, 2, 3]
    assert d["e"].dtype == np.float64 and d["e"].tolist() == [1.5, 2.5, 3.5]
    assert d["v"] == ["a", "b", "c"]
    assert d["I"].dtype == np.float64 and np.isnan(d["I"][1])
    assert d["c"].tolist() == [1, 2, 3]
    assert d["U"] == ["a", None, "c"]
    # duration[ns] keeps its counts verbatim, NaT at the null
    assert dbg["units"][-1] == "n"
    assert d["n"].dtype == np.dtype("timedelta64[ns]")
    assert d["n"][0] == np.timedelta64(1000, "ns")
    assert np.isnat(d["n"][1])
    assert d["n"][2] == np.timedelta64(250000, "ns")
    out = pa.table(f)
    assert out.schema.field("u").type == pa.uint64()
    assert out.schema.field("v").type == pa.string()
    assert out.schema.field("n").type == pa.duration("ns")


def ns_table(n=20000):
    """Sub-µs-fraction ns columns past the zero-copy floor."""
    return pa.table(
        {
            "t": pa.array(
                [1700000000123456789, None, -3600000000001]
                + [1700000000000000000 + k for k in range(3, n)],
                type=pa.timestamp("ns", tz="America/New_York"),
            ),
            "u": pa.array(
                [90, None, 86400000000000] + list(range(3, n)),
                type=pa.duration("ns"),
            ),
            "x": pa.array([k * 0.5 for k in range(n)]),
        }
    )


def test_from_arrow_ns_verbatim_construction():
    # timestamp[ns]/duration[ns] adopt verbatim: the counts, the unit and
    # the tzone ride the column — no double-second rescale anywhere
    f = pymizu.Frame.from_arrow(ns_table())
    dbg = _pymizu._frame_debug(f)
    assert dbg["adopted"] == (True, True, True)
    assert dbg["units"] == ("n", "n", None)
    d = f.to_dict()
    assert d["t"].dtype == np.dtype("datetime64[ns]")
    assert d["t"][0] == np.datetime64(1700000000123456789, "ns")
    assert np.isnat(d["t"][1])
    assert d["t"][2] == np.datetime64(-3600000000001, "ns")
    assert d["u"].dtype == np.dtype("timedelta64[ns]")
    assert d["u"][0] == np.timedelta64(90, "ns")
    assert np.isnat(d["u"][1])
    assert d["u"][2] == np.timedelta64(86400000000000, "ns")
    out = pa.table(f)
    assert out.schema.field("t").type == pa.timestamp(
        "ns", tz="America/New_York"
    )
    assert out.schema.field("u").type == pa.duration("ns")
    src = ns_table()
    for c in src.column_names:
        assert out.column(c).null_count == src.column(c).null_count


def test_from_arrow_ns_wire_verbatim():
    # past the zero-copy floor the ns columns cross as INT64+tscale MIZL
    # leaves and read back verbatim (the debug surface's units are the
    # leaf-form proof: 'n' arrives only off an INT64+tscale leaf)
    f = pymizu.Frame.from_arrow(ns_table())
    h, p = same_pair()
    h.send(f)
    g = p.recv(10)
    assert _pymizu._frame_debug(g)["units"] == ("n", "n", None)
    d = g.to_dict()
    assert type(d["x"].base).__name__ == "_ShmView"  # region-backed
    assert d["t"].dtype == np.dtype("datetime64[ns]")
    assert d["t"][0] == np.datetime64(1700000000123456789, "ns")
    assert d["u"][2] == np.timedelta64(86400000000000, "ns")
    out = pa.table(g)
    assert out.schema.field("t").type == pa.timestamp(
        "ns", tz="America/New_York"
    )
    assert out.schema.field("u").type == pa.duration("ns")
    assert out.column("t").null_count == 1
    p.destroy()
    h.destroy()


def test_from_arrow_ns_echo_peer():
    # the spawned-peer harness: the region handoff between real processes
    peer = """
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(x)
"""
    f = pymizu.Frame.from_arrow(ns_table())
    with pymizu.Channel.create(peer) as ch:
        ch.send(f)
        g = ch.recv(timeout=10)
        assert _pymizu._frame_debug(g)["units"][:2] == ("n", "n")
        d = g.to_dict()
        assert d["t"][0] == np.datetime64(1700000000123456789, "ns")
        assert pa.table(g).column("t").null_count == 1


def test_from_arrow_ns_skew_fallback():
    # a peer ident without MIZU_CAP_MIZL_TSN (the test-only _ident
    # override): the write takes the REAL-seconds fallback — the peer reads
    # µs-rounded values, no error
    f = pymizu.Frame.from_arrow(ns_table())
    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token, _ident=(3, 31))
    p.ready_set()
    assert h.ready_wait(10)
    assert h.send(f) is True
    g = p.recv(10)
    assert _pymizu._frame_debug(g)["units"] == ("u", "u", None)
    d = g.to_dict()
    assert d["t"].dtype == np.dtype("datetime64[us]")
    assert d["t"][0] == np.datetime64(round(1700000000123456789 * 1e-3), "us")
    assert np.isnat(d["t"][1])
    assert d["u"].dtype == np.dtype("timedelta64[us]")
    out = pa.table(g)
    assert out.schema.field("t").type == pa.timestamp(
        "us", tz="America/New_York"
    )
    assert out.schema.field("u").type == pa.duration("us")
    p.destroy()
    h.destroy()


def test_from_arrow_sm_verbatim():
    # the s/m corollary: second/millisecond columns adopt verbatim and
    # surface at their own unit (no coerced µs); a far-future timestamp[s]
    # past the old 9e18 µs decline round-trips exactly, copy arm included
    t = pa.table(
        {
            "s": pa.array([1 << 60, None, 1700000000], type=pa.timestamp("s")),
            "m": pa.array([1700000000000, None, 60], type=pa.timestamp("ms")),
        }
    )
    f = pymizu.Frame.from_arrow(t)
    dbg = _pymizu._frame_debug(f)
    assert dbg["adopted"] == (True, True)
    assert dbg["units"] == ("s", "m")
    d = f.to_dict()
    assert d["s"].dtype == np.dtype("datetime64[s]")
    assert d["s"][0] == np.datetime64(1 << 60, "s")
    assert np.isnat(d["s"][1])
    assert d["m"].dtype == np.dtype("datetime64[ms]")
    assert d["m"][0] == np.datetime64(1700000000000, "ms")
    out = pa.table(f)
    assert out.schema.field("s").type == pa.timestamp("s")
    assert out.schema.field("m").type == pa.timestamp("ms")
    # the copy arm (a chunked reader) is verbatim too
    b1 = pa.record_batch(
        [pa.array([1 << 60, None], type=pa.timestamp("s"))], names=["s"]
    )
    b2 = pa.record_batch([pa.array([0], type=pa.timestamp("s"))], names=["s"])
    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("s", pa.timestamp("s"))]), [b1, b2]
    )
    f2 = pymizu.Frame.from_arrow(reader)
    dbg2 = _pymizu._frame_debug(f2)
    assert not any(dbg2["adopted"]) and dbg2["units"] == ("s",)
    d2 = f2.to_dict()
    assert d2["s"][0] == np.datetime64(1 << 60, "s")
    assert np.isnat(d2["s"][1])
    # the malformed-utf8-offsets gate still declines a hand-rolled column
    offs = pa.py_buffer(struct.pack("<4i", 0, 4, 2, 8))
    data = pa.py_buffer(b"abcdefgh")
    arr = pa.Array.from_buffers(pa.string(), 3, [None, offs, data])
    with pytest.raises(pymizu.MizuError, match="column 's'.*malformed"):
        pymizu.Frame.from_arrow(pa.table({"s": arr}))


def test_from_arrow_ns_pickle():
    # below the zero-copy floor the pickle form carries the unit; an
    # old-form pickle (no unit element) restores as µs
    f = pymizu.Frame.from_arrow(
        pa.table(
            {
                "t": pa.array(
                    [1700000000123456789, None], type=pa.timestamp("ns")
                ),
                "u": pa.array([90, None], type=pa.duration("ns")),
            }
        )
    )
    h, p = same_pair()
    h.send(f)
    g = p.recv(10)
    d = g.to_dict()
    assert d["t"].dtype == np.dtype("datetime64[ns]")
    assert d["t"][0] == np.datetime64(1700000000123456789, "ns")
    assert d["u"][0] == np.timedelta64(90, "ns")
    p.destroy()
    h.destroy()
    f2 = pickle.loads(pickle.dumps(f))
    assert _pymizu._frame_debug(f2)["units"] == ("n", "n")
    d = f2.to_dict()
    assert d["t"][0] == np.datetime64(1700000000123456789, "ns")
    assert np.isnat(d["u"][1])
    old_ts = _pymizu._frame_rebuild(
        ["t"], [("t", [1700000000000000, None], "UTC")], None
    )
    assert _pymizu._frame_debug(old_ts)["units"] == ("u",)
    assert old_ts.to_dict()["t"].dtype == np.dtype("datetime64[us]")
    old_td = _pymizu._frame_rebuild(["u"], [("T", [1000, None])], None)
    assert old_td.to_dict()["u"].dtype == np.dtype("timedelta64[us]")


def test_from_arrow_ns_edge_cases():
    # an all-null ns column adopts its bitmap; a zero-row column keeps its
    # dtype
    f = pymizu.Frame.from_arrow(
        pa.table({"t": pa.array([None, None, None], type=pa.timestamp("ns"))})
    )
    assert _pymizu._frame_debug(f)["adopted"] == (True,)
    assert all(np.isnat(v) for v in f.to_dict()["t"])
    assert pa.table(f).column("t").null_count == 3
    f = pymizu.Frame.from_arrow(
        pa.table({"t": pa.array([], type=pa.timestamp("ns"))})
    )
    assert len(f) == 0
    assert f.to_dict()["t"].dtype == np.dtype("datetime64[ns]")
    assert pa.table(f).schema.field("t").type == pa.timestamp("ns")


def test_from_arrow_uint64_past_2_to_53_warns():
    # exact at adoption (no conversion, no warning); the warning fires at
    # the loss — a foreign send to a peer without MIZU_CAP_MIZL_U64V (bits
    # 0-5 pass the MIZL gate), on the MIZL-writer and 'I'-writer paths alike
    from tests.helpers import foreign_pair

    t = pa.table({"u": pa.array([2**53 + 1] * 20000, type=pa.uint64())})
    f = pymizu.Frame.from_arrow(t)
    assert f.to_dict()["u"].tolist() == [2**53 + 1] * 20000
    h, p = foreign_pair(caps=63)
    with pytest.warns(RuntimeWarning, match=r"2\^53"):
        assert h.send(t) is True
    assert np.isnan(p.recv(30).to_dict()["u"][0])
    with pytest.warns(RuntimeWarning, match=r"2\^53"):
        assert h.send(f) is True
    assert np.isnan(p.recv(30).to_dict()["u"][0])
    p.destroy()
    h.destroy()


def test_from_arrow_declines_unsupported_types():
    cases = {
        "bin": pa.array([b"a"], type=pa.binary()),
        "dec": pa.array([1], type=pa.decimal128(10, 2)),
        "d64": pa.array([1], type=pa.date64()),
        "tm": pa.array([1], type=pa.time64("us")),
        "lst": pa.array([[1, 2]], type=pa.list_(pa.int32())),
        "stc": pa.array([{"a": 1}], type=pa.struct({"a": pa.int32()})),
    }
    for name, col in cases.items():
        with pytest.raises(pymizu.MizuError, match=f"column '{name}'"):
            pymizu.Frame.from_arrow(pa.table({name: col}))
    # an ordered dictionary has no portable home
    od = pa.DictionaryArray.from_arrays(
        pa.array([0, 1], type=pa.int32()), pa.array(["a", "b"])
    )
    t = pa.table(
        {"ord": od.cast(pa.dictionary(pa.int32(), pa.utf8(), ordered=True))}
    )
    with pytest.raises(pymizu.MizuError, match="column 'ord'"):
        pymizu.Frame.from_arrow(t)


def test_from_arrow_malformed_offsets_declined():
    # a hand-rolled producer with non-monotonic string offsets is declined
    # at the construction-time validation walk, never silently trusted
    offs = pa.py_buffer(struct.pack("<3i", 0, 4, 2))
    data = pa.py_buffer(b"abcd")
    arr = pa.Array.from_buffers(pa.string(), 2, [None, offs, data])
    with pytest.raises(pymizu.MizuError, match="column 's'.*malformed"):
        pymizu.Frame.from_arrow(pa.table({"s": arr}))


def test_from_arrow_producer_failures():
    # no __arrow_c_stream__
    with pytest.raises(pymizu.MizuError, match="no __arrow_c_stream__"):
        pymizu.Frame.from_arrow(object())

    # the export itself fails
    class BadExport:
        def __arrow_c_stream__(self):
            raise RuntimeError("no stream today")

    with pytest.raises(pymizu.MizuError, match="no stream today"):
        pymizu.Frame.from_arrow(BadExport())

    # a non-struct root (a bare array, not a table)
    with pytest.raises(pymizu.MizuError, match="not a struct"):
        pymizu.Frame.from_arrow(pa.chunked_array([[1.5, 2.5]]))


class UnknownNullsStream(RawArrowStream):
    """Reports null_count = -1 (the C Data Interface's "not yet computed")
    on every column, with the validity bitmap present."""

    def __init__(self, columns):
        super().__init__(columns)
        for _, children, *_ in self._batches:
            for a in children:
                a.null_count = -1


def test_from_arrow_unknown_null_count():
    # a spec-legal producer count of -1: the export serves the popcount,
    # never a bare MemoryError (P-14). The producer must stay referenced:
    # the adoption borrows its buffers
    prod = UnknownNullsStream([("g", "x", [([1.5, 2.0, 3.0], [1, 0, 1])])])
    f = pymizu.Frame.from_arrow(prod)
    d = f.to_dict()["x"]
    assert d[0] == 1.5 and d[1] != d[1] and d[2] == 3.0
    t = pa.table(f)
    assert t.column("x").null_count == 1
    assert t.column("x").to_pylist() == [1.5, None, 3.0]


def test_frame_rebuild_rejects_a_surrogate_tz():
    # a crafted pickle state with a lone-surrogate tz raises the
    # malformed-state ValueError, never a SystemError (P-20)
    f = pymizu.Frame.from_arrow(
        pa.table({"ts": pa.array([0, 1], type=pa.timestamp("us", tz="UTC"))})
    )
    fn, state = f.__reduce__()
    names, columns, rn = state
    kind, payload, tz, unit = columns[0]
    columns[0] = (kind, payload, "bad\udce9tz", unit)
    with pytest.raises(ValueError, match="malformed frame rebuild state"):
        fn(names, columns, rn)


def test_frame_type_not_constructible():
    with pytest.raises(TypeError, match="cannot create"):
        pymizu.Frame()


def test_frame_debug_type_error():
    with pytest.raises(TypeError, match="needs a Frame"):
        _pymizu._frame_debug(42)
