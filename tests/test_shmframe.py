"""The MIZL frame writer (Phase 3.6) and the export-provenance REF
(Phase 3.8): in-process coverage over the foreign_pair harness (the host
stages for an R peer word) and same-language channels. The real-R
round-trips are test_crosslang.py's."""

import gc
import pickle

import pytest
from tests.helpers import foreign_pair

import pymizu
from pymizu import _pymizu

pa = pytest.importorskip("pyarrow", reason="the frame writer is Arrow-fed")
np = pytest.importorskip("numpy")

CAPS_R = 7   # MIZU_CAP_MIZS | MIZU_CAP_ATTRS | MIZU_CAP_MIZL
CAPS_R33 = CAPS_R | 16   # + MIZU_CAP_MIZL_REF (the F2 remote leaf)


def same_pair():
    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token)
    p.ready_set()
    assert h.ready_wait(10)
    return h, p


def big_table(n=20000, with_nulls=True):
    """A frame past the 32 KiB zero-copy floor, one column per kind."""
    import datetime

    def maybe(vals, k):
        return None if with_nulls and k % 10 == 0 else vals

    return pa.table({
        "i": pa.array([maybe(k, k) for k in range(n)], type=pa.int32()),
        "x": pa.array([maybe(k * 0.5, k) for k in range(n)],
                      type=pa.float64()),
        "l": pa.array([maybe(k, k) for k in range(n)], type=pa.int64()),
        "b": pa.array([maybe(k % 2 == 0, k) for k in range(n)]),
        "s": pa.array([maybe(f"s{k % 100}", k) for k in range(n)]),
        "f": pa.array([maybe(["u", "v", "w"][k % 3], k)
                       for k in range(n)]).dictionary_encode(),
        "d": pa.array([maybe(datetime.date(2020, 1, 1) +
                             datetime.timedelta(days=k), k)
                       for k in range(n)], type=pa.date32()),
        "t": pa.array([maybe(datetime.datetime(2021, 1, 1) +
                             datetime.timedelta(seconds=k), k)
                       for k in range(n)], type=pa.timestamp("us")),
    })


def test_mizl_frame_fixed_width():
    h, p = foreign_pair(caps=CAPS_R)
    tbl = big_table(with_nulls=False).select(["i", "x", "l", "b"])
    h.send(tbl)
    f = p.recv(10)
    assert type(f).__name__ == "Frame"
    d = f.to_dict()
    assert type(d["i"].base).__name__ == "_ShmView"   # region-backed
    assert not d["i"].flags.writeable
    assert d["i"].dtype == np.int32 and d["i"][1:4].tolist() == [1, 2, 3]
    assert d["x"].dtype == np.float64 and d["x"][1] == 0.5
    assert d["l"].dtype == np.int64 and d["l"][1] == 1
    # NA-free by the validity section: to_dict applies the copied-read rule
    assert d["b"].dtype == np.bool_
    assert d["b"][1:4].tolist() == [False, True, False]
    t = pa.table(f)
    assert t.schema.field("b").type == pa.bool_()     # LGL exports bool
    assert all(t.column(c).null_count == 0 for c in t.column_names)
    p.destroy()
    h.destroy()


def test_mizl_frame_nulls_and_validity():
    h, p = foreign_pair(caps=CAPS_R)
    h.send(big_table().select(["i", "x", "l", "b"]))
    f = p.recv(10)
    d = f.to_dict()
    # the raw page buffers keep the in-band sentinels
    assert d["i"][0] == -2**31 and d["i"][1] == 1
    assert np.isnan(d["x"][0]) and d["x"][1] == 0.5
    assert d["l"][0] == -2**63
    # the Arrow exports read NA-honest off the validity sections
    t = pa.table(f)
    for c in ("i", "x", "l", "b"):
        assert t.column(c).null_count == 2000
    assert t.column("i").slice(0, 3).to_pylist() == [None, 1, 2]
    p.destroy()
    h.destroy()


def test_mizl_frame_string_forms():
    h, p = foreign_pair(caps=CAPS_R)
    n = 20000
    base = [f"s{k % 100}" for k in range(n)]
    # utf8 (i32 offsets), large_utf8 (i64), and polars' string_view
    h.send(pa.table({"u": pa.array(base, type=pa.string()),
                     "U": pa.array(base, type=pa.large_string())}))
    f = p.recv(10)
    d = f.to_dict()
    assert d["u"][:3] == ["s0", "s1", "s2"]
    assert d["U"][:3] == ["s0", "s1", "s2"]
    pl = pytest.importorskip("polars")
    h.send(pl.DataFrame({"v": base}))
    f2 = p.recv(10)
    assert f2.to_dict()["v"][:3] == ["s0", "s1", "s2"]
    p.destroy()
    h.destroy()


def test_mizl_frame_string_nulls():
    h, p = foreign_pair(caps=CAPS_R)
    n = 20000
    vals = [None if k % 7 == 0 else f"s{k}" for k in range(n)]
    h.send(pa.table({"s": pa.array(vals)}))
    f = p.recv(10)
    d = f.to_dict()
    assert d["s"][0] is None and d["s"][1] == "s1"
    assert pa.table(f).column("s").null_count == len([k for k in range(n)
                                                      if k % 7 == 0])
    p.destroy()
    h.destroy()


def test_mizl_frame_dictionary_and_temporal():
    h, p = foreign_pair(caps=CAPS_R)
    h.send(big_table().select(["f", "d", "t"]))
    f = p.recv(10)
    d = f.to_dict()
    assert d["f"][1:4] == ["v", "w", "u"]
    assert d["f"][0] is None
    assert str(d["d"].dtype) == "datetime64[D]"
    assert str(d["d"][1]) == "2020-01-02"
    assert str(d["t"].dtype) == "datetime64[us]"
    t = pa.table(f)
    assert t.schema.field("f").type == pa.dictionary(pa.int32(),
                                                     pa.string())
    assert t.column("f").null_count == 2000
    assert t.schema.field("d").type == pa.date32()
    assert t.schema.field("t").type == pa.timestamp("us", tz="UTC")
    p.destroy()
    h.destroy()


def test_mizl_frame_multibatch_and_sliced():
    h, p = foreign_pair(caps=CAPS_R)
    n = 20000
    t = pa.table({
        "a": pa.chunked_array([
            pa.array([None if k % 3 == 0 else k for k in range(n // 2)],
                     type=pa.int32()),
            pa.array([k for k in range(n // 2, n)], type=pa.int32()),
        ]),
        "s": pa.chunked_array([
            pa.array([f"a{k}" for k in range(n // 2)]),
            pa.array([None if k % 2 == 0 else f"b{k}"
                      for k in range(n // 2)]),
        ]),
    })
    h.send(t)
    f = p.recv(10)
    d = f.to_dict()
    assert d["a"][0] == -2**31 and d["a"][1] == 1 and d["a"][n - 1] == n - 1
    assert d["s"][0] == "a0" and d["s"][n // 2] is None
    assert pa.table(f).column("a").null_count == len(
        [k for k in range(n // 2) if k % 3 == 0])
    # a sliced column (a nonzero Arrow offset)
    h.send(pa.table({"i": pa.array([k for k in range(n)],
                                   type=pa.int32()).slice(100)}))
    f2 = p.recv(10)
    d2 = f2.to_dict()
    assert len(f2) == n - 100 and d2["i"][:3].tolist() == [100, 101, 102]
    p.destroy()
    h.destroy()


def test_mizl_frame_row_names_and_int64():
    h, p = foreign_pair(caps=CAPS_R)
    h.send(big_table(n=20000, with_nulls=False).select(["l"]))
    f = p.recv(10)
    assert f.row_names is None
    assert f.names == ("l",)
    p.destroy()
    h.destroy()


def test_mizl_declines():
    h, p = foreign_pair(caps=CAPS_R)
    n = 20000
    with pytest.raises(pymizu.DeclinedError, match="no portable home"):
        h.send(pa.table({"s": pa.array([[k] for k in range(n)],
                                      type=pa.list_(pa.int32()))}))
    with pytest.raises(pymizu.DeclinedError, match="zero-column"):
        h.send(pa.table({}))
    pl = pytest.importorskip("polars")
    with pytest.raises(pymizu.DeclinedError, match="pl.Categorical"):
        h.send(pl.DataFrame({
            "e": pl.Series(["a"] * n, dtype=pl.Enum(["a", "b"]))}))
    # uint8 with Arrow nulls: declined, never a sentinel write into the
    # 1-byte column — small (inline) and past the floor alike
    with pytest.raises(pymizu.DeclinedError, match="uint8"):
        h.send(pa.table({"u": pa.array([1, None, 3], type=pa.uint8())}))
    with pytest.raises(pymizu.DeclinedError, match="uint8"):
        h.send(pa.table({"u": pa.array([None if k % 5 == 0 else k % 256
                                        for k in range(n)],
                                       type=pa.uint8())}))
    p.destroy()
    h.destroy()


def test_mizl_capability_gate():
    # a peer short of the frame conjunction gets the attr copy instead:
    # caps 0, and ATTRS|MIZL without MIZS against a string column
    tbl = big_table().select(["i", "s"])
    for caps in (0, 2 | 4):
        h, p = foreign_pair(caps=caps)
        h.send(tbl)
        f = p.recv(10)
        d = f.to_dict()
        assert type(d["i"].base).__name__ != "_ShmView"   # a copy
        assert d["i"][1:4].tolist() == [1, 2, 3]
        assert d["s"][0] is None and d["s"][1] == "s1"
        p.destroy()
        h.destroy()


def test_same_language_frame_mizl():
    # a pymizu.Frame past the floor crosses Python -> Python as MIZL: a
    # region-backed Frame reads back (container-exact)
    h0, p0 = foreign_pair()   # a copy-backed Frame, built off the corpus path
    h0.send(big_table().select(["i", "x", "s", "f", "t"]))
    f = p0.recv(10)
    h, p = same_pair()
    h.send(f)
    got = p.recv(10)
    assert type(got).__name__ == "Frame"
    d = got.to_dict()
    assert type(d["i"].base).__name__ == "_ShmView"   # region-backed
    assert d["i"][0] == -2**31 and d["i"][1] == 1
    assert d["x"][1] == 0.5
    assert d["s"][0] is None and d["s"][1] == "s1"
    assert d["f"][1] == "v"
    assert pa.table(got).column("i").null_count == 2000
    # the tzone rides the leaf blob verbatim
    assert pa.table(got).schema.field("t").type == pa.timestamp(
        "us", tz="UTC")
    p.destroy()
    h.destroy()
    p0.destroy()
    h0.destroy()


def test_same_language_small_frame_pickles():
    h0, p0 = foreign_pair()
    h0.send(pa.table({"i": pa.array([1, 2, 3], type=pa.int32())}))
    f = p0.recv(10)
    h, p = same_pair()
    h.send(f)
    got = p.recv(10)
    d = got.to_dict()
    assert type(d["i"].base).__name__ != "_ShmView"   # a pickle copy
    assert d["i"].tolist() == [1, 2, 3]
    p.destroy()
    h.destroy()
    p0.destroy()
    h0.destroy()


def test_same_language_pandas_stays_pandas():
    pd = pytest.importorskip("pandas")
    n = 20000
    df = pd.DataFrame({"i": np.arange(n, dtype=np.int32),
                       "x": np.arange(n, dtype=np.float64)})
    h, p = same_pair()
    h.send(df)
    got = p.recv(10)
    assert isinstance(got, pd.DataFrame)   # pickle, container-exact
    assert got["i"].iloc[1] == 1
    p.destroy()
    h.destroy()


def test_pool_frame_result_mizl():
    # a homogeneous pool: a Frame result stages MIZL and the submitter
    # reads a region-backed Frame
    from tests.helpers import frame_unpickle

    pool = pymizu.Pool.create(2)
    h0, p0 = foreign_pair()
    h0.send(big_table().select(["i", "x"]))
    f = p0.recv(10)
    try:
        task = pool.submit(frame_unpickle, pickle.dumps(f))
        got = task.collect(30)
        assert type(got).__name__ == "Frame"
        d = got.to_dict()
        assert type(d["i"].base).__name__ == "_ShmView"
        assert d["i"][0] == -2**31 and d["i"][1] == 1
    finally:
        pool.destroy()
        p0.destroy()
        h0.destroy()


# Phase 3.8: the export-provenance REF


def provenance_setup():
    """host h1 (R peer word) stages a fixed-width frame; the test process
    imports the received region-backed Frame into polars and sends the
    polars frame on a second foreign channel."""
    pl = pytest.importorskip("polars")
    h1, p1 = foreign_pair(caps=CAPS_R)
    h1.send(big_table(with_nulls=False).select(["i", "x", "l"]))
    f = p1.recv(10)
    df = pl.DataFrame(f)
    h2, p2 = foreign_pair(caps=CAPS_R)
    return h1, p1, f, df, h2, p2


def test_provenance_ref_unmodified():
    # an unmodified round trip stages REF: the region's refcount gains the
    # relayed read's add, REFHELD is set, and no new region is written
    h1, p1, f, df, h2, p2 = provenance_setup()
    rc0 = f.to_dict()["i"].base.refcount
    flags0 = f.to_dict()["i"].base.flags
    h2.send(df)
    got = p2.recv(10)
    assert type(got).__name__ == "Frame"
    d = got.to_dict()
    assert d["i"][1:4].tolist() == [1, 2, 3]
    assert d["x"][1] == 0.5
    assert f.to_dict()["i"].base.refcount == rc0 + 1     # the REF's add
    assert f.to_dict()["i"].base.flags & 1 == 1          # REFHELD
    assert flags0 & 1 == 0
    h2.destroy()
    p2.destroy()
    p1.destroy()
    h1.destroy()


def test_provenance_ref_pyarrow_strings():
    # pyarrow hands back every exported buffer, string and factor columns
    # included, so its unmodified round trips REF too
    h1, p1 = foreign_pair(caps=CAPS_R)
    h1.send(big_table(with_nulls=False).select(["i", "s", "f"]))
    f = p1.recv(10)
    t = pa.table(f)
    rc0 = f.to_dict()["i"].base.refcount   # the export pins a loan
    h2, p2 = foreign_pair(caps=CAPS_R)
    h2.send(t)
    got = p2.recv(10)
    d = got.to_dict()
    assert d["s"][:2] == ["s0", "s1"]
    assert d["f"][1] == "v"
    assert f.to_dict()["i"].base.refcount == rc0 + 1
    h2.destroy()
    p2.destroy()
    p1.destroy()
    h1.destroy()


def test_provenance_modifications_copy():
    # every modification fails the record and takes the layout write: the
    # modified values arrive, the original region's refcount unmoved
    pl = pytest.importorskip("polars")
    n = 20000
    h1, p1, f, df, h2, p2 = provenance_setup()
    rc0 = f.to_dict()["i"].base.refcount
    cases = [
        # new nulls over the same values
        (df.with_columns(pl.when(pl.col("i") < 5)
                         .then(None).otherwise(pl.col("i")).name.keep()),
         lambda d: d["i"][0] == -2**31 and d["i"][5] == 5),
        # a new name
        (df.rename({"i": "j"}), lambda d: d["j"][1] == 1),
        # a shorter row selection
        (df.head(n - 1), lambda d: d["i"][1] == 1),
        # one computed column added
        (df.with_columns((pl.col("i") * 2).alias("j")),
         lambda d: d["j"][1] == 2),
        # a Date cast (a new type over the same bytes): a Date leaf, no REF
        (df.with_columns(pl.col("i").cast(pl.Date)),
         lambda d: str(d["i"].dtype) == "datetime64[D]"),
    ]
    for mod, check in cases:
        h2.send(mod)
        got = p2.recv(10)
        d = got.to_dict()
        assert len(got) == len(mod) and check(d)
        assert f.to_dict()["i"].base.refcount == rc0   # never the REF
        # CAPS_R has no MIZL_REF: a partial match writes every column —
        # no remote leaves, no REFHELD (the conditional conjunction)
        assert f.to_dict()["i"].base.flags & 1 == 0
    h2.destroy()
    p2.destroy()
    p1.destroy()
    h1.destroy()


# F2: the per-column REF (MIZL directory tag 33)


def test_provenance_remote_leaf_one_computed():
    # the payoff row: one computed column — the unmodified two cross as
    # remote leaves over the source region, the computed one a layout
    # leaf; pymizu's own frame path reads them back (tree_column)
    pl = pytest.importorskip("polars")
    h1, p1 = foreign_pair(caps=CAPS_R33)
    h1.send(big_table(with_nulls=False).select(["i", "x", "l"]))
    f = p1.recv(10)
    df = pl.DataFrame(f).with_columns((pl.col("x") * 2).alias("x"))
    src = f.to_dict()["i"].base   # a view over the source region
    rc0 = src.refcount
    h2, p2 = foreign_pair(caps=CAPS_R33)
    h2.send(df)
    got = p2.recv(10)
    # the two holds' counted loans, one per remote column, and REFHELD
    assert src.refcount == rc0 + 2
    assert src.flags & 1 == 1
    d = got.to_dict()
    # the remote columns: views whose refcount word is the source
    # region's own (the aliasing proof), values exact
    assert d["i"].base.refcount == src.refcount
    assert d["l"].base.refcount == src.refcount
    assert d["i"][1:4].tolist() == [1, 2, 3]
    assert d["l"][1] == 1
    # the computed column: the new values, laid out in the frame's region
    assert d["x"][1:4].tolist() == [1.0, 2.0, 3.0]
    del d, got
    gc.collect()
    assert src.refcount == rc0   # the holds and view loans released
    h2.destroy()
    p2.destroy()
    p1.destroy()
    h1.destroy()


def test_provenance_remote_leaf_two_exports():
    # columns matched from two acquisitions: one remote leaf each (no
    # whole-frame candidate), both regions' refcounts bump
    pl = pytest.importorskip("polars")
    h1, p1 = foreign_pair(caps=CAPS_R33)
    h1.send(big_table(with_nulls=False).select(["i", "x", "l"]))
    f1 = p1.recv(10)
    h1b, p1b = foreign_pair(caps=CAPS_R33)
    h1b.send(big_table(with_nulls=False).select(["i", "x", "l"]))
    f2 = p1b.recv(10)
    df1, df2 = pl.DataFrame(f1), pl.DataFrame(f2)
    mod = df1.with_columns(df2["l"])   # [f1.i, f1.x, f2.l]
    src1 = f1.to_dict()["i"].base
    src2 = f2.to_dict()["i"].base
    rc1, rc2 = src1.refcount, src2.refcount
    h2, p2 = foreign_pair(caps=CAPS_R33)
    h2.send(mod)
    got = p2.recv(10)
    assert src1.refcount == rc1 + 2    # columns i, x
    assert src2.refcount == rc2 + 1    # column l
    assert src1.flags & 1 == 1 and src2.flags & 1 == 1
    d = got.to_dict()
    assert d["i"].base.refcount == src1.refcount
    assert d["l"].base.refcount == src2.refcount
    assert d["i"][1:4].tolist() == [1, 2, 3]
    assert d["x"][1] == 0.5 and d["l"][1] == 1
    del d, got
    gc.collect()
    assert src1.refcount == rc1 and src2.refcount == rc2
    h2.destroy()
    p2.destroy()
    p1b.destroy()
    h1b.destroy()
    p1.destroy()
    h1.destroy()


def test_provenance_remote_leaf_all_remote_with_nulls():
    # every column a remote leaf, matched from two acquisitions, with
    # null-carrying referenced columns: the header tallies no remote
    # nulls (the vcount exclusion — a tallied one would check corrupt,
    # failing the receive), the table rows carry the claims, and the
    # receive is exact, nulls included
    pl = pytest.importorskip("polars")
    h1, p1 = foreign_pair(caps=CAPS_R33)
    h1.send(big_table().select(["i", "x", "l"]))   # nulls every 10th
    f1 = p1.recv(10)
    h1b, p1b = foreign_pair(caps=CAPS_R33)
    h1b.send(big_table().select(["i", "x", "l"]))
    f2 = p1b.recv(10)
    df1, df2 = pl.DataFrame(f1), pl.DataFrame(f2)
    mod = df1.with_columns(df2["x"])   # [f1.i, f2.x, f1.l]
    h2, p2 = foreign_pair(caps=CAPS_R33)
    h2.send(mod)
    got = p2.recv(10)
    d = got.to_dict()
    assert d["i"][0] == -2**31 and d["i"][1] == 1
    assert np.isnan(d["x"][0]) and d["x"][1] == 0.5
    assert d["l"][0] == -2**63 and d["l"][1] == 1
    h2.destroy()
    p2.destroy()
    p1b.destroy()
    h1b.destroy()
    p1.destroy()
    h1.destroy()


def test_provenance_remote_leaf_dictionary():
    # an unmodified dictionary column REFs whole — codes and levels ride
    # the referenced leaf; a recomputed one falls to the layout write.
    # pyarrow mutates: it hands the untouched column's buffers back
    h1, p1 = foreign_pair(caps=CAPS_R33)
    h1.send(big_table(with_nulls=False).select(["i", "f"]))
    f = p1.recv(10)
    t = pa.table(f)
    src = f.to_dict()["i"].base
    rc0 = src.refcount
    n = t.num_rows
    h2, p2 = foreign_pair(caps=CAPS_R33)
    mod = t.set_column(0, "i", pa.array([k * 2 for k in range(n)],
                                        type=pa.int32()))
    h2.send(mod)
    got = p2.recv(10)
    assert src.refcount == rc0 + 1             # only f is remote
    d = got.to_dict()
    assert d["f"][:3] == ["u", "v", "w"]       # the referenced factor leaf
    assert d["i"][1:4].tolist() == [2, 4, 6]   # the computed column
    # a recomputed dictionary: the layout write, no second remote leaf
    del d, got
    gc.collect()
    rc1 = src.refcount
    mod2 = t.set_column(1, "f", pa.array([f"s{k}" for k in range(n)]))
    h2.send(mod2)
    got = p2.recv(10)
    assert src.refcount == rc1 + 1             # i remote, f laid out
    d = got.to_dict()
    assert d["f"][:3] == ["s0", "s1", "s2"]
    assert d["i"][1:4].tolist() == [1, 2, 3]
    del d, got
    gc.collect()
    assert src.refcount == rc1
    h2.destroy()
    p2.destroy()
    p1.destroy()
    h1.destroy()


def test_provenance_polars_string_column_copies():
    # polars re-views strings (string_view), so a frame holding one never
    # matches the record: the MIZL write, values exact
    pl = pytest.importorskip("polars")
    h1, p1 = foreign_pair(caps=CAPS_R)
    h1.send(big_table(with_nulls=False).select(["i", "s"]))
    f = p1.recv(10)
    df = pl.DataFrame(f)
    rc0 = f.to_dict()["i"].base.refcount   # the export pins a loan
    h2, p2 = foreign_pair(caps=CAPS_R)
    h2.send(df)
    got = p2.recv(10)
    d = got.to_dict()
    assert d["s"][:2] == ["s0", "s1"]
    assert f.to_dict()["i"].base.refcount == rc0
    h2.destroy()
    p2.destroy()
    p1.destroy()
    h1.destroy()
