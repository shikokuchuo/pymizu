# The labeled shapes (MIZU_CAP_LABELED): DimArray propagation rules,
# stash validation, the wire emission and decline gates, and the REF
# gates' label compare — no R needed (the foreign channel is the
# in-process _ident override).

import pickle

import numpy as np
import pytest
from helpers import foreign_pair

import pymizu
from pymizu import _pymizu

LABELED = 1 << 7


def _dimarray(a):
    return a.view(pymizu.DimArray)


def test_labels_never_propagate():
    a = _dimarray(np.arange(6).reshape(2, 3).astype(np.float64))
    a.dimnames = (("r1", "r2"), ("c1", "c2", "c3"))
    a.axis_names = ("row", "col")
    a.r_class = "table"
    for built in (
        a + 1,
        a[::-1],
        a.T,
        a.copy(),
        a.reshape(3, 2),
        np.asarray(a),
    ):
        assert not isinstance(built, pymizu.DimArray) or (
            built.names is None
            and built.dimnames is None
            and built.axis_names is None
            and built.r_class is None
        )
    # a.T is the regression guard: the transpose view must not inherit
    assert a.T.dimnames is None
    # the pristine array keeps its labels
    assert a.dimnames[0][1] == "r2" and a.r_class == "table"


def test_labels_reattach_explicitly():
    a = _dimarray(np.arange(3).astype(np.int32))
    assert a.names is None
    a.names = ("x", None, "z")
    assert a.names == ("x", None, "z")
    a.names = None
    assert a.names is None


def test_pickled_dimarray_drops_labels():
    # numpy's subclass __reduce__ fires __array_finalize__ on the
    # rebuild: a pickled DimArray arrives unlabeled (the documented
    # same-language honest-loss rule)
    a = _dimarray(np.arange(3).astype(np.float64))
    a.names = ("a", "b", "c")
    b = pickle.loads(pickle.dumps(a))
    assert isinstance(b, pymizu.DimArray)
    assert b.names is None
    assert np.array_equal(np.asarray(b), np.asarray(a))


def _decline(arr, match):
    h, p = foreign_pair(LABELED)
    try:
        with pytest.raises(pymizu.DeclinedError, match=match):
            h.send(arr)
    finally:
        p.close()
        h.close()


def test_stash_validation():
    a = _dimarray(np.arange(3).astype(np.float64))
    a.names = ("a", "b")  # short
    _decline(a, "names")
    b = _dimarray(np.arange(3).astype(np.float64))
    b.names = ("a", "b", "c")
    b.dimnames = (("a", "b", "c"),)  # both slots
    _decline(b, "mutually exclusive")
    m = _dimarray(np.arange(6).reshape(2, 3).astype(np.float64))
    m.axis_names = ("row", "col")  # without dimnames
    _decline(m, "requires dimnames")
    m2 = _dimarray(np.arange(6).reshape(2, 3).astype(np.float64))
    m2.dimnames = (("r1", "r2"), ("c1", "c2"))  # short axis entry
    _decline(m2, "dimnames entry")
    m3 = _dimarray(np.arange(6).reshape(2, 3).astype(np.float64))
    m3.dimnames = (("r1", "r2"), ("c1", "c2", "c3"))
    m3.r_class = "not-a-class"
    _decline(m3, "r_class")
    m4 = _dimarray(np.arange(6).reshape(2, 3).astype(np.float64))
    m4.r_class = "table"  # without dimnames
    _decline(m4, "requires dimnames")


def test_labeled_emission_round_trips():
    h, p = foreign_pair(LABELED)
    try:
        # a named vector, NA in names
        a = _dimarray(np.array([1.5, 2.5, 3.5]))
        a.names = ("x", None, "z")
        h.send(a)
        got = p.recv(10)
        assert isinstance(got, pymizu.DimArray)
        assert got.names == ("x", None, "z")
        assert np.array_equal(np.asarray(got), np.asarray(a))
        # a matrix with dimnames (unnamed axes)
        m = _dimarray(np.arange(6).reshape(2, 3).astype(np.int32))
        m.dimnames = (("r1", "r2"), ("c1", "c2", "c3"))
        h.send(m)
        got = p.recv(10)
        assert isinstance(got, pymizu.DimArray)
        assert got.dimnames == (("r1", "r2"), ("c1", "c2", "c3"))
        assert got.axis_names is None and got.r_class is None
        assert np.array_equal(np.asarray(got), np.asarray(m))
        # named axes + the table class (the attr(list, {names}) form)
        t = _dimarray(np.array([[1, 2], [3, 4]], dtype=np.int32))
        t.dimnames = (("a", "b"), ("c", "d"))
        t.axis_names = ("", "")
        t.r_class = "table"
        h.send(t)
        got = p.recv(10)
        assert got.dimnames == (("a", "b"), ("c", "d"))
        assert got.axis_names == ("", "")
        assert got.r_class == "table"
        assert np.array_equal(np.asarray(got), np.asarray(t))
        # a 1-D table (dim the scalar form)
        t1 = _dimarray(np.array([3, 1], dtype=np.int32))
        t1.dimnames = (("x", "y"),)
        t1.axis_names = ("",)
        t1.r_class = "table"
        h.send(t1)
        got = p.recv(10)
        assert got.dimnames == (("x", "y"),)
        assert got.axis_names == ("",)
        assert got.r_class == "table"
        # a named integer64
        i64 = _dimarray(np.array([1, 2], dtype=np.int64))
        i64.names = ("a", "b")
        h.send(i64)
        got = p.recv(10)
        assert got.names == ("a", "b") and got.dtype == np.int64
        # a named 1-D strided view
        s = _dimarray(np.arange(8).astype(np.float64)[::2])
        s.names = ("a", "b", "c", "d")
        h.send(s)
        got = p.recv(10)
        assert got.names == ("a", "b", "c", "d")
        assert np.array_equal(np.asarray(got), np.asarray(s))
    finally:
        p.close()
        h.close()


def test_extent1_labels_round_trip():
    # extent-1 label positions take the strv tag, never the 0x04 scalar
    # collapse: a bare str entry has no home, and an NA one would read
    # as a nil axis (the corpus's table-1lev / named-vec-len1-na /
    # dimnames-na-extent1 cases pin the bytes)
    h, p = foreign_pair(LABELED)
    try:
        # extent-1 dimnames entry + extent-1 axis name (table("a")'s
        # shape)
        t = _dimarray(np.array([1], dtype=np.int32))
        t.dimnames = (("a",),)
        t.axis_names = ("",)
        t.r_class = "table"
        h.send(t)
        got = p.recv(10)
        assert got.dimnames == (("a",),)
        assert got.axis_names == ("",)
        assert got.r_class == "table"
        # an extent-1 NA entry is distinct from a nil axis
        e = _dimarray(np.array([1.0]))
        e.dimnames = ((None,),)
        h.send(e)
        got = p.recv(10)
        assert got.dimnames == ((None,),)
        # a length-1 NA name
        n = _dimarray(np.array([1.0]))
        n.names = (None,)
        h.send(n)
        got = p.recv(10)
        assert got.names == (None,)
    finally:
        p.close()
        h.close()


def test_labeled_caps_gate():
    # a caps-less peer: decline at send naming the capability, never a
    # strip (metadata is data) — the inline and layout tiers alike
    a = _dimarray(np.array([1.5, 2.5]))
    a.names = ("x", "y")
    for caps in (0, 2):  # no LABELED; ATTRS alone
        h, p = foreign_pair(caps)
        try:
            with pytest.raises(pymizu.DeclinedError, match="labeled"):
                h.send(a)
            big = _dimarray(np.arange(100000, dtype=np.float64))
            big.names = tuple(f"n{i}" for i in range(100000))
            with pytest.raises(pymizu.DeclinedError, match="labeled"):
                h.send(big)
        finally:
            p.close()
            h.close()


def _region_dimarray():
    """A region-backed labeled array (past the zc floor) off a foreign
    receive: (the array, the holding channel pair)."""
    h, p = foreign_pair(2 | LABELED)  # ATTRS + LABELED
    m = _dimarray(np.arange(20000, dtype=np.float64).reshape(200, 100))
    m.dimnames = (
        tuple(f"r{i}" for i in range(200)),
        tuple(f"c{i}" for i in range(100)),
    )
    m.axis_names = ("row", "col")
    h.send(m)
    got = p.recv(10)
    assert isinstance(got, pymizu.DimArray)
    assert got.dimnames == m.dimnames
    return got, (h, p)


def test_ref_probe_label_compare():
    got, (h, p) = _region_dimarray()
    try:
        # pristine: the labels equal the region blob's — the probe admits
        assert _pymizu._view_check(got)
        # a relabeled send: the probe rejects (crosses by value instead)
        got.dimnames = (got.dimnames[0][:-1] + ("changed",), got.dimnames[1])
        assert not _pymizu._view_check(got)
    finally:
        p.close()
        h.close()


def test_ref_probe_labelless_view_copies():
    got, (h, p) = _region_dimarray()
    try:
        # a plain array over the same bytes carries no labels: the
        # region's labels do not resurrect (honest loss)
        plain = np.asarray(memoryview(got))
        assert plain.base is not None
        assert not _pymizu._view_check(
            plain.view(np.float64).reshape(got.shape)
        )
    finally:
        p.close()
        h.close()


def test_ref_echo_pristine_region():
    got, (h, p) = _region_dimarray()
    try:
        # the pristine echo re-sends as the region name: zero bytes move
        # (the REF form stages, the peer receives the view again)
        assert _pymizu._view_check(got)
    finally:
        p.close()
        h.close()


def test_labeled_temporal_declines():
    # a named Date / POSIXct stays declined: no labeled temporal shape
    a = _dimarray(np.arange(3).astype("datetime64[D]"))
    a.names = ("x", "y", "z")
    _decline(a, "temporal")


def test_same_language_drops_labels():
    # the documented same-language honest loss: buffer/pickle tiers
    # carry the values, not the labels
    h, p = foreign_pair(0)  # reverse direction is same-language
    try:
        a = _dimarray(np.array([1.5, 2.5]))
        a.names = ("x", "y")
        p.send(a)
        got = h.recv(10)
        assert np.array_equal(np.asarray(got), np.asarray(a))
        assert getattr(got, "names", None) is None
    finally:
        p.close()
        h.close()
