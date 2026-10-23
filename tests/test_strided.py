# Nested strided arrays cross: the stale depth guard's regression file.
# Every R matrix arrives in Python as an F-order view, so an R list
# holding a matrix must echo whole — the inline writer gathers strided
# leaves F-order at any depth, exactly as the top-level and layout tiers
# always did. No R needed (the foreign channel is the in-process _ident
# override).

import numpy as np
import pytest
from helpers import foreign_pair

import pymizu
from pymizu import _pymizu

LABELED = 1 << 7


def _dimarray(a):
    return a.view(pymizu.DimArray)


def test_nested_f_order_matrix_crosses():
    a = np.arange(6, dtype=np.float64).reshape(2, 3, order="F")
    assert not a.flags.c_contiguous
    h, p = foreign_pair()
    try:
        assert h.send({"m": a}) is None  # was DeclinedError
        got = p.recv(10)
        assert np.array_equal(got["m"], a)
        # the gather is F-order: the same bytes as the C-order twin
        twin = np.array([[0.0, 2.0, 4.0], [1.0, 3.0, 5.0]])
        assert _pymizu._write_stream({"m": a}) == _pymizu._write_stream(
            {"m": twin}
        )
    finally:
        p.close()
        h.close()


def test_nested_negative_stride_1d():
    a = np.arange(6, dtype=np.float64)[::-1]
    h, p = foreign_pair()
    try:
        assert h.send([a]) is None
        got = p.recv(10)
        assert np.array_equal(got[0], [5.0, 4.0, 3.0, 2.0, 1.0, 0.0])
    finally:
        p.close()
        h.close()


def test_nested_strided_slice_2d():
    s = np.arange(36, dtype=np.int32).reshape(6, 6)[::2, ::2]
    h, p = foreign_pair()
    try:
        assert h.send({"s": s}) is None
        got = p.recv(10)
        assert np.array_equal(got["s"], s)
    finally:
        p.close()
        h.close()


def test_nested_named_strided_vec():
    # the ixe_named_vec nonzero-stride branch
    s = _dimarray(np.arange(4, dtype=np.float64)[::-1])
    s.names = ("a", "b", "c", "d")
    h, p = foreign_pair(LABELED)
    try:
        assert h.send({"v": s}) is None
        got = p.recv(10)
        assert got["v"].names == ("a", "b", "c", "d")
        assert np.array_equal(np.asarray(got["v"]), [3.0, 2.0, 1.0, 0.0])
    finally:
        p.close()
        h.close()


def test_received_dim_view_echoes_nested():
    # the ability.cov shape: received F-order views (a dimnames matrix
    # and a named vector) nested in the received dict, echoed whole
    m = _dimarray(np.arange(6, dtype=np.float64).reshape(2, 3))
    m.dimnames = (("r1", "r2"), ("c1", "c2", "c3"))
    v = _dimarray(np.array([1.5, 2.5]))
    v.names = ("x", "y")
    h, p = foreign_pair(LABELED)
    try:
        assert h.send({"cov": m, "center": v}) is None
        received = p.recv(10)
        assert not received["cov"].flags.c_contiguous  # the F-order view
        assert h.send(received) is None  # was DeclinedError at x['cov']
        got = p.recv(10)
        assert got["cov"].dimnames == (("r1", "r2"), ("c1", "c2", "c3"))
        assert np.array_equal(np.asarray(got["cov"]), np.asarray(m))
        assert got["center"].names == ("x", "y")
        assert np.array_equal(np.asarray(got["center"]), [1.5, 2.5])
    finally:
        p.close()
        h.close()


def test_strided_leaf_tier_parity():
    # the same strided leaf in a small container (the inline writer) and
    # a big one (the MIZL layout tier — R's caps mask has it engage)
    # reads back identical values
    leaf = np.arange(12, dtype=np.int32).reshape(3, 4)[:, ::-1]
    h, p = foreign_pair(caps=31)
    try:
        assert h.send({"k": leaf, "pad": 0}) is None
        small = p.recv(10)
        assert h.send({"k": leaf, "pad": list(range(20000))}) is None
        big = p.recv(10)
        assert [v.item() for v in big["pad"][:3]] == [0, 1, 2]  # MIZL
        assert np.array_equal(np.asarray(big["k"]), leaf)
        assert np.array_equal(np.asarray(big["k"]), small["k"])
    finally:
        p.close()
        h.close()


def test_suboffsets_still_declines():
    tb = pytest.importorskip("_testbuffer")
    b = tb.ndarray(list(range(6)), (2, 3), format="i", flags=tb.ND_PIL)
    assert memoryview(b).suboffsets
    h, p = foreign_pair()
    try:
        with pytest.raises(pymizu.DeclinedError, match="suboffsets"):
            h.send({"m": b})
        with pytest.raises(pymizu.DeclinedError, match="suboffsets"):
            h.send(b)
    finally:
        p.close()
        h.close()
