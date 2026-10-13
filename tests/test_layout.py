"""The layout leaf emitters' byte pins (no peer): an MIZH body equals
the 'I' writer's vector body for the same value, an attrs blob equals
the 'I' writer's dict portion, an MIZS block carries the string bytes —
value-exactness by construction, pinned here."""

import pathlib
import subprocess
import sys
import textwrap

import numpy as np

import pymizu
from pymizu import _pymizu

leaf = _pymizu._layout_leaf
write = _pymizu._write_stream

CAPS_R = 31  # R's mask: MIZS | ATTRS | MIZL | TASKREF | MIZL_REF


def foreign_pair(caps=CAPS_R):
    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    p, _ = _pymizu._channel_attach(h.token, _ident=(2, caps))
    p.ready_set()
    assert h.ready_wait(10)
    return h, p


def stream_body_blob(s, body_bytes):
    # an attr-carrying 'I' stream: 2 header + 1 attr tag + 9 vec header,
    # then the body, then the attrs dict; a leaf blob carries its own
    # 2-byte stream header
    return s[12 : 12 + body_bytes], s[:2] + s[12 + body_bytes :]


def test_mizs_leaf_block():
    body, blob = leaf(["a", "bc"])
    assert blob == b""
    # the geometry tail is the packed UTF-8 bytes
    assert body.endswith(b"abc")
    _, blob = leaf([None, "x"])
    assert blob == b""
    body2, _ = leaf([None, "x"])
    # a None clears its validity bit; the str bytes are "x"
    assert body2.endswith(b"x")
    assert len(body2) % 64 == 0 or len(body2) > 0


def test_mizh_leaf_identity_1d():
    x = np.arange(6, dtype=np.int32)
    body, blob = leaf(x)
    assert body == x.tobytes()
    assert blob == b""
    assert leaf(x)[0] == write(x)[11:]  # 2 header + 9 vec header


def test_mizh_leaf_dim_c_order():
    m = np.arange(6, dtype=np.int32).reshape(2, 3)
    body, blob = leaf(m)
    assert body == m.tobytes(order="F")
    want_body, want_blob = stream_body_blob(write(m), 24)
    assert body == want_body
    assert blob == want_blob


def test_mizh_leaf_dim_f_order():
    m = np.asfortranarray(np.arange(6, dtype=np.float64).reshape(2, 3))
    body, blob = leaf(m)
    assert body == m.tobytes(order="F")
    _, want_blob = stream_body_blob(write(m), 48)
    assert blob == want_blob


def test_mizh_leaf_dim_strided():
    m = np.arange(12, dtype=np.int32).reshape(3, 4)[:, ::-1]
    body, blob = leaf(m)
    assert body == m.tobytes(order="F")
    _, want_blob = stream_body_blob(write(m), 48)
    assert blob == want_blob


def test_mizh_leaf_dim_3d():
    m = np.arange(24, dtype=np.float64).reshape(2, 3, 4)
    body, blob = leaf(m)
    assert body == m.tobytes(order="F")
    want_body, want_blob = stream_body_blob(write(m), 192)
    assert body == want_body
    assert blob == want_blob


def test_mizh_leaf_int64_dim():
    m = np.arange(6, dtype=np.int64).reshape(2, 3)
    body, blob = leaf(m)
    assert body == m.tobytes(order="F")
    _, want_blob = stream_body_blob(write(m), 48)
    assert blob == want_blob


def test_mizh_leaf_date():
    d = np.array(["2022-03-21", "2022-03-22"], dtype="datetime64[D]")
    body, blob = leaf(d)
    want_body, want_blob = stream_body_blob(write(d), 16)
    assert body == want_body
    assert blob == want_blob


def test_mizh_leaf_posixct_nat():
    p = np.array([1700000000, "NaT"], dtype="datetime64[s]")
    body, blob = leaf(p)
    want_body, want_blob = stream_body_blob(write(p), 16)
    assert body == want_body
    assert blob == want_blob
    # NaT lands as the NA_real_ payload
    assert body[8:] == bytes.fromhex("a20700000000f07f")


def test_mizh_leaf_difftime():
    t = np.array([90, 135], dtype="timedelta64[s]")
    body, blob = leaf(t)
    want_body, want_blob = stream_body_blob(write(t), 16)
    assert body == want_body
    assert blob == want_blob


def test_mizh_leaf_declines():
    # a conversion dtype (uint64), a non-str list, a calendar-unit
    # temporal, a 0-d buffer
    assert leaf(np.array([1, 2], dtype=np.uint64)) is None
    assert leaf([1, 2]) is None
    assert leaf(np.array(["2022"], dtype="datetime64[Y]")) is None
    assert leaf(np.array(5, dtype=np.int32)) is None


def test_mizh_leaf_temporal_scalars():
    # numpy temporal scalars stage as 1-element leaves
    body, blob = leaf(np.datetime64("2022-03-21", "D"))
    assert len(body) == 8
    body, blob = leaf(np.timedelta64(90, "s"))
    assert len(body) == 8


def big_ints(n=20000):
    return list(range(n))


def test_mizl_tree_roundtrip():
    # a nested tree crosses as one MIZL region: string leaves wrap as
    # views, scalars read as the 'I' codec's values
    h, p = foreign_pair()
    try:
        x = {"a": big_ints(), "b": ["x", "y", None, ""], "c": {"d": 1.5}}
        assert h.send(x) is True
        got = p.recv(5)
        assert type(got) is dict
        assert [v.item() for v in got["a"][:3]] == [0, 1, 2]
        assert got["b"].to_list() == ["x", "y", None, ""]
        assert got["c"]["d"].item() == 1.5
    finally:
        p.destroy()
        h.destroy()


def test_mizl_tree_scalar_wire_types():
    # int32-range ints land INT, wider INT64; -2^63 keeps its int64 value
    # here (R reads the sentinel as NA_integer64_ — the crosslang pin)
    h, p = foreign_pair()
    try:
        assert h.send([2**40, -(2**31), 5, -(2**63)] + big_ints()) is True
        got = p.recv(5)
        assert got[0].item() == 2**40
        assert got[1].item() == -(2**31)
        assert got[2].item() == 5
        assert got[3].item() == -(2**63)
    finally:
        p.destroy()
        h.destroy()


def test_mizl_tree_buffer_and_temporal_leaves():
    h, p = foreign_pair()
    try:
        m = np.arange(200000, dtype=np.float64).reshape(400, 500)
        d = np.arange(
            np.datetime64(1700000000, "s"), np.datetime64(1700100000, "s")
        )
        assert h.send([m, d, "tail"]) is True
        got = p.recv(5)
        assert np.array_equal(np.asarray(got[0]), m)
        assert np.asarray(got[0]).shape == (400, 500)
        assert np.asarray(got[1])[0] == np.datetime64(1700000000, "s")
        assert got[2].to_list() == ["tail"]
    finally:
        p.destroy()
        h.destroy()


def test_mizl_tree_vetoes():
    h, p = foreign_pair()
    try:
        # a None outside a string list vetoes: the whole tree takes 'I'
        assert h.send([True, None] + big_ints()) is True
        got = p.recv(5)
        assert got[:2] == [True, None]
        # a conversion dtype vetoes (conversion stays 'I'-only)
        assert h.send([np.arange(200000, dtype=np.uint64)]) is True
        got = p.recv(5)
        assert isinstance(got[0], np.ndarray)
        # a tree-borrowed leaf view (a local leaf's element, loan != NULL)
        # never names its region: the tree stages, the leaf by value
        assert h.send({"k": big_ints()}) is True
        tree = p.recv(5)
        borrowed = tree["k"]
        assert not _pymizu._view_check(borrowed)
        assert h.send([borrowed, 1] + big_ints()) is True
        got = p.recv(5)
        assert not _pymizu._view_check(got[0])
        assert np.array_equal(np.asarray(got[0]), np.asarray(borrowed))
        # a past-int64 int has no home in either tier: DeclinedError
        import pytest

        with pytest.raises(pymizu.DeclinedError, match="int64"):
            h.send([2**90] + big_ints())
        # a non-str dict key has no home in either tier
        with pytest.raises(pymizu.DeclinedError, match="non-str dict key"):
            h.send({1: big_ints()})
        # a lone-surrogate str has no home in either tier
        with pytest.raises(pymizu.DeclinedError, match="UTF-8"):
            h.send(["\ud800"] + big_ints())
    finally:
        p.destroy()
        h.destroy()


def test_mizl_tree_remote_leaves():
    # a re-sendable view nested in a tree crosses as a remote leaf
    # (directory tag 33): REFHELD set, the resolve one counted add
    h, p = foreign_pair()
    try:
        assert h.send(np.arange(200000, dtype=np.float64)) is True
        view = p.recv(5)
        assert _pymizu._view_check(view)
        assert h.send([view, 1] + big_ints()) is True
        rc = view.base.refcount
        got = p.recv(5)
        assert _pymizu._view_check(got[0])
        assert got[0].base.flags & 1  # REFHELD
        assert view.base.refcount == rc + 1
        assert np.array_equal(np.asarray(got[0]), np.asarray(view))
        # a string view remote leaf
        assert h.send(["x", "y"] * 10000) is True
        sview = p.recv(5)
        assert h.send({"s": sview, "v": big_ints()}) is True
        got = p.recv(5)
        assert _pymizu._view_check(got["s"])
        assert got["s"].flags & 1
        assert got["s"].to_list()[:4] == ["x", "y", "x", "y"]
        # nested a level down (the child node's own validity tail)
        assert h.send({"a": {"b": view}, "c": big_ints()}) is True
        got = p.recv(5)
        assert _pymizu._view_check(got["a"]["b"])
        assert np.array_equal(np.asarray(got["a"]["b"]), np.asarray(view))
        # two leaves referencing one region: one counted add each (the
        # old tree's views released ahead of the baseline)
        del got
        assert h.send([view, view, 1] + big_ints()) is True
        rc = view.base.refcount
        got = p.recv(5)
        assert _pymizu._view_check(got[0]) and _pymizu._view_check(got[1])
        assert view.base.refcount == rc + 2
        # a resolved remote leaf is itself re-sendable (a chained send)
        el = got[0]
        del got
        assert h.send([el, 1] + big_ints()) is True
        rc = view.base.refcount
        got = p.recv(5)
        assert _pymizu._view_check(got[0])
        assert view.base.refcount == rc + 1
    finally:
        p.destroy()
        h.destroy()


def test_mizl_tree_remote_leaf_caps_decline():
    # a peer short of MIZU_CAP_MIZL_REF gets the 'I' by-value copy
    h, p = foreign_pair(caps=15)  # all but MIZL_REF (bit 4)
    try:
        assert h.send(np.arange(200000, dtype=np.float64)) is True
        view = p.recv(5)
        assert _pymizu._view_check(view)
        assert h.send([view, 1] + big_ints()) is True
        got = p.recv(5)
        assert not _pymizu._view_check(got[0])
        assert np.array_equal(np.asarray(got[0]), np.asarray(view))
    finally:
        p.destroy()
        h.destroy()


def test_mizl_tree_caps_decline():
    # a peer short of MIZL takes the 'I' copy (a short-cap strlist too)
    h, p = foreign_pair(caps=3)  # MIZS | ATTRS, no MIZL
    try:
        assert h.send(big_ints()) is True
        got = p.recv(5)
        assert type(got) is list and got[:3] == [0, 1, 2]
    finally:
        p.destroy()
        h.destroy()
    h, p = foreign_pair(caps=6)  # ATTRS | MIZL, no MIZS
    try:
        assert h.send(["x"] * 20000) is True
        assert p.recv(5) == ["x"] * 20000
    finally:
        p.destroy()
        h.destroy()


def test_mizl_tree_below_floor_copies():
    h, p = foreign_pair()
    try:
        assert h.send({"k": [1, 2, 3]}) is True
        got = p.recv(5)
        assert got == {"k": [1, 2, 3]}
        assert not _pymizu._view_check(got["k"][0])
    finally:
        p.destroy()
        h.destroy()


def test_mizl_tree_depth_cap():
    deep = []
    for _ in range(65):
        deep = [deep]
    h, p = foreign_pair()
    try:
        # past the cap both tiers decline: the send raises DeclinedError
        import pytest

        with pytest.raises(pymizu.DeclinedError, match="depth"):
            h.send(deep)
    finally:
        p.destroy()
        h.destroy()


def test_np_type_probe_reprobes_after_numpy_import(tmp_path):
    # a stage that ran the buffer-type probes before numpy was importable
    # (an early scalar send) must not cache "no numpy" forever — the
    # reader's first vector imports numpy, and a datetime64 array must
    # still find its temporal home
    res = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import sys

                from pymizu import _pymizu

                assert "numpy" not in sys.modules
                h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
                p, _ = _pymizu._channel_attach(h.token, _ident=(2, 0))
                p.ready_set()
                assert h.ready_wait(10)
                assert h.send(True) is True   # the probes run pre-numpy
                assert p.recv(5) is True
                import numpy as np

                d = np.array(["2022-03-21"], dtype="datetime64[D]")
                assert h.send(d) is True
                got = p.recv(5)
                assert np.asarray(got) == d
                p.destroy()
                h.destroy()
                """
            ),
        ],
        cwd=pathlib.Path(__file__).parent.parent,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr
