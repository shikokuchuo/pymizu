"""The layout leaf emitters' byte pins (no peer): an MIZH body equals
the 'I' writer's vector body for the same value, an attrs blob equals
the 'I' writer's dict portion, an MIZS block carries the string bytes —
value-exactness by construction, pinned here."""

import pathlib
import subprocess
import sys
import textwrap

import numpy as np

from pymizu import _pymizu

leaf = _pymizu._layout_leaf
write = _pymizu._write_stream


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
