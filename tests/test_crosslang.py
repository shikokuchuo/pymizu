"""Cross-language round-trips: real R and Python peer processes in both
directions, with real user programs crossing as MIZU_DROP_SOURCE drops.
Skipped unless Rscript and the installed mizu package (with the source-drop
path) are present — the skip_if_no_child_mizu() mirror."""

import os
import pathlib
import shutil
import subprocess
import sys

import pytest

import pymizu

RSCRIPT = shutil.which("Rscript")
REPO_PY = str(pathlib.Path(pymizu.__file__).resolve().parent.parent)

# The R echo peer: a real user program crossing as a source drop.
R_ECHO = """
repeat {
  x <- mizu::mizu_recv(ch, timeout = 30)
  if (inherits(x, "mizu_sentinel")) break
  mizu::mizu_send(ch, x)
}
"""


@pytest.fixture(scope="module")
def r_mizu():
    """The shipped R-peer launcher; its probe is the
    skip_if_no_child_mizu() mirror."""
    try:
        return pymizu.r_launcher()
    except pymizu.MizuError:
        pytest.skip("Rscript with the mizu package (source-drop support) "
                    "not available")


def test_r_launcher_missing_rscript():
    with pytest.raises(pymizu.MizuError):
        pymizu.r_launcher(rscript="/nonexistent/Rscript")


R_REF_RELAY = """
x <- runif(2e5)
mizu::mizu_send(ch, x)
y <- mizu::mizu_recv(ch, timeout = 30)
flags <- .Call(mizu:::mizu_zc_refcount, y)[[2L]]
mizu::mizu_send(ch, c(
  .Call(mizu:::mizu_zc_view_check, y),
  flags %% 2L == 1L,
  identical(y, x)
))
"""


def test_r_peer_view_relayed_by_reference(r_mizu):
    # R -> Python -> R: the return hop is a REF naming R's own region, so
    # R gets a view of it (REFHELD) that is identical() to what it sent
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_REF_RELAY, launcher=r_mizu)
    try:
        v = ch.recv(30)
        assert ch.send(v) is True
        assert np.asarray(ch.recv(30)).tolist() == [1, 1, 1]
    finally:
        ch.close()


def test_r_peer_echo_roundtrip(r_mizu):
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        for x in [
            np.array([1.5, 2.5, 3.5]),                    # float64 <-> REALSXP
            np.arange(10, dtype=np.int32),                # int32 <-> INTSXP
            np.arange(256, dtype=np.uint8),               # uint8 <-> RAWSXP
        ]:
            assert ch.send(x) is True
            got = ch.recv(30)
            assert np.array_equal(np.asarray(got), x)
        # bytes stage raw and cross back as a uint8 array (RAWSXP)
        assert ch.send(b"\x00\x01 bare bytes") is True
        assert ch.recv(30).tobytes() == b"\x00\x01 bare bytes"
    finally:
        ch.close()


def test_r_peer_zero_copy_view(r_mizu):
    np = pytest.importorskip("numpy")
    src = """
x <- as.numeric(seq_len(2000000)) + 0   # materialize: an ALTREP sequence
i <- seq_len(2000000) + 0L              # would serialize, never SHM_VEC
mizu::mizu_send(ch, x)
mizu::mizu_send(ch, i)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        v = ch.recv(30)
        assert isinstance(v, np.ndarray)
        assert v.dtype == np.dtype("float64")
        assert len(v) == 2000000
        assert v[0] == 1.0 and v[-1] == 2000000.0
        # the zero-copy proof: a read-only array over the owner's mapping
        assert not v.flags.writeable
        assert type(v.base).__name__ == "_ShmView"
        with pytest.raises(ValueError):
            v[0] = 0.0
        i = ch.recv(30)
        assert i.dtype == np.dtype("int32")
        assert i[0] == 1 and i[-1] == 2000000
    finally:
        ch.close()
    # the view outlives its read (and the channel): the owner pins the
    # mapping, and a slice keeps the owner alive through its base chain
    s = v[::2]
    del v
    assert s[0] == 1.0 and s[-1] == 1999999.0


def test_r_peer_int64_roundtrip(r_mizu):
    """int64 is a native wire type: numpy int64 crosses bit-identically,
    landing in R as an integer64 vector (bit64's layout) — no conversion."""
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        for x in [
            np.array([0, 1, -1, 2**53 + 1, -(2**53) - 1], dtype=np.int64),
            np.arange(2000000, dtype=np.int64) * 2**32,  # 16 MB: SHM_VEC
        ]:
            assert ch.send(x) is True
            got = ch.recv(30)
            assert isinstance(got, np.ndarray)
            assert got.dtype == np.int64
            assert np.array_equal(got, x)
    finally:
        ch.close()


def test_r_peer_int64_na_sentinel(r_mizu):
    """INT64_MIN is NA_integer64_ (the documented sentinel) both ways. The
    R side asserts the wire bits, so the check needs no bit64 install; when
    bit64 is loadable it also confirms the is.na() semantics."""
    np = pytest.importorskip("numpy")
    src = r"""
i64le <- function(v) writeBin(unclass(v), raw(), size = 8L, endian = "little")
fromle <- function(r) readBin(r, "double", size = 8L, endian = "little",
                              n = length(r) %/% 8L)
x <- mizu::mizu_recv(ch, timeout = 30)
ok <- inherits(x, "integer64") && length(x) == 2L &&
  identical(i64le(x), as.raw(c(0, 0, 0, 0, 0, 0, 0, 0x80,
                              7, 0, 0, 0, 0, 0, 0, 0)))
if (requireNamespace("bit64", quietly = TRUE)) {
  ok <- ok && is.na(x[1L]) && !is.na(x[2L])
}
# R -> Python: an NA_integer64_ (INT64_MIN bits) and a value past 2^53
v <- structure(fromle(as.raw(c(0, 0, 0, 0, 0, 0, 0, 0x80,
                               1, 0, 0, 0, 0, 0, 0x20, 0))),
               class = "integer64")
mizu::mizu_send(ch, if (isTRUE(ok)) v else "R check failed")
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        assert ch.send(np.array([-(2**63), 7], dtype=np.int64)) is True
        got = ch.recv(30)
        assert isinstance(got, np.ndarray) and got.dtype == np.int64
        assert list(got) == [-(2**63), 2**53 + 1]
    finally:
        ch.close()


def test_r_peer_string_and_na(r_mizu):
    src = """
mizu::mizu_send(ch, "hello world")
mizu::mizu_send(ch, NA_character_)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        assert ch.recv(30) == "hello world"
        assert ch.recv(30) is None
    finally:
        ch.close()


def test_r_peer_set_declined_at_send(r_mizu):
    # a set has no portable home: the interchange walk raises DeclinedError
    # at send time (the peer is never sent a stream it cannot read)
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        with pytest.raises(pymizu.DeclinedError, match="set"):
            ch.send({1, 2, 3})
        # the channel is unharmed: a portable value still crosses
        assert ch.send("after") is True
        assert ch.recv(30) == "after"
    finally:
        ch.close()


def test_r_peer_masked_array_declined(r_mizu):
    """A MaskedArray is a buffer subclass (the raw tier would drop the
    mask): DeclinedError at send on a foreign channel, and the channel is
    unharmed for a portable value after it."""
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        m = np.ma.MaskedArray([1.0, 2.0], mask=[True, False])
        with pytest.raises(pymizu.DeclinedError, match="buffer subclass"):
            ch.send(m)
        a = np.array([1.5, 2.5])
        assert ch.send(a) is True
        assert np.array_equal(np.asarray(ch.recv(30)), a)
    finally:
        ch.close()


def test_r_peer_environment_declined_on_r_side(r_mizu):
    """An environment has no portable home either: the R interchange
    writer raises at send (mizu_error_not_portable), and the R side
    reports it — the private R serialize stream never crosses."""
    src = """
e <- tryCatch(mizu::mizu_send(ch, new.env()),
              error = function(e) conditionMessage(e))
mizu::mizu_send(ch, e)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        got = ch.recv(30)
        assert "not portable" in got and "environment" in got
    finally:
        ch.close()


def test_r_peer_batch_of_interop_lists(r_mizu):
    """A send_batch of R lists: every element crosses as an 'I' stream,
    in order — the foreign-payload decline of the private-codec era is
    gone from the interchange path."""
    src = """
mizu::mizu_send_batch(ch, list("a", list(1, 2)))
mizu::mizu_send(ch, "after")
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        assert ch.recv_batch(4, timeout=30) == ["a", [1.0, 2.0], "after"]
    finally:
        ch.close()


def test_r_host_python_peer(r_mizu):
    """The other direction: an R host, a Python peer spawned through
    python -m pymizu.child, assertions on the R side."""
    script = pathlib.Path(__file__).parent / "r_host_roundtrip.R"
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_PY + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [RSCRIPT, str(script), sys.executable],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    assert "OK" in proc.stdout
