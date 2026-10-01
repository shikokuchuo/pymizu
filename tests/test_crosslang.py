"""Cross-language round-trips: real R and Python peer processes in both
directions, with real user programs crossing as MIZU_DROP_SOURCE drops.
Skipped unless Rscript and the installed mizu package (with the source-drop
path) are present — the skip_if_no_child_mizu() mirror."""

import gc
import os
import pathlib
import shutil
import subprocess
import sys
import warnings

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


R_STR_VIEW = """
x <- rep(c("hello", NA_character_, "", "héllo ✓", strrep("long ", 2000)),
         length.out = 20000)
mizu::mizu_send(ch, x)
mizu::mizu_recv(ch, timeout = 60)   # stay alive while the consumer reads
"""


def test_r_peer_string_vector_view(r_mizu):
    # an R character vector past the floor crosses as a region-backed
    # string view (MIZS): to_list() is the explicit copy
    ch = pymizu.Channel.create(R_STR_VIEW, launcher=r_mizu)
    try:
        v = ch.recv(30)
        assert type(v).__name__ == "_ShmStrView"
        want = ["hello", None, "", "héllo ✓", "long " * 2000]
        got = v.to_list()
        assert len(got) == 20000
        assert got[:5] == want and got[5:10] == want
    finally:
        ch.close()


def test_r_peer_string_view_arrow(r_mizu):
    # the string block is Arrow large_utf8-shaped: the export hands the
    # three buffers (validity bitmap, i64 offsets, packed bytes) in place
    pa = pytest.importorskip("pyarrow")
    ch = pymizu.Channel.create(R_STR_VIEW, launcher=r_mizu)
    try:
        v = ch.recv(30)
        arr = pa.array(v)
        assert arr.type == pa.large_string()
        assert arr.null_count == 4000
        want = ["hello", None, "", "héllo ✓", "long " * 2000]
        assert arr.slice(0, 10).to_pylist() == want + want
    finally:
        ch.close()


def test_r_peer_string_view_polars(r_mizu):
    pl = pytest.importorskip("polars")
    ch = pymizu.Channel.create(R_STR_VIEW, launcher=r_mizu)
    try:
        s = pl.Series(ch.recv(30))
        assert s.dtype == pl.String and len(s) == 20000
        assert s.null_count() == 4000
        assert s.head(6).to_list() == ["hello", None, "", "héllo ✓",
                                       "long " * 2000, "hello"]
    finally:
        ch.close()


R_STR_RELAY = """
x <- rep(c("alpha", NA_character_, "beta", strrep("s", 5000)),
         length.out = 3000)
mizu::mizu_send(ch, x)
y <- mizu::mizu_recv(ch, timeout = 30)
flags <- .Call(mizu:::mizu_zc_refcount, y)[[2L]]
mizu::mizu_send(ch, c(
  .Call(mizu:::mizu_zc_view_check, y),
  flags %% 2L == 1L,
  identical(y, x)
))
"""


def test_r_peer_string_view_relayed_by_reference(r_mizu):
    # R -> Python -> R: the return hop is a REF naming R's own region, so
    # R gets a view of it (REFHELD) that is identical() to what it sent
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_STR_RELAY, launcher=r_mizu)
    try:
        v = ch.recv(30)
        assert type(v).__name__ == "_ShmStrView"
        assert ch.send(v) is True
        assert np.asarray(ch.recv(30)).tolist() == [1, 1, 1]
    finally:
        ch.close()


PY_ECHO = """
while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x)
"""


def test_py_peer_string_view_echo(r_mizu):
    # Python -> Python: a received string view re-sent crosses as REF; the
    # echo peer's read resolves it to the same region's string view
    r_ch = pymizu.Channel.create(R_STR_VIEW, launcher=r_mizu)
    py_ch = pymizu.Channel.create(
        "import pymizu\n" + PY_ECHO,
    )
    try:
        v = r_ch.recv(30)
        assert type(v).__name__ == "_ShmStrView"
        assert py_ch.send(v) is True
        back = py_ch.recv(30)
        assert type(back).__name__ == "_ShmStrView"
        assert back.to_list()[:5] == ["hello", None, "", "héllo ✓",
                                      "long " * 2000]
    finally:
        py_ch.close()
        r_ch.close()


def test_r_peer_copied_read_na_rules(r_mizu):
    # the copied-read rule (3.2): a foreign INT with NAs reads float64
    # with NA_real_-payload NaNs on the raw tiers; LGL reads bool_ when
    # NA-free, int32 with the sentinel otherwise; the zero-copy view stays
    # int32 on the raw page buffer
    np = pytest.importorskip("numpy")
    src = """
mizu::mizu_send(ch, c(1L, NA_integer_, 3L))                    # RAWVEC
x <- rep(1:100, length.out = 30000); x[5] <- NA_integer_       # arena
mizu::mizu_send(ch, x)
v <- rep(1:100, length.out = 1e6); v[7] <- NA_integer_         # the view
mizu::mizu_send(ch, v)
mizu::mizu_send(ch, c(TRUE, FALSE, TRUE))
mizu::mizu_send(ch, c(TRUE, NA, FALSE))
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        a = ch.recv(30)
        assert a.dtype == np.float64
        assert a[0] == 1 and np.isnan(a[1]) and a[2] == 3
        e = ch.recv(30)
        assert e.dtype == np.float64
        assert np.where(np.isnan(e))[0].tolist() == [4]
        f = ch.recv(30)
        assert f.dtype == np.int32 and not f.flags.writeable
        assert np.where(f == -2**31)[0].tolist() == [6]
        b = ch.recv(30)
        assert b.dtype == np.bool_ and list(b) == [True, False, True]
        c = ch.recv(30)
        assert c.dtype == np.int32 and list(c) == [1, -2**31, 0]
    finally:
        ch.close()


def test_r_peer_view_arrow_na_rules(r_mizu):
    # every Arrow export of LGL is Arrow bool with a validity bitmap
    # (the bit-pack built at export); INT exports int32 with a validity
    # bitmap, built lazily off the sentinels on a pre-section region
    pa = pytest.importorskip("pyarrow")
    # the acks pace the sends: each stage recycles the region the consumer
    # just released, so the Linux churn fallback never engages
    src = """
send <- function(x) {
  mizu::mizu_send(ch, x)
  mizu::mizu_recv(ch, timeout = 30)
}
l <- rep(c(TRUE, FALSE, NA), length.out = 1e6)
send(l)
cl <- rep(c(TRUE, FALSE), length.out = 1e6)
send(cl)
i <- rep(1:100, length.out = 1e6); i[3] <- NA_integer_
send(i)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        la = pa.array(ch.recv(30).base)
        assert la.type == pa.bool_() and la.null_count == 333333
        assert la.slice(0, 4).to_pylist() == [True, False, None, True]
        del la  # drop the export's loan before the ack
        ch.send(b"")
        ca = pa.array(ch.recv(30).base)
        assert ca.type == pa.bool_() and ca.null_count == 0
        assert ca.slice(0, 3).to_pylist() == [True, False, True]
        del ca
        ch.send(b"")
        ia = pa.array(ch.recv(30).base)
        assert ia.type == pa.int32() and ia.null_count == 1
        assert ia.slice(0, 4).to_pylist() == [1, 2, None, 4]
        del ia
        ch.send(b"")
    finally:
        ch.close()


def test_r_peer_view_to_numpy(r_mizu):
    # _ShmView.to_numpy() (3.4): the copied-read rule over views, with
    # NA-freeness read off the region's validity section before any scan
    # ({0, 0} pre-section regions here — the fallback scan, cached)
    np = pytest.importorskip("numpy")
    # the acks pace the sends: each stage recycles the region the consumer
    # just released, so the Linux churn fallback never engages
    src = """
send <- function(x) {
  mizu::mizu_send(ch, x)
  mizu::mizu_recv(ch, timeout = 30)
}
l <- rep(c(TRUE, FALSE, NA), length.out = 1e6)
send(l)
cl <- rep(c(TRUE, FALSE), length.out = 1e6)
send(cl)
i <- rep(1:100, length.out = 1e6); i[3] <- NA_integer_
send(i)
ci <- rep(1:100, length.out = 1e6)
send(ci)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        a = ch.recv(30).base.to_numpy()          # LGL with NAs: the view
        assert a.dtype == np.int32 and not a.flags.writeable
        assert list(a[:3]) == [1, 0, -2**31]
        del a                     # the view-backed copy pins the region
        ch.send(b"")
        b = ch.recv(30).base.to_numpy()          # clean LGL: a bool_ copy
        assert b.dtype == np.bool_ and list(b[:3]) == [True, False, True]
        del b
        ch.send(b"")
        c = ch.recv(30).base.to_numpy()          # INT with an NA: float64
        assert c.dtype == np.float64
        assert c[0] == 1 and np.isnan(c[2]) and c[3] == 4
        del c
        ch.send(b"")
        d = ch.recv(30).base.to_numpy()          # clean INT: the view
        assert d.dtype == np.int32 and not d.flags.writeable
        assert d[0] == 1 and d[-1] == 100
        del d
        ch.send(b"")
    finally:
        ch.close()


def test_r_peer_view_to_arrow(r_mizu):
    # .to_arrow() re-exposes the __arrow_c_array__ export (3.4): LGL is
    # Arrow bool, INT64 carries its validity bitmap
    pa = pytest.importorskip("pyarrow")
    src = """
l <- rep(c(TRUE, FALSE, NA), length.out = 1e6)
mizu::mizu_send(ch, l)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        arr = pa.Array._import_from_c_capsule(*ch.recv(30).base.to_arrow())
        assert arr.type == pa.bool_() and arr.null_count == 333333
        assert arr.slice(0, 3).to_pylist() == [True, False, None]
    finally:
        ch.close()


def test_py_peer_stamped_region_to_numpy():
    # pymizu stamps its own buffer stages known-NA-free ({0, -1}): a
    # genuine -2^31 in a Python int32 survives to_numpy as a value — the
    # section settles the verdict, no scan, no conversion
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create("import pymizu\n" + PY_ECHO)
    try:
        a = np.arange(200000, dtype=np.int32)
        a[5] = -2**31
        assert ch.send(a) is True
        back = ch.recv(30)               # the echo REFs the region back
        out = back.base.to_numpy()
        assert out.dtype == np.int32 and out[5] == -2**31
        assert np.shares_memory(out, back)
    finally:
        ch.close()


def test_r_peer_int64_na_warns(r_mizu):
    # int64 keeps its dtype and warns on a detected INT64_MIN (3.2's
    # warn-only rule), naming .to_arrow() as the NA-honest accessor
    np = pytest.importorskip("numpy")
    src = r"""
fromle <- function(r) readBin(r, "double", size = 8L, endian = "little",
                              n = length(r) %/% 8L)
v <- structure(fromle(as.raw(c(0, 0, 0, 0, 0, 0, 0, 0x80,
                               7, 0, 0, 0, 0, 0, 0, 0))),
               class = "integer64")
mizu::mizu_send(ch, v)
w <- structure(fromle(as.raw(c(1, 0, 0, 0, 0, 0, 0, 0,
                               2, 0, 0, 0, 0, 0, 0, 0))),
               class = "integer64")
mizu::mizu_send(ch, w)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        with pytest.warns(UserWarning, match="to_arrow"):
            a = ch.recv(30)
        assert a.dtype == np.int64 and list(a) == [-(2**63), 7]
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            b = ch.recv(30)
        assert list(b) == [1, 2]
    finally:
        ch.close()


def test_py_peer_int_sentinels_stay_exact():
    # the copied-read scans are gated on a foreign writer: a
    # Python<->Python int32 holding a genuine -2^31 (or int64 a -2^63)
    # round-trips unchanged, dtype and value, no warning (3.2)
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create("import pymizu\n" + PY_ECHO)
    try:
        a = np.array([1, -2**31, 3], dtype=np.int32)
        assert ch.send(a) is True
        got = ch.recv(30)
        assert got.dtype == np.int32 and list(got) == [1, -2**31, 3]
        b = np.array([1, -2**63], dtype=np.int64)
        assert ch.send(b) is True
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            got = ch.recv(30)
        assert got.dtype == np.int64 and list(got) == [1, -2**63]
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
        with pytest.warns(UserWarning, match="to_arrow"):
            got = ch.recv(30)   # 3.2's warn-only rule: dtype unchanged
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
        # recv_batch drains what has arrived; the peer's two statements
        # need not land before the first drain, so accumulate
        got = []
        while len(got) < 3:
            batch = ch.recv_batch(4, timeout=30)
            if not isinstance(batch, list):
                break
            got.extend(batch)
        assert got == ["a", [1.0, 2.0], "after"]
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


# ---------------------------------------------------------------------------
# The acceptance rows: a both-directions row per tag family, the documented
# relay shifts, the negative rows, and the identity exchange (spec-table
# coverage itself lives in the golden corpus, driven per binding).
# ---------------------------------------------------------------------------

# The Python echo peer is this test file's host program; R_ECHO is the R
# peer's loop. For R->Python->R the R program holds the value and reports
# the verdict of its own comparison.


def test_identity_exchange_reports_foreign(r_mizu):
    # an R peer reports MIZU_LANG_R with the layout caps mizu declares;
    # the word is read off the region, no launcher attribute
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        assert ch._h._peer_ident() == (2, 31)  # R: all five caps
    finally:
        ch.close()


def test_py_r_py_exact_families(r_mizu):
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        exact = [
            None, True, False, 1, -1, 1.5, -2.5, 1 + 2j, "héllo ✓", "",
            0, 2147483647, -2147483647,
            [1, "a", [True, 2.5]],
            {"a": 1, "b": {"z": None}},
            np.array([1.5, 2.5]),
            np.array([1, 2, 3], dtype=np.int32),
            np.array([1, 2], dtype=np.int64),
            np.array([1, 255], dtype=np.uint8),
            np.array([1 + 2j, -3 + 0.5j]),
            np.array([[1.0, 2.0], [3.0, 4.0]]),          # C-order 2x2
            np.asfortranarray(np.arange(6).reshape(2, 3)),
            np.array([19000, 19001], dtype="datetime64[D]"),
            np.array([1700000000000000, 1700000000500000],
                     dtype="datetime64[us]"),
        ]
        for x in exact:
            assert ch.send(x) is True
            got = ch.recv(30)
            if isinstance(x, np.ndarray):
                if x.dtype.kind == "M":
                    assert np.array_equal(got.view(np.int64), x.view(np.int64))
                else:
                    assert np.array_equal(got, x)
            else:
                assert got == x
        # numpy scalars nested in a dict read as Python scalars
        assert ch.send({"m": np.float64(1.5), "i": np.int64(7),
                        "b": np.bool_(True)}) is True
        assert ch.recv(30) == {"m": 1.5, "i": 7, "b": True}
        # a nested bool_ array returns as a bool_ array (lglv both ways)
        assert ch.send({"b": np.array([True, False])}) is True
        got = ch.recv(30)
        assert got["b"].dtype == np.bool_ and list(got["b"]) == [True, False]
    finally:
        ch.close()


def test_py_r_py_documented_shifts(r_mizu):
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        # a tuple returns as a list
        assert ch.send((1, "a")) is True
        assert ch.recv(30) == [1, "a"]
        # a C-order multi-dim array returns F-order, value-exact
        m = np.arange(6, dtype=np.float64).reshape(2, 3)
        assert ch.send(m) is True
        got = ch.recv(30)
        assert got.flags.f_contiguous and np.array_equal(got, m)
        # stdlib date / datetime scalars return as length-1 arrays
        import datetime
        assert ch.send(datetime.date(2022, 3, 21)) is True
        got = ch.recv(30)
        assert got == np.array(["2022-03-21"], dtype="datetime64[D]")[0]
        # length-1 int32 / float64 / complex128 / bool_ arrays -> scalars
        for a, want in [
            (np.array([5], dtype=np.int32), 5),
            (np.array([1.5]), 1.5),
            (np.array([1 + 2j]), 1 + 2j),
            (np.array([True]), True),
        ]:
            assert ch.send(a) is True
            assert ch.recv(30) == want
        # an int outside (INT_MIN, INT_MAX] -> a length-1 int64 array
        for v in [-2147483648, 2147483648, 2**53]:
            assert ch.send(v) is True
            got = ch.recv(30)
            assert got.dtype == np.int64 and got[0] == v
        # an int of exactly -2^63 -> None (the sentinel collision)
        assert ch.send(-(2**63)) is True
        assert ch.recv(30) is None
        # an int32 array holding -2^31: R reads NA_integer_ (the value
        # shift); the return hop applies the copied-read rule (3.2) —
        # a foreign INT with NAs reads float64 with NA_real_-payload NaNs
        a = np.array([1, -2147483648, 3], dtype=np.int32)
        assert ch.send(a) is True
        got = ch.recv(30)
        assert got.dtype == np.float64
        assert got[0] == 1 and np.isnan(got[1]) and got[2] == 3
        # bytes -> a uint8 array
        assert ch.send(b"\x00\x01") is True
        assert np.array_equal(ch.recv(30), [0, 1])
        # the widened dtypes
        cases = [
            (np.array([1, 2], dtype=np.int8), np.int32),
            (np.array([1, 2], dtype=np.uint32), np.float64),
            (np.array([1, 2, 3], dtype=np.float32), np.float64),
            (np.array([1 + 2j, 3 + 4j], dtype=np.complex64), np.complex128),
        ]
        for a, dt in cases:
            assert ch.send(a) is True
            got = ch.recv(30)
            assert got.dtype == dt and np.allclose(got, a)
    finally:
        ch.close()


def test_r_py_r_exact_families(r_mizu):
    # the R side holds each value and reports identical() on the relay
    src = r"""
report <- function(x, expect = x) {
  mizu::mizu_send(ch, x)
  y <- mizu::mizu_recv(ch, timeout = 30)
  mizu::mizu_send(ch, identical(y, expect))
}
report(NULL)
report(TRUE)
report(1L)
report(1.5)
report(1+2i)
report("héllo ✓")
report(as.raw(c(0x00, 0xff)))
report(list(1L, "a", list(TRUE, 2.5)))
report(list(a = 1L, b = list(z = NULL)))
report(c(1.5, 2.5))
report(c(1L, 2L, 3L))
report(c(TRUE, FALSE, TRUE))   # NA-free logical stays exact (3.2)
report(c(1+2i, -3+0.5i))
report(matrix(1:6, 2, 3))
report(matrix(c(1+1i, 2+2i, 3+3i, 4+4i), 2))
report(matrix(as.raw(1:6), 2, 3))
report(list(b = c(TRUE, FALSE)))
report(as.Date("2022-03-21") + 0:2)
report(.POSIXct(c(1700000000, 1700000000.5), tz = "UTC"))
if (requireNamespace("bit64", quietly = TRUE)) {
  report(bit64::as.integer64(c(1, -1, 2^53 + 1)))
  report(bit64::as.integer64(5))          # length 1 stays integer64
  im4 <- bit64::as.integer64(1:4)
dim(im4) <- c(2L, 2L)
report(im4)
}
lx <- iconv(c("héllo", "wörld"), from = "UTF-8", to = "latin1")
mizu::mizu_send(ch, lx)
y <- mizu::mizu_recv(ch, timeout = 30)
mizu::mizu_send(ch, identical(y, as.list(lx)))   # the strv shift: a list
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        n_reports = src.count("report(")
        for _ in range(n_reports):
            x = ch.recv(30)
            assert ch.send(x) is True
            verdict = ch.recv(30)
            assert verdict is True
        lx = ch.recv(30)
        assert lx == ["héllo", "wörld"]
        assert ch.send(lx) is True
        # the strv shift: a length != 1 string vector returns as a list
        assert ch.recv(30) is True
    finally:
        ch.close()


def test_r_py_r_documented_shifts(r_mizu):
    # one row per documented R->Python->R shift, R reporting the verdict
    src = r"""
report2 <- function(x, expect) {
  mizu::mizu_send(ch, x)
  y <- mizu::mizu_recv(ch, timeout = 30)
  mizu::mizu_send(ch, identical(y, expect))
}
report2(c("a", "bc"), list("a", "bc"))        # strv len != 1 -> a list
report2(factor(c("b", "a"), levels = c("a", "b")), list("b", "a"))
report2(NA, NULL)                              # the NA scalars -> NULL
report2(NA_integer_, NULL)
report2(NA_character_, NULL)
report2(array(1:2, 2), 1:2)                    # a length-1 dim: plain vector
report2(.POSIXct(1700000000, tz = ""),
        .POSIXct(1700000000, tz = "UTC"))      # absent == "" -> "UTC"
report2(.POSIXct(1700000000, tz = "Europe/Paris"),
        .POSIXct(1700000000, tz = "UTC"))      # a named zone normalizes
report2(c(TRUE, NA), c(1L, NA))                # logical with NA -> integer
# integer with NA: 3.2's copied-read rule reads the foreign INT as
# float64 with NA_real_-payload NaNs — the relay returns numeric (3.2)
report2(c(1L, NA, -3L), c(1, NA, -3))
report2(c(1L, NA), c(1, NA))
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        n_reports = src.count("report2(")
        for _ in range(n_reports):
            x = ch.recv(30)
            assert ch.send(x) is True
            verdict = ch.recv(30)
            assert verdict is True
    finally:
        ch.close()


def test_crosslang_negative_rows(r_mizu):
    np = pytest.importorskip("numpy")
    # R-side send-time declines (mizu_error_not_portable), reported back
    src = r"""
report_err <- function(x) {
  e <- tryCatch({ mizu::mizu_send(ch, x); "no error" },
                error = function(e) conditionMessage(e))
  mizu::mizu_send(ch, e)
}
report_err(ordered(c("a", "b")))
df <- data.frame(x = 1:2)
class(df) <- c("tbl_df", "tbl", "data.frame") # a tibble's shape, no package
report_err(df)
bs <- "héllo"
Encoding(bs) <- "bytes"
report_err(bs)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        for _ in range(src.count("report_err(")):
            got = ch.recv(30)
            assert "not portable" in got
    finally:
        ch.close()
    # Python-side DeclinedErrors over the live foreign channel
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        with pytest.raises(pymizu.DeclinedError):
            ch.send(np.array([1], dtype="m8[s]"))
        pa = pytest.importorskip("pyarrow")
        with pytest.raises(pymizu.DeclinedError, match="no portable home"):
            ch.send(pa.table({"t": pa.array([1], type=pa.time32("s"))}))
        pl = pytest.importorskip("polars")
        with pytest.raises(pymizu.DeclinedError, match="pl.Categorical"):
            ch.send(pl.Series("e", ["a"], dtype=pl.Enum(["a", "b"])))
        with pytest.raises(pymizu.DeclinedError, match="ordered"):
            ch.send(pa.table({
                "c": pa.DictionaryArray.from_arrays(
                    [0], pa.array(["a"]), ordered=True)
            }))
        pd = pytest.importorskip("pandas")
        with pytest.raises(pymizu.DeclinedError):
            ch.send(pd.DataFrame({
                "o": pd.Series([1, "x"], dtype=object)   # pyarrow rejects
            }))
    finally:
        ch.close()


def test_crosslang_frame_and_arrow_consumers(r_mizu):
    pa = pytest.importorskip("pyarrow")
    pl = pytest.importorskip("polars")
    # data.frame -> Frame: to_dict with no Arrow library, consumers one line
    src = r"""
df <- data.frame(
  n = c(1.5, 2.5, 3.5),
  f = factor(c("a", "b", "a")),
  s = c("x", "y", "z"),
  i = 1:3
)
mizu::mizu_send(ch, df)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        f = ch.recv(30)
        assert isinstance(f, pymizu.Frame)
        d = f.to_dict()
        assert d["n"].tolist() == [1.5, 2.5, 3.5]
        assert d["f"] == ["a", "b", "a"]
        assert d["s"] == ["x", "y", "z"]
        assert d["i"].tolist() == [1, 2, 3]
        assert f.row_names is None
        assert pl.DataFrame(f).to_dicts() == [
            {"n": 1.5, "f": "a", "s": "x", "i": 1},
            {"n": 2.5, "f": "b", "s": "y", "i": 2},
            {"n": 3.5, "f": "a", "s": "z", "i": 3},
        ]
        t = pa.table(f)
        assert t.num_rows == 3
        assert t.to_pydict() == {
            "n": [1.5, 2.5, 3.5],
            "f": ["a", "b", "a"],
            "s": ["x", "y", "z"],
            "i": [1, 2, 3],
        }
        # the factor column is an Arrow dictionary column
        assert str(pa.table(f).schema.field("f").type).startswith("dictionary")
    finally:
        ch.close()


def test_crosslang_frame_relay_through_export(r_mizu):
    # a data.frame relayed R -> Python -> R through the received Frame's
    # own export: factor columns and row names included, identical()
    src = r"""
relay <- function(df) {
  mizu::mizu_send(ch, df)
  f <- mizu::mizu_recv(ch, timeout = 30)
  mizu::mizu_send(ch, f)
  y <- mizu::mizu_recv(ch, timeout = 30)
  mizu::mizu_send(ch, identical(y, df))
}
relay(data.frame(n = c(1.5, 2.5), f = factor(c("a", "b"))))
relay(data.frame(x = 1:3, row.names = c("a", "b", "c")))
relay(data.frame(x = 1:3, row.names = c(10L, 20L, 30L)))
relay(data.frame(d = as.Date("2020-01-01") + 0:2,
                 t = .POSIXct(c(1700000000, 1700000001, 1700000002),
                              tz = "UTC")))
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        for _ in range(src.count("relay(")):
            f = ch.recv(30)
            assert isinstance(f, pymizu.Frame)
            assert ch.send(f) is True          # the Frame's own export
            f2 = ch.recv(30)                   # R echoes the data.frame back
            assert ch.send(f2) is True
            assert ch.recv(30) is True
    finally:
        ch.close()


def test_crosslang_arrow_frames_to_dataframe(r_mizu):
    # Arrow frames write the frame shape, R reads a data.frame (polars,
    # pyarrow, pandas rows; multi-batch included)
    src = r"""
chk <- function(expected_names, n) {
  df <- mizu::mizu_recv(ch, timeout = 30)
  mizu::mizu_send(ch, is.data.frame(df) &&
    identical(names(df), expected_names) && nrow(df) == n)
}
chk(c("s", "x", "c"), 3L)
chk(c("a", "b"), 4L)
chk(c("s", "o", "i", "c"), 2L)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        pl = pytest.importorskip("polars")
        ch.send(pl.DataFrame({
            "s": ["a", "b", None],
            "x": [1, 2, 3],
            "c": pl.Series(["u", "v", "u"], dtype=pl.Categorical),
        }))
        assert ch.recv(30) is True
        pa = pytest.importorskip("pyarrow")
        ch.send(pa.table({
            "a": pa.chunked_array([[1, 2], [3, 4]]),
            "b": pa.chunked_array([["x", "y"], ["z", "w"]]),
        }))
        assert ch.recv(30) is True
        pd = pytest.importorskip("pandas")
        ch.send(pd.DataFrame({
            "s": pd.array(["a", "b"], dtype="str"),
            "o": pd.Series(["x", None], dtype=object),
            "i": pd.array([1, None], dtype="Int64"),
            "c": pd.Series(["u", "v"], dtype=pd.CategoricalDtype(["u", "v"])),
        }))
        assert ch.recv(30) is True
    finally:
        ch.close()


def test_crosslang_altrep_and_large_attributed(r_mizu):
    np = pytest.importorskip("numpy")
    # compact sequences cross value-exact, the sender staying compact;
    # a big one crosses as a zero-copy view
    src = r"""
x <- 1:100000
mizu::mizu_send(ch, x)
mizu::mizu_send(ch, length(serialize(x, NULL)) < 1000L)
big <- 1:1e7
mizu::mizu_send(ch, big)
mizu::mizu_send(ch, length(serialize(big, NULL)) < 1000L)
df <- data.frame(a = 1:100000, b = seq(0, 1, length.out = 100000))
mizu::mizu_send(ch, df)
f <- factor(rep(c("a", "b"), length.out = 100000))
mizu::mizu_send(ch, f)
e <- tryCatch({
  nv <- runif(100000)
  names(nv) <- paste0("n", seq_along(nv))
  mizu::mizu_send(ch, nv)
  "no error"
}, error = function(e) conditionMessage(e))
mizu::mizu_send(ch, e)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        got = ch.recv(30)
        assert np.array_equal(np.asarray(got), np.arange(1, 100001))
        assert ch.recv(30) is True               # the sender is still compact
        big = ch.recv(30)
        assert isinstance(big, np.ndarray) and not big.flags.writeable
        assert big[0] == 1 and big[-1] == 1e7
        assert ch.recv(30) is True
        f = ch.recv(30)
        d = f.to_dict()
        assert d["a"].tolist()[:3] == [1, 2, 3]
        assert d["b"].tolist()[-1] == 1.0
        fac = ch.recv(30)
        assert fac == ["a", "b"] * 50000        # a big factor: the copy tier
        got = ch.recv(30)
        assert "not portable" in got and "named atomic vector" in got
    finally:
        ch.close()


def test_crosslang_matrices_and_mixed_dict(r_mizu):
    np = pytest.importorskip("numpy")
    # matrices both directions identical(), a 3-D array, an integer64
    # matrix, and the mixed dict-of-arrays (the data-science payload)
    src = r"""
m <- matrix(c(1.5, 2.5, 3.5, 4.5, 5.5, 6.5), 2, 3)
mizu::mizu_send(ch, m)
y <- mizu::mizu_recv(ch, timeout = 30)
mizu::mizu_send(ch, identical(y, m))
a <- array(1:8, c(2, 2, 2))
mizu::mizu_send(ch, a)
y <- mizu::mizu_recv(ch, timeout = 30)
mizu::mizu_send(ch, identical(y, a))
im <- bit64::as.integer64(1:6)
dim(im) <- c(2L, 3L)   # class preserved (base matrix() would drop it)
mizu::mizu_send(ch, im)
y <- mizu::mizu_recv(ch, timeout = 30)
mizu::mizu_send(ch, identical(y, im))
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        got = ch.recv(30)
        assert got.shape == (2, 3) and got.flags.f_contiguous
        assert ch.send(got) is True
        assert ch.recv(30) is True
        got = ch.recv(30)
        assert got.shape == (2, 2, 2)
        assert ch.send(got) is True
        assert ch.recv(30) is True
        got = ch.recv(30)
        assert got.dtype == np.int64 and got.shape == (2, 3)
        assert ch.send(got) is True
        assert ch.recv(30) is True
    finally:
        ch.close()
    # the mixed dict-of-arrays
    ch = pymizu.Channel.create(R_ECHO, launcher=r_mizu)
    try:
        x = {
            "floats": np.array([1.5, 2.5]),
            "ints": np.array([1, 2], dtype=np.int32),
            "flags": np.array([True, False]),
            "strings": ["a", None, "c"],
            "count": 3,
            "label": "exp",
        }
        assert ch.send(x) is True
        got = ch.recv(30)
        assert set(got) == set(x)
        assert np.array_equal(got["floats"], x["floats"])
        assert np.array_equal(got["ints"], x["ints"])
        assert got["strings"] == ["a", None, "c"]
        assert got["flags"].dtype == np.bool_
        assert got["count"] == 3 and got["label"] == "exp"
    finally:
        ch.close()


# ---------------------------------------------------------------------------
# Phase 3.5: attributed layouts R -> Python — the region-backed homes (the
# MIZL tree wrap, the region-backed Frame, MIZH attr roots) and the validity
# sections on foreign sends.


R_REGION_FRAME = r"""
df <- data.frame(x = 1:300000, y = runif(300000),
                 s = rep(c("a", "b", NA), 100000),
                 stringsAsFactors = FALSE)
mizu::mizu_send(ch, df)
mizu::mizu_recv(ch, timeout = 60)
"""


def test_r_peer_frame_region_backed(r_mizu):
    # an R data.frame past the zc floor crosses as one MIZL region: the
    # numeric columns are views over the shared pages, the string column
    # reads off its MIZS block; row.names automatic -> None
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_REGION_FRAME, launcher=r_mizu)
    try:
        f = ch.recv(60)
        assert type(f).__name__ == "Frame"
        assert len(f) == 300000 and f.names == ("x", "y", "s")
        assert f.row_names is None
        d = f.to_dict()
        assert d["x"].dtype == np.int32 and not d["x"].flags.writeable
        assert type(d["x"].base).__name__ == "_ShmView"
        assert d["x"][:5].tolist() == [1, 2, 3, 4, 5]
        assert d["y"].dtype == np.float64 and not d["y"].flags.writeable
        assert d["s"][:5] == ["a", "b", None, "a", "b"]
    finally:
        ch.close()


def test_r_peer_frame_factor_and_int64_columns(r_mizu):
    # a factor leaf is a dictionary column (the region's 1-based codes, the
    # blob's levels); an integer64 leaf rides directory tag 32 to int64
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    src = r"""
df <- data.frame(f = factor(rep(c("aa", "bb", NA), 100000)),
                 i = bit64::as.integer64(1:300000))
mizu::mizu_send(ch, df)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        f = ch.recv(60)
        d = f.to_dict()
        assert d["f"][:5] == ["aa", "bb", None, "aa", "bb"]
        assert d["i"].dtype == np.int64 and d["i"][:3].tolist() == [1, 2, 3]
        t = pa.table(f)
        assert t.schema.field("f").type == pa.dictionary(
            pa.int32(), pa.string())
        assert t.column("f").null_count == 100000
        assert t.column("f").slice(0, 3).to_pylist() == ["aa", "bb", None]
        assert t.schema.field("i").type == pa.int64()
    finally:
        ch.close()


def test_r_peer_frame_date_posixct_columns(r_mizu):
    # Date / POSIXct columns convert (owned): date32 and timestamp[us],
    # the POSIXct tzone kept as export metadata
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    src = r"""
df <- data.frame(d = as.Date("2024-01-01") + 0:299999,
                 p = as.POSIXct("2024-01-01 00:00:00", tz = "UTC") +
                   0:299999)
mizu::mizu_send(ch, df)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        f = ch.recv(60)
        d = f.to_dict()
        assert d["d"].dtype == np.dtype("datetime64[D]")
        assert str(d["d"][1]) == "2024-01-02"
        assert d["p"].dtype == np.dtype("datetime64[us]")
        t = pa.table(f)
        assert t.schema.field("d").type == pa.date32()
        assert t.schema.field("p").type == pa.timestamp("us", tz="UTC")
        assert t.column("p").null_count == 0
    finally:
        ch.close()


def test_r_peer_frame_row_names(r_mizu):
    # non-automatic row.names ride the root blob: character as a list,
    # integer as an int32 array
    np = pytest.importorskip("numpy")
    src = r"""
df <- data.frame(x = 1:300000, y = runif(300000))
row.names(df) <- paste0("r", 1:300000)
mizu::mizu_send(ch, df)
df2 <- data.frame(x = 1:300000)
row.names(df2) <- as.integer(seq(2, 600000, by = 2))
mizu::mizu_send(ch, df2)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        f = ch.recv(60)
        assert f.row_names[:3] == ["r1", "r2", "r3"]
        f2 = ch.recv(60)
        assert f2.row_names.dtype == np.int32
        assert f2.row_names[:3].tolist() == [2, 4, 6]
    finally:
        ch.close()


def test_r_peer_factor_standalone_region(r_mizu):
    # a standalone factor past the floor: an MIZH INT root with the factor
    # blob — the same list[str | None] home as the copy tier
    src = r"""
f <- factor(rep(c("x", "y", NA), 100000))
mizu::mizu_send(ch, f)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        f = ch.recv(60)
        assert type(f) is list and len(f) == 300000
        assert f[:5] == ["x", "y", None, "x", "y"]
    finally:
        ch.close()


def test_r_peer_matrix_region_f_order(r_mizu):
    # a matrix past the floor arrives as an F-order view over the region
    # (the {dim} blob on an MIZH root); a re-send crosses by value with
    # its shape — REF stays a 1-D rule — value-exact both ways
    np = pytest.importorskip("numpy")
    src = r"""
m <- matrix(runif(200000), nrow = 400)
mizu::mizu_send(ch, m)
y <- mizu::mizu_recv(ch, timeout = 30)
mizu::mizu_send(ch, identical(y, m))
im <- bit64::as.integer64(1:200000)
dim(im) <- c(400L, 500L)
mizu::mizu_send(ch, im)
z <- mizu::mizu_recv(ch, timeout = 30)
mizu::mizu_send(ch, identical(z, im))
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        m = ch.recv(60)
        assert m.shape == (400, 500) and m.flags.f_contiguous
        assert m.dtype == np.float64 and not m.flags.writeable
        assert ch.send(m) is True                  # F-order: by value
        assert ch.recv(30) is True
        im = ch.recv(60)
        assert im.shape == (400, 500) and im.flags.f_contiguous
        assert im.dtype == np.int64 and im[:3, 0].tolist() == [1, 2, 3]
        assert im[0, :3].tolist() == [1, 401, 801]   # F-order layout
        assert ch.send(np.ascontiguousarray(im)) is True   # C-order
        assert ch.recv(30) is True
    finally:
        ch.close()


def test_r_peer_date_posixct_region(r_mizu):
    # Date / POSIXct MIZH roots materialize datetime64 off the shared
    # pages (one conversion copy)
    np = pytest.importorskip("numpy")
    src = r"""
d <- as.Date("2024-01-01") + 0:299999
mizu::mizu_send(ch, d)
p <- as.POSIXct("2024-01-01 00:00:00", tz = "UTC") + 0:299999
mizu::mizu_send(ch, p)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        d = ch.recv(60)
        assert d.dtype == np.dtype("datetime64[D]")
        assert str(d[0]) == "2024-01-01"
        assert d[-1] == np.datetime64("2024-01-01") + 299999
        p = ch.recv(60)
        assert p.dtype == np.dtype("datetime64[us]")
        assert p[0] == np.datetime64("2024-01-01T00:00:00.000000")
    finally:
        ch.close()


def test_r_peer_named_list_dict_of_views(r_mizu):
    # a large named list crosses as one region: a dict of views over the
    # shared pages (one loan), not an 'I' copy
    np = pytest.importorskip("numpy")
    src = r"""
x <- list(a = runif(200000), b = 1:200000)
mizu::mizu_send(ch, x)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        d = ch.recv(60)
        assert type(d) is dict and sorted(d) == ["a", "b"]
        assert type(d["a"].base).__name__ == "_ShmView"
        assert d["a"].dtype == np.float64 and not d["a"].flags.writeable
        assert d["b"][:3].tolist() == [1, 2, 3]
        # one loan for the whole tree: both views name one region
        assert d["a"].base.refcount == d["b"].base.refcount == 2
    finally:
        ch.close()


def test_r_peer_nested_tree(r_mizu):
    # a nested tree recurses: dict homes within an unnamed list, string
    # leaves as string views over the same region
    np = pytest.importorskip("numpy")
    src = r"""
nested <- list(p = list(q = runif(200000)),
               r = list(s = 1:200000, t = "hi"))
mizu::mizu_send(ch, nested)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        n = ch.recv(60)
        assert type(n) is dict and sorted(n) == ["p", "r"]
        assert n["p"]["q"].dtype == np.float64
        assert n["r"]["s"][:2].tolist() == [1, 2]
        assert n["r"]["t"].to_list() == ["hi"]
        assert n["p"]["q"].base.refcount == 2   # one loan for the tree
    finally:
        ch.close()


R_PATH_REF = r"""
x <- list(runif(200000), 1:200000)
mizu::mizu_send(ch, x)
v <- mizu::mizu_recv(ch, timeout = 30)   # the REF back: R's own view
el <- v[[2]]                              # an element view (a path REF)
mizu::mizu_send(ch, el)
mizu::mizu_recv(ch, timeout = 60)
"""


def test_r_peer_path_ref(r_mizu):
    # an R element view re-sent crosses as a path REF: the tree walk
    # resolves it to the element's wrap over the same region
    pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_PATH_REF, launcher=r_mizu)
    try:
        x = ch.recv(60)
        assert ch.send(x) is True
        el = ch.recv(60)
        assert type(el).__name__ == "ndarray"
        assert el[:4].tolist() == [1, 2, 3, 4]
        assert type(el.base).__name__ == "_ShmView"
    finally:
        ch.close()


R_FRAME_REF = r"""
df <- data.frame(a = runif(200000), b = 1:200000)
mizu::mizu_send(ch, df)
v <- mizu::mizu_recv(ch, timeout = 30)   # the frame REF: R's own view
mizu::mizu_send(ch, c(identical(v, df),
                      .Call(mizu:::mizu_zc_refcount, v)[[2L]] %% 2L == 1L))
mizu::mizu_recv(ch, timeout = 60)
"""


def test_r_peer_frame_ref(r_mizu):
    # a received Frame re-sent whole crosses as a REF naming R's own
    # region (REFHELD): R's view of it is identical() to what it sent
    np = pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_FRAME_REF, launcher=r_mizu)
    try:
        f = ch.recv(60)
        assert type(f).__name__ == "Frame"
        assert ch.send(f) is True
        assert np.asarray(ch.recv(30)).tolist() == [1, 1]
    finally:
        ch.close()


R_REFCOUNT = r"""
df <- data.frame(x = 1:300000, y = runif(300000))
mizu::mizu_send(ch, df)
v <- mizu::mizu_recv(ch, timeout = 30)   # the REF back: R's own view
mizu::mizu_send(ch, .Call(mizu:::mizu_zc_refcount, v)[[1L]])
mizu::mizu_recv(ch, timeout = 30)         # Python dropped the tree
mizu::mizu_send(ch, .Call(mizu:::mizu_zc_refcount, v)[[1L]])
mizu::mizu_recv(ch, timeout = 60)
"""


def test_r_peer_tree_refcount_balance(r_mizu):
    # one counted zc-ref per tree: the wrap adds one, the last view's
    # death subs it — the region's refcount returns to the producer's
    pytest.importorskip("numpy")
    ch = pymizu.Channel.create(R_REFCOUNT, launcher=r_mizu)
    try:
        f = ch.recv(60)
        v = f.to_dict()["x"].base
        assert v.refcount == 2          # R's producer loan + the tree's
        assert ch.send(f) is True       # the frame REF
        # R's producer loan is reaped by its own later verbs, so the
        # steady state is the tree anchor + R's own view ...
        assert ch.recv(30) == 2
        del f, v
        gc.collect()
        assert ch.send("drop") is True
        # ... then just R's view: the tree anchor's sub fired
        assert ch.recv(30) == 1
    finally:
        ch.close()


def test_r_peer_frame_export_polars(r_mizu):
    # the region-backed Frame's Arrow export: polars ingests the stream
    # zero-copy — every fixed-width column's data pointer inside the
    # acquisition's own mapping, the string column's variadic buffer too
    pytest.importorskip("numpy")
    pl = pytest.importorskip("polars")
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)
    src = r"""
df <- data.frame(x = c(1:299999, NA), y = runif(300000),
                 s = rep(c("short", strrep("long", 1000), NA), 100000),
                 stringsAsFactors = FALSE)
mizu::mizu_send(ch, df)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        f = ch.recv(60)
        pf = pl.from_arrow(f)
        assert pf.schema == {"x": pl.Int32, "y": pl.Float64, "s": pl.String}
        assert pf["x"].null_count() == 1 and pf["s"].null_count() == 100000
        spans = pymizu._pymizu._debug_export_spans()
        ptrs = _polars_col_buffers(pf)
        # Int32 / Float64: the values buffer; String: the variadic data
        # buffer (a >12-byte string keeps it non-inline)
        for p in (ptrs[0][1], ptrs[1][1], ptrs[2][2]):
            assert any(a <= p < a + s for a, s in spans)
    finally:
        ch.close()


def test_r_peer_frame_export_survives_frame(r_mizu):
    # the export's loan is its own: a consumer holding the arrays pins
    # the region past the Frame's death. A fully conforming consumer
    # (the explicit pyarrow reader) runs the C-side release chain to
    # zero — sub and unmap; polars keeps some structs past del + gc
    # (consumer-side), so its half asserts survival, not the release
    pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    pl = pytest.importorskip("polars")
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)
    ch = pymizu.Channel.create(R_REGION_FRAME, launcher=r_mizu)
    try:
        f = ch.recv(60)
        before = set(pymizu._pymizu._debug_export_spans())
        reader = pa.RecordBatchStreamReader.from_stream(f)
        t = reader.read_all()
        assert t.num_rows == 300000
        assert len(set(pymizu._pymizu._debug_export_spans()) - before) == 1
        del reader, t
        gc.collect()
        assert set(pymizu._pymizu._debug_export_spans()) == before
        v = f.to_dict()["x"].base
        assert v.refcount == 2
        pf = pl.from_arrow(f)
        assert v.refcount == 3          # + the export's acquisition
        del f, v
        gc.collect()
        # polars holds the arrays: the region stays mapped, the data valid
        assert len(set(pymizu._pymizu._debug_export_spans()) - before) == 1
        assert pf["x"][0] == 1
        del pf
        gc.collect()
    finally:
        ch.close()


def test_r_peer_frame_validity_sections(r_mizu):
    # the validity section on a foreign send: an INT column's NAs read
    # off the leaf's section (to_numpy converts, Arrow nulls), a clean
    # column is known-NA-free (no scan, a NULL bitmap)
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    src = r"""
df <- data.frame(x = c(1:299999, NA), y = 1:300000)
mizu::mizu_send(ch, df)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        f = ch.recv(60)
        d = f.to_dict()
        x = d["x"].base.to_numpy()      # the section says NAs: float64
        assert x.dtype == np.float64 and np.isnan(x[-1])
        y = d["y"].base.to_numpy()      # known-NA-free: the int32 view
        assert y.dtype == np.int32 and not y.flags.writeable
        t = pa.table(f)
        assert t.column("x").null_count == 1
        assert t.column("y").null_count == 0
    finally:
        ch.close()


def test_r_peer_seq_columned_frame_region(r_mizu):
    # an ALTREP (seq) column crosses as MIZL on a foreign handle, the
    # sender's column staying compact
    pytest.importorskip("numpy")
    src = r"""
df <- data.frame(id = 1:300000, v = runif(300000))
mizu::mizu_send(ch, df)
mizu::mizu_send(ch, length(serialize(df[[1L]], NULL)) < 1000L)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        f = ch.recv(60)
        d = f.to_dict()
        assert d["id"][:3].tolist() == [1, 2, 3]
        assert d["id"][-1] == 300000
        assert ch.recv(30) is True      # the sender is still compact
    finally:
        ch.close()


def test_r_peer_region_declines_on_r_side(r_mizu):
    # past the floor on a foreign handle: a named vector, a difftime and
    # a closure-leafed tree all raise at send (no portable home)
    src = r"""
e1 <- tryCatch({
  nv <- runif(300000)
  names(nv) <- paste0("n", 1:300000)
  mizu::mizu_send(ch, nv)
  "no error"
}, error = function(e) conditionMessage(e))
mizu::mizu_send(ch, e1)
e2 <- tryCatch({
  mizu::mizu_send(ch, as.difftime(1:300000, units = "secs"))
  "no error"
}, error = function(e) conditionMessage(e))
mizu::mizu_send(ch, e2)
e3 <- tryCatch({
  mizu::mizu_send(ch, list(f = function() 1, big = runif(200000)))
  "no error"
}, error = function(e) conditionMessage(e))
mizu::mizu_send(ch, e3)
mizu::mizu_recv(ch, timeout = 60)
"""
    ch = pymizu.Channel.create(src, launcher=r_mizu)
    try:
        assert "not portable" in ch.recv(30)
        assert "not portable" in ch.recv(30)
        assert "not portable" in ch.recv(30)
    finally:
        ch.close()


def _polars_col_buffers(pf):
    """Each column's ArrowArray buffer pointers off the polars frame's
    own __arrow_c_stream__ export (the zero-copy aliasing assertion)."""
    import ctypes

    class ArrowArrayStream(ctypes.Structure):
        _fields_ = [(n, ctypes.c_void_p) for n in
                    ("get_schema", "get_next", "get_last_error",
                     "release", "private_data")]

    class ArrowArray(ctypes.Structure):
        pass

    ArrowArray._fields_ = [
        ("length", ctypes.c_int64), ("null_count", ctypes.c_int64),
        ("offset", ctypes.c_int64), ("n_buffers", ctypes.c_int64),
        ("n_children", ctypes.c_int64),
        ("buffers", ctypes.POINTER(ctypes.c_void_p)),
        ("children", ctypes.POINTER(ctypes.POINTER(ArrowArray))),
        ("dictionary", ctypes.POINTER(ArrowArray)),
        ("release", ctypes.c_void_p), ("private_data", ctypes.c_void_p),
    ]
    get_ptr = ctypes.pythonapi.PyCapsule_GetPointer
    get_ptr.restype = ctypes.c_void_p
    get_ptr.argtypes = [ctypes.py_object, ctypes.c_char_p]
    addr = get_ptr(pf.__arrow_c_stream__(), b"arrow_array_stream")
    stream = ArrowArrayStream.from_address(addr)
    get_next = ctypes.CFUNCTYPE(
        ctypes.c_int, ctypes.POINTER(ArrowArrayStream),
        ctypes.POINTER(ArrowArray))(stream.get_next)
    arr = ArrowArray()
    # polars' stream is single-shot: exactly one get_next, no get_schema
    try:
        assert get_next(ctypes.byref(stream), ctypes.byref(arr)) == 0
        out = []
        for i in range(arr.n_children):
            child = arr.children[i].contents
            out.append([child.buffers[j] for j in range(child.n_buffers)])
        return out
    finally:
        if arr.release:
            ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArray))(
                arr.release)(ctypes.byref(arr))


# Phase 3.6/3.8: attributed layouts Python -> R (the MIZL frame writer) and
# the export-provenance REF on the round trip.


R_FRAME_READ = r"""
df <- mizu::mizu_recv(ch, timeout = 60)
res <- c(
  is.data.frame(df) && nrow(df) == 300000L && ncol(df) == 4L,
  identical(names(df), c("i", "x", "s", "f")),
  is.integer(df[["i"]]) && is.double(df[["x"]]) &&
    is.character(df[["s"]]) && is.factor(df[["f"]]),
  sum(is.na(df[["i"]])) == 30000L && sum(is.na(df[["x"]])) == 30000L,
  sum(is.na(df[["s"]])) == 30000L && sum(is.na(df[["f"]])) == 30000L,
  identical(levels(df[["f"]]), c("v", "w", "u")),   # first-seen order
  identical(df[["i"]][1:4], c(NA_integer_, 1:3)),
  identical(df[["x"]][2], 0.5),
  identical(df[["s"]][1:3], c(NA, "s1", "s2"))
)
mizu::mizu_send(ch, res)
"""


def test_py_peer_polars_frame_to_dataframe(r_mizu):
    # a polars frame past the floor stages as one MIZL region (string_view
    # columns gathered): R reads a data.frame
    pl = pytest.importorskip("polars")
    n = 300000
    ch = pymizu.Channel.create(R_FRAME_READ, launcher=r_mizu)
    try:
        ch.send(pl.DataFrame({
            "i": pl.Series([None if k % 10 == 0 else k for k in range(n)],
                           dtype=pl.Int32),
            "x": pl.Series([None if k % 10 == 0 else k * 0.5
                            for k in range(n)], dtype=pl.Float64),
            "s": pl.Series([None if k % 10 == 0 else f"s{k % 100}"
                            for k in range(n)], dtype=pl.String),
            "f": pl.Series([None if k % 10 == 0 else ["u", "v", "w"][k % 3]
                            for k in range(n)], dtype=pl.Categorical),
        }))
        assert all(ch.recv(60))
    finally:
        ch.close()


R_FRAME_READ2 = r"""
df <- mizu::mizu_recv(ch, timeout = 60)
res <- c(
  is.data.frame(df) && nrow(df) == 200000L,
  bit64::is.integer64(df[["l"]]),
  inherits(df[["d"]], "Date"),
  inherits(df[["p"]], "POSIXct") && attr(df[["p"]], "tzone") == "UTC",
  is.factor(df[["f"]]) && identical(levels(df[["f"]]), c("a", "b")),
  sum(is.na(df[["d"]])) == 20000L,
  identical(df[["l"]][1:3], bit64::as.integer64(1:3)),
  identical(df[["d"]][2], as.Date("2020-01-02")),
  identical(df[["f"]][1:3], factor(c("a", "b", "a"), levels = c("a", "b")))
)
mizu::mizu_send(ch, res)
"""


def test_py_peer_pyarrow_frame_kinds(r_mizu):
    # pyarrow: int64 -> integer64, date32 -> Date, timestamp -> POSIXct
    # (tzone kept), dictionary -> factor
    pa = pytest.importorskip("pyarrow")
    import datetime
    n = 200000
    ch = pymizu.Channel.create(R_FRAME_READ2, launcher=r_mizu)
    try:
        ch.send(pa.table({
            "l": pa.array([k + 1 for k in range(n)], type=pa.int64()),
            "d": pa.array([None if k % 10 == 0 else
                           datetime.date(2020, 1, 1) +
                           datetime.timedelta(days=k)
                           for k in range(n)], type=pa.date32()),
            "p": pa.array([datetime.datetime(2021, 1, 1) +
                           datetime.timedelta(seconds=k)
                           for k in range(n)], type=pa.timestamp("us")),
            "f": pa.array([["a", "b"][k % 2] for k in range(n)],
                          type=pa.string()).dictionary_encode(),
        }))
        assert all(ch.recv(60))
    finally:
        ch.close()


R_FRAME_RELAY = r"""
df <- data.frame(i = 1:300000, x = runif(300000),
                 l = bit64::as.integer64(1:300000))
mizu::mizu_send(ch, df)
y <- mizu::mizu_recv(ch, timeout = 60)   # the re-sent frame
rc <- .Call(mizu:::mizu_zc_refcount, y)
mizu::mizu_send(ch, c(rc[[1]] >= 2L, rc[[2]] %% 2L == 1L, identical(y, df)))
"""


def test_py_peer_frame_relay_refs_unmodified(r_mizu):
    # R -> polars -> R of an unmodified fixed-width frame: the return hop
    # is a REF naming R's own region (the provenance record), so R reads a
    # REFHELD view of it, identical() to what it sent
    pl = pytest.importorskip("polars")
    ch = pymizu.Channel.create(R_FRAME_RELAY, launcher=r_mizu)
    try:
        f = ch.recv(60)
        df = pl.DataFrame(f)
        assert ch.send(df) is True
        # the REF emit marked the region (the refcount delta races R's
        # producer-loan reap; the flag is the deterministic read)
        assert f.to_dict()["i"].base.flags & 1 == 1        # REFHELD
        assert all(ch.recv(60))
    finally:
        ch.close()


R_FRAME_RELAY_MOD = r"""
df <- data.frame(i = 1:300000, x = runif(300000))
mizu::mizu_send(ch, df)
y <- mizu::mizu_recv(ch, timeout = 60)
want <- df[-nrow(df), ]
mizu::mizu_send(ch, c(is.data.frame(y), nrow(y) == nrow(want),
                      identical(y, want)))
"""


def test_py_peer_frame_relay_modified_copies(r_mizu):
    # a modified frame (head) fails the record: the MIZL write, the
    # modified values arriving — never the original region
    pl = pytest.importorskip("polars")
    ch = pymizu.Channel.create(R_FRAME_RELAY_MOD, launcher=r_mizu)
    try:
        f = ch.recv(60)
        df = pl.DataFrame(f)
        flags0 = f.to_dict()["i"].base.flags
        assert ch.send(df.head(299999)) is True
        assert f.to_dict()["i"].base.flags == flags0   # no REF emit
        assert all(ch.recv(60))
    finally:
        ch.close()


R_FRAME_RELAY_ONE_COMPUTED = r"""
df <- data.frame(i = 1:300000, x = runif(300000), l = 1:300000)
mizu::mizu_send(ch, df)
y <- mizu::mizu_recv(ch, timeout = 60)
nm <- .Call(mizu:::mizu_zc_view_name, y[["i"]])
rc <- .Call(mizu:::mizu_zc_refcount, y[["i"]])
mizu::mizu_send(ch, c(
  grepl("[", nm, fixed = TRUE),        # a remote leaf over df's own region
  rc[[1]] >= 2L, rc[[2]] %% 2L == 1L,  # counted, REFHELD
  identical(y[["i"]], df[["i"]]),
  identical(y[["l"]], df[["l"]]),
  identical(y[["x"]], 2 * df[["x"]])
))
"""


def test_py_peer_frame_relay_one_computed_remote_leaves(r_mizu):
    # R -> polars (one computed column) -> R, F2: the unmodified columns
    # cross back as remote leaves (directory tag 33) over R's own region
    # — R reads views of it; the computed column a layout leaf
    pl = pytest.importorskip("polars")
    ch = pymizu.Channel.create(R_FRAME_RELAY_ONE_COMPUTED, launcher=r_mizu)
    try:
        f = ch.recv(60)
        df = pl.DataFrame(f).with_columns((pl.col("x") * 2).alias("x"))
        assert ch.send(df) is True
        # the REFHELD store rides the send (deterministic); the values and
        # the aliasing are R's assertions
        assert f.to_dict()["i"].base.flags & 1 == 1
        assert all(ch.recv(60))
    finally:
        ch.close()


R_FRAME_REFS = r"""
df <- data.frame(i = 1:300000, s = rep(c("a", "b", NA), 100000),
                 f = factor(rep(c("u", "v"), 150000)),
                 stringsAsFactors = FALSE)
mizu::mizu_send(ch, df)
y <- mizu::mizu_recv(ch, timeout = 60)
rc <- .Call(mizu:::mizu_zc_refcount, y)
mizu::mizu_send(ch, c(rc[[1]] >= 2L, rc[[2]] %% 2L == 1L, identical(y, df)))
"""


def test_py_peer_frame_resend_refs(r_mizu):
    # the received region-backed Frame re-sent whole is the 3.7 REF route
    # (string and factor columns included): R reads its own region back
    ch = pymizu.Channel.create(R_FRAME_REFS, launcher=r_mizu)
    try:
        f = ch.recv(60)
        assert ch.send(f) is True
        assert all(ch.recv(60))
    finally:
        ch.close()


def test_py_peer_pyarrow_relay_refs_strings(r_mizu):
    # pyarrow hands back every exported buffer, string and dictionary
    # columns included, so its unmodified round trip REFs the whole frame
    pa = pytest.importorskip("pyarrow")
    ch = pymizu.Channel.create(R_FRAME_REFS, launcher=r_mizu)
    try:
        f = ch.recv(60)
        t = pa.table(f)
        assert ch.send(t) is True
        assert f.to_dict()["i"].base.flags & 1 == 1        # REFHELD
        assert all(ch.recv(60))
    finally:
        ch.close()
