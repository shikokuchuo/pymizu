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
        assert ch._h._peer_ident() == (2, 5)  # MIZU_LANG_R, MIZS | MIZL
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
        # shift); the return array keeps the sentinel in place until the
        # copied-read rule lands (3.2)
        a = np.array([1, -2147483648, 3], dtype=np.int32)
        assert ch.send(a) is True
        got = ch.recv(30)
        assert got.dtype == np.int32 and got[1] == -2147483648
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
# integer with NA is exact until 3.2's copied-read rule: RAWVEC INT
# reads as int32 with the sentinel in place
report2(c(1L, NA, -3L), c(1L, NA, -3L))
report2(c(1L, NA), c(1L, NA))
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
