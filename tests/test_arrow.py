"""Conversion staging and Arrow PyCapsule interop: every fixed-width
numeric dtype crosses to R with correct NA handling — numpy via the
buffer protocol, Arrow producers (pyarrow, polars) via
``__arrow_c_array__`` — and a received view exports to any Arrow consumer
zero-copy. R-side type and NA assertions use a real R peer (the
skip_if_no_child_rei() mirror); the rest use a Python echo peer."""

import ctypes
import gc
import warnings

import pytest
from tests.helpers import ret_arrow_nulls, ret_int64_array

import pyrei

np = pytest.importorskip("numpy", reason="numpy not installed")
pa = pytest.importorskip("pyarrow", reason="pyarrow not installed")

ECHO_PEER = """
import pyrei
while True:
    x = ch.recv()
    if x is pyrei.CLOSED or x is pyrei.PEER_GONE:
        break
    ch.send(x)
"""

# The R checker peer: read a spec string, read a vector, assert its type
# and values (NA positions included), echo it back — or send the error
# message instead of the echo. The int64 specs assert the bit patterns
# (little-endian int64 encodings riding REALSXP storage), so the checker
# needs no bit64 install.
R_CHECK = r"""
i64le <- function(v) writeBin(unclass(v), raw(), size = 8L, endian = "little")
repeat {
  spec <- rei::rei_recv(ch, timeout = 30)
  if (inherits(spec, "rei_sentinel")) break
  x <- rei::rei_recv(ch, timeout = 30)
  ok <- tryCatch({
    switch(spec,
      raw = stopifnot(is.raw(x), identical(x, as.raw(c(0, 127, 255)))),
      int = stopifnot(is.integer(x), identical(x, c(-1L, 2L, 3L))),
      intpos = stopifnot(is.integer(x), identical(x, c(1L, 2L, 3L))),
      dbl = stopifnot(is.double(x), identical(x, c(-1, 2, 3))),
      dblpos = stopifnot(is.double(x), identical(x, c(1, 2, 3))),
      u32max = stopifnot(is.double(x), identical(x, c(0, 1, 4294967295))),
      i64 = stopifnot(
        inherits(x, "integer64"),
        identical(i64le(x), as.raw(c(0xff, 0xff, 0xff, 0xff,
                                    0xff, 0xff, 0xff, 0xff,
                                    2, 0, 0, 0, 0, 0, 0, 0,
                                    3, 0, 0, 0, 0, 0, 0, 0)))),
      i64edge = stopifnot(
        inherits(x, "integer64"),
        identical(i64le(x), as.raw(c(0, 0, 0, 0, 0, 0, 0x20, 0,
                                    0, 0, 0, 0, 0, 0, 0xe0, 0xff)))),
      i64big = stopifnot(
        inherits(x, "integer64"),
        identical(i64le(x), as.raw(c(1, 0, 0, 0, 0, 0, 0x20, 0,
                                    0xff, 0xff, 0xff, 0xff,
                                    0xff, 0xff, 0xdf, 0xff)))),
      i64na24 = stopifnot(
        inherits(x, "integer64"),
        identical(i64le(x), as.raw(c(2, 0, 0, 0, 0, 0, 0, 0,
                                    0, 0, 0, 0, 0, 0, 0, 0x80,
                                    4, 0, 0, 0, 0, 0, 0, 0)))),
      i64na2 = stopifnot(
        inherits(x, "integer64"),
        identical(i64le(x), as.raw(c(1, 0, 0, 0, 0, 0, 0x20, 0,
                                    0, 0, 0, 0, 0, 0, 0, 0x80)))),
      over1 = stopifnot(is.double(x), length(x) == 1L, is.na(x)),
      lgl = stopifnot(is.logical(x), identical(x, c(TRUE, FALSE, TRUE))),
      cplx = stopifnot(is.complex(x), identical(x, c(1+2i, -3+0.5i))),
      intna = stopifnot(is.integer(x), identical(x, c(1L, NA_integer_, 3L))),
      lglena = stopifnot(is.logical(x), identical(x, c(TRUE, NA, FALSE))),
      dblna = stopifnot(is.double(x), identical(x, c(1.5, NA_real_, 3.5))),
      intmin = stopifnot(is.integer(x), length(x) == 3L,
                         is.na(x[1]), identical(x[2:3], c(1L, NA_integer_))),
      int5 = stopifnot(is.integer(x),
                       identical(x, c(10L, 20L, 30L, 40L, 50L))),
      empty_int = stopifnot(is.integer(x), length(x) == 0L),
      empty_dbl = stopifnot(is.double(x), length(x) == 0L),
      stop("unknown spec")
    )
    TRUE
  }, error = function(e) conditionMessage(e))
  rei::rei_send(ch, if (isTRUE(ok)) x else ok)
}
"""


@pytest.fixture
def echo():
    ch = pyrei.Channel.create(ECHO_PEER)
    yield ch
    ch.close()


@pytest.fixture
def pool():
    p = pyrei.Pool.create(2)
    yield p
    p.stop()


@pytest.fixture(scope="module")
def r_rei():
    """The shipped R-peer launcher; its probe is the
    skip_if_no_child_rei() mirror."""
    try:
        return pyrei.r_launcher()
    except pyrei.ReiError:
        pytest.skip("Rscript with the rei package (source-drop support) "
                    "not available")


@pytest.fixture
def rcheck(r_rei):
    ch = pyrei.Channel.create(R_CHECK, launcher=r_rei)
    yield ch
    ch.close()


def r_case(ch, spec, payload):
    assert ch.send(spec) is True
    assert ch.send(payload) is True
    got = ch.recv(timeout=30)
    if isinstance(got, str):
        raise AssertionError(f"R check failed for {spec!r}: {got}")
    return got


def _exporter(arr):
    base = getattr(arr, "base", None)
    while base is not None and not hasattr(base, "refcount"):
        base = getattr(base, "base", None)
    return base


def _settle_producer(ch):
    # the producer's loan on the view's region releases at the peer's
    # first reap after consumer-done — one echo round-trip forces it, so
    # the refcount baseline in the export tests is the view's loan alone
    assert ch.send(0) is True
    assert ch.recv(timeout=5) == 0


# A minimal __arrow_c_array__ producer over (patched) pyarrow capsules, for
# the shapes pyarrow itself will not export (null_count == -1, an
# already-consumed struct, ...).
_lib = ctypes.pythonapi
_lib.PyCapsule_GetPointer.restype = ctypes.c_void_p
_lib.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]


class _ArrowArray(ctypes.Structure):
    _fields_ = [("length", ctypes.c_int64), ("null_count", ctypes.c_int64),
                ("offset", ctypes.c_int64), ("n_buffers", ctypes.c_int64),
                ("n_children", ctypes.c_int64), ("buffers", ctypes.c_void_p),
                ("children", ctypes.c_void_p), ("dictionary", ctypes.c_void_p),
                ("release", ctypes.c_void_p),
                ("private_data", ctypes.c_void_p)]


class _PatchedArray:
    def __init__(self, arr, null_count=None):
        self._caps = arr.__arrow_c_array__()
        if null_count is not None:
            ap = _lib.PyCapsule_GetPointer(self._caps[1], b"arrow_array")
            _ArrowArray.from_address(ap).null_count = null_count

    def __arrow_c_array__(self):
        return self._caps


# -- the numpy matrix (the buffer-protocol front-end) -------------------------


def test_numpy_dtype_matrix(rcheck):
    # every fixed-width numeric dtype crosses; R asserts the type and values
    cases = [
        ("raw", np.array([0, 127, 255], dtype=np.uint8)),
        ("int", np.array([-1, 2, 3], dtype=np.int8)),
        ("int", np.array([-1, 2, 3], dtype=np.int16)),
        ("intpos", np.array([1, 2, 3], dtype=np.uint16)),
        ("int", np.array([-1, 2, 3], dtype=np.int32)),
        ("u32max", np.array([0, 1, 4294967295], dtype=np.uint32)),
        ("i64", np.array([-1, 2, 3], dtype=np.int64)),
        ("dblpos", np.array([1, 2, 3], dtype=np.uint64)),
        ("dbl", np.array([-1, 2, 3], dtype=np.float32)),
        ("dbl", np.array([-1.0, 2.0, 3.0], dtype=np.float64)),
        ("lgl", np.array([True, False, True], dtype=np.bool_)),
        ("cplx", np.array([1 + 2j, -3 + 0.5j], dtype=np.complex64)),
        ("cplx", np.array([1 + 2j, -3 + 0.5j], dtype=np.complex128)),
    ]
    for spec, a in cases:
        r_case(rcheck, spec, a)


def test_numpy_echo_dtypes(rcheck):
    # the echo's dtype reports the R-side wire type (R re-stages what it
    # received): int64 arrives as int64 (R integer64), bool as int32 (R
    # logical reads width-compatibly)
    b = r_case(rcheck, "i64", np.array([-1, 2, 3], dtype=np.int64))
    assert b.dtype == np.int64
    b = r_case(rcheck, "lgl", np.array([True, False, True]))
    assert b.dtype == np.int32
    assert list(b) == [1, 0, 1]


def test_int64_exact_and_sentinel(rcheck):
    # int64 is a native wire type: values past 2^53 cross exactly, no
    # warning (the copy tier is a pure memcpy)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        b = r_case(rcheck, "i64edge",
                   np.array([2**53, -(2**53)], dtype=np.int64))
        assert np.array_equal(b, [2**53, -(2**53)])
        b = r_case(rcheck, "i64big",
                   np.array([2**53 + 1, -(2**53) - 1], dtype=np.int64))
        assert np.array_equal(b, [2**53 + 1, -(2**53) - 1])
        # INT64_MIN is the missing sentinel (documented): a genuine one
        # reads as NA_integer64_ in R and echoes back as INT64_MIN
        b = r_case(rcheck, "i64na2",
                   np.array([2**53 + 1, -(2**63)], dtype=np.int64))
        assert list(b) == [2**53 + 1, -(2**63)]
    # uint64 stays lossy: past 2^53 warns and converts to NA_real_
    with pytest.warns(RuntimeWarning, match="beyond"):
        r_case(rcheck, "over1", np.array([2**53 + 1], dtype=np.uint64))


def test_range_warning_as_error(echo):
    # under warnings-as-errors the warning raises, but only after the write
    # half completed: nothing half-published, the channel stays consistent
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(RuntimeWarning, match="beyond"):
            echo.send(np.array([2**53 + 1], dtype=np.uint64))
    assert echo.send(np.array([1.0, 2.0])) is True
    assert np.array_equal(echo.recv(timeout=5), [1.0, 2.0])


def test_prefixed_buffer_formats(echo):
    # ctypes exports a '<'-prefixed format ('<i', or '<l' on Windows):
    # '='/'<' byte-order prefixes convert (every supported platform is
    # little-endian); '>'/'!' still fall to pickle
    buf = (ctypes.c_int32 * 3)(7, 8, 9)
    assert memoryview(buf).format in ("<i", "<l")
    assert echo.send(buf) is True
    assert list(echo.recv(timeout=5)) == [7, 8, 9]


# -- Arrow import -------------------------------------------------------------


def test_arrow_import_nulls(rcheck):
    b = r_case(rcheck, "intna", pa.array([1, None, 3], type=pa.int32()))
    assert b[1] == -2147483648  # the NA sentinel is visible on the echo
    r_case(rcheck, "lglena", pa.array([True, None, False]))
    b = r_case(rcheck, "dblna", pa.array([1.5, None, 3.5], type=pa.float64()))
    assert np.isnan(b[1])


def test_arrow_masked_int64(rcheck):
    # values cross exactly, the null lane writes INT64_MIN (reads as
    # NA_integer64_ in R) — no warning: the copy tier is a pure memcpy
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        b = r_case(rcheck, "i64na24", pa.array([2, None, 4], type=pa.int64()))
        assert list(b) == [2, -(2**63), 4]
        # an out-of-range valid value under a mask: exact, still no warning
        b = r_case(rcheck, "i64na2",
                   pa.array([2**53 + 1, None], type=pa.int64()))
        assert list(b) == [2**53 + 1, -(2**63)]


def test_arrow_masked_intmin(rcheck):
    # a genuine INT_MIN under a mask reads as NA_integer_ in R: warn once
    with pytest.warns(RuntimeWarning, match="-2147483648"):
        b = r_case(rcheck, "intmin",
                   pa.array([-2147483648, 1, None], type=pa.int32()))
    assert list(b) == [-2147483648, 1, -2147483648]


def test_arrow_sliced(rcheck):
    # offset != 0: an element offset into the data buffer, a bit offset
    # into the validity bitmap
    r_case(rcheck, "intpos",
           pa.array([0, 1, 2, 3, 99], type=pa.int32()).slice(1, 3))
    r_case(rcheck, "intna",
           pa.array([0, 1, None, 3, 99], type=pa.int32()).slice(1, 3))
    # a sliced bool: the data buffer is bit-packed too — the offset is a
    # bit offset into both bitmaps
    r_case(rcheck, "lglena",
           pa.array([False, True, None, False, True]).slice(1, 3))


def test_arrow_empty(rcheck):
    r_case(rcheck, "empty_int", pa.array([], type=pa.int32()))
    r_case(rcheck, "empty_int", pa.array([1, 2, 3], type=pa.int32()).slice(3))
    r_case(rcheck, "empty_dbl", pa.array([], type=pa.float64()))


def test_arrow_null_count_unknown(rcheck):
    # null_count == -1 ("not yet computed") with a non-NULL bitmap takes
    # the masked path (pyarrow computes the count eagerly, so patch the
    # exported struct to reconstruct the shape)
    data = np.array([10, 20, 30, 40, 50], dtype=np.int32)
    arr = pa.Array.from_buffers(
        pa.int32(), 5,
        [pa.py_buffer(bytes([0b00011111])), pa.py_buffer(data.tobytes())],
        null_count=-1,
    )
    b = r_case(rcheck, "int5", _PatchedArray(arr, null_count=-1))
    assert list(b) == [10, 20, 30, 40, 50]


def test_arrow_rejections(echo):
    # uint8 with nulls: RAWSXP has no NA
    with pytest.raises(TypeError, match="raw vectors have no NA"):
        echo.send(pa.array([1, None], type=pa.uint8()))
    # strings and temporal: unsupported formats
    with pytest.raises(TypeError, match="unsupported Arrow format"):
        echo.send(pa.array(["a", "b"]))
    with pytest.raises(TypeError, match="unsupported Arrow format"):
        echo.send(pa.array([1, 2], type=pa.timestamp("s")))
    # nested
    with pytest.raises(TypeError, match="nested"):
        echo.send(pa.array([[1, 2], [3]]))
    # dictionary-encoded: presents the index format ("i") — reject, or the
    # indices would silently import as values
    with pytest.raises(TypeError, match="dictionary-encoded"):
        echo.send(pa.array([1, 2, 1]).dictionary_encode())
    # stream-only producers: name the remedy
    with pytest.raises(TypeError, match="combine_chunks"):
        echo.send(pa.chunked_array([[1, 2], [3]]))
    with pytest.raises(TypeError, match="combine_chunks"):
        echo.send(pa.table({"a": [1, 2]}))


def test_arrow_invalid_capsules(echo):
    # an already-consumed struct has release == NULL (the spec's move
    # semantics): the first send consumes it, the replay rejects
    p = _PatchedArray(pa.array([1, 2, 3], type=pa.int32()))
    assert echo.send(p) is True
    assert list(echo.recv(timeout=5)) == [1, 2, 3]
    with pytest.raises(TypeError, match="invalid Arrow"):
        echo.send(p)
    # null_count < -1 rejects
    with pytest.raises(TypeError, match="invalid Arrow"):
        echo.send(_PatchedArray(pa.array([1, 2, 3], type=pa.int32()),
                                null_count=-2))
    # an exception from the dunder itself propagates
    class Boom:
        def __arrow_c_array__(self):
            raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        echo.send(Boom())


# -- the channel gate ------------------------------------------------------


def test_pool_results_keep_pickle(pool):
    # the conversion pass is channel-scoped: pool results are
    # Python-both-ends by construction and round-trip losslessly (int64
    # on the memcpy raw tier, an Arrow table on the pickle path)
    out = pool.submit(ret_int64_array).collect(timeout=30)
    assert out.dtype == np.int64
    assert list(out) == [1, 2, 3]
    out = pool.submit(ret_arrow_nulls).collect(timeout=30)
    assert out.null_count == 1
    assert out.to_pylist() == [1, None, 3]


def test_py_py_channel_normalizes(echo):
    # a channel peer's language is unknowable at stage time: non-wire
    # dtypes convert on Py->Py channels too (int64 is a wire type and
    # crosses exactly; uint64 still converts). The escape hatch for an
    # exact Py->Py send of a converting dtype: nest the array in a
    # container (keeps the pickle path)
    assert echo.send(np.array([1, 2, 3], dtype=np.int64)) is True
    assert echo.recv(timeout=5).dtype == np.int64
    assert echo.send(np.array([1, 2, 3], dtype=np.uint64)) is True
    assert echo.recv(timeout=5).dtype == np.float64
    assert echo.send((np.array([1, 2, 3], dtype=np.uint64),)) is True
    assert echo.recv(timeout=5)[0].dtype == np.uint64
    # Arrow nulls convert to the NA sentinel, indistinguishable from a
    # genuine INT_MIN on the Python side
    assert echo.send(pa.array([1, None, 3], type=pa.int32())) is True
    assert list(echo.recv(timeout=5)) == [1, -2147483648, 3]


# -- round-trip fidelity ---------------------------------------------------


def test_na_real_echo_bit_exact(r_rei):
    # an untouched received view re-stages as untouched bytes: the NA_real_
    # payload survives the relay (R distinguishes it from a plain NaN)
    src = """
x <- as.numeric(seq_len(40000)) + 0   # past the zc floor: a view crosses
x[2] <- NA_real_
x[3] <- NaN
rei::rei_send(ch, x)
y <- rei::rei_recv(ch, timeout = 30)
ok <- tryCatch({
  stopifnot(is.double(y), identical(y, x))
  TRUE
}, error = function(e) conditionMessage(e))
rei::rei_send(ch, if (isTRUE(ok)) "OK" else ok)
"""
    ch = pyrei.Channel.create(src, launcher=r_rei)
    try:
        v = ch.recv(timeout=30)
        assert isinstance(v, np.ndarray) and v.dtype == np.float64
        assert np.isnan(v[1]) and np.isnan(v[2])
        assert ch.send(v) is True
        assert ch.recv(timeout=30) == "OK"
    finally:
        ch.close()


def test_na_real_under_compute(r_rei):
    # Python-side compute treats the NA sentinel as a NaN value; whether
    # the payload itself survives is platform-dependent (IEEE NaN payload
    # propagation), so R-side only is.na() is locked, not NA vs NaN
    src = """
x <- as.numeric(seq_len(40000)) + 0
x[2] <- NA_real_
rei::rei_send(ch, x)
y <- rei::rei_recv(ch, timeout = 30)
ok <- tryCatch({
  stopifnot(is.double(y), is.na(y[2]), identical(y[1], 1))
  TRUE
}, error = function(e) conditionMessage(e))
rei::rei_send(ch, if (isTRUE(ok)) "OK" else ok)
"""
    ch = pyrei.Channel.create(src, launcher=r_rei)
    try:
        v = ch.recv(timeout=30)
        with np.errstate(invalid="ignore"):
            computed = v + 0
        assert ch.send(computed) is True
        assert ch.recv(timeout=30) == "OK"
    finally:
        ch.close()


def test_logical_relay_loses_tag(r_rei):
    # R logical exports to Python as int32 on both surfaces (Arrow bool is
    # bit-packed: no zero-copy): the tag does not survive the relay, the
    # values (0/1, INT_MIN for NA) do
    src = """
rei::rei_send(ch, c(TRUE, FALSE, NA))
y <- rei::rei_recv(ch, timeout = 30)
ok <- tryCatch({
  stopifnot(is.integer(y), identical(y, c(1L, 0L, NA_integer_)))
  TRUE
}, error = function(e) conditionMessage(e))
rei::rei_send(ch, if (isTRUE(ok)) "OK" else ok)
"""
    ch = pyrei.Channel.create(src, launcher=r_rei)
    try:
        v = ch.recv(timeout=30)
        assert v.dtype == np.int32
        assert ch.send(v) is True
        assert ch.recv(timeout=30) == "OK"
    finally:
        ch.close()


# -- Arrow export (the view's __arrow_c_array__) ---------------------------


def test_export_roundtrip(echo):
    a = np.arange(100000, dtype=np.float64)  # past the zc floor: a view
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    view = _exporter(b)
    assert view is not None
    arr = pa.array(view)  # pyarrow consumes the dunder
    assert arr.type == pa.float64()
    assert len(arr) == 100000
    assert arr.null_count == 0
    assert arr[0].as_py() == 0.0 and arr[-1].as_py() == 99999.0


def test_export_types(echo):
    # one Arrow representation per wire type
    cases = [
        (np.arange(100000, dtype=np.float64), pa.float64()),
        (np.arange(100000, dtype=np.int32), pa.int32()),
        (bytes(range(256)) * 2000, pa.uint8()),  # past the channel raw floor
    ]
    for payload, ptype in cases:
        assert echo.send(payload) is True
        view = _exporter(echo.recv(timeout=5))
        arr = pa.array(view)
        assert arr.type == ptype
        assert len(arr) * arr.type.bit_width // 8 == len(
            memoryview(payload).cast("B"))


def test_export_lifetime(echo):
    # the export holds its own mapping and its own zc loan: +1 on the
    # region refcount, back to baseline after the consumer releases
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    view = _exporter(b)
    _settle_producer(echo)
    rc0 = view.refcount
    caps = view.__arrow_c_array__()
    assert view.refcount == rc0 + 1
    arr = pa.Array._import_from_c_capsule(*caps)  # consumes (moves) the pair
    del caps
    gc.collect()
    assert view.refcount == rc0 + 1  # the consumer holds the loan now
    del arr
    gc.collect()
    assert view.refcount == rc0


def test_export_unconsumed_gc(echo):
    # an export never consumed: the capsule destructors call release
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    view = _exporter(echo.recv(timeout=5))
    _settle_producer(echo)
    rc0 = view.refcount
    caps = view.__arrow_c_array__()
    assert view.refcount == rc0 + 1
    del caps
    gc.collect()
    assert view.refcount == rc0


def test_export_outlives_view(echo):
    # the export's own mapping pins the pages after the view is gone
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    view = _exporter(echo.recv(timeout=5))
    caps = view.__arrow_c_array__()
    del view
    gc.collect()
    arr = pa.Array._import_from_c_capsule(*caps)
    assert arr[0].as_py() == 0.0 and arr[-1].as_py() == 99999.0


def test_export_requested_schema_ignored(echo):
    # an unsupported-but-compatible request gets the default export (the
    # spec's sanctioned fallback); the requested capsule is borrowed
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    view = _exporter(echo.recv(timeout=5))
    f32 = pa.array([1.0], type=pa.float32()).__arrow_c_array__()[0]
    arr = pa.Array._import_from_c_capsule(
        *view.__arrow_c_array__(requested_schema=f32))
    assert arr.type == pa.float64()


def test_export_complex_rejected(echo):
    a = np.arange(100000, dtype=np.complex128)
    assert echo.send(a) is True
    view = _exporter(echo.recv(timeout=5))
    with pytest.raises(TypeError, match="complex"):
        view.__arrow_c_array__()


def test_export_r_logical_and_na(r_rei):
    # R logical exports as int32 (Arrow bool is bit-packed); R's NA
    # sentinels arrive as visible values with null_count == 0 (documented)
    src = """
rei::rei_send(ch, rep(c(TRUE, FALSE, NA), length.out = 100000))
rei::rei_send(ch, rep(c(1L, NA), length.out = 100000))
"""
    ch = pyrei.Channel.create(src, launcher=r_rei)
    try:
        view = _exporter(np.asarray(ch.recv(timeout=30)))
        arr = pa.array(view)
        assert arr.type == pa.int32()
        assert arr[:3].to_pylist() == [1, 0, -2147483648]
        assert arr.null_count == 0
        view = _exporter(np.asarray(ch.recv(timeout=30)))
        arr = pa.array(view)
        assert arr.type == pa.int32()
        assert arr[:2].to_pylist() == [1, -2147483648]
        assert arr.null_count == 0
    finally:
        ch.close()


def test_export_polars_smoke(echo):
    pl = pytest.importorskip("polars", reason="polars not installed")
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    view = _exporter(echo.recv(timeout=5))
    # the Arrow-array entry point for a flat producer (from_arrow builds a
    # DataFrame and demands a struct)
    s = pl.Series(view)
    assert s.len() == 100000
    assert s[0] == 0.0 and s[-1] == 99999.0


def test_view_without_numpy():
    # the no-numpy fallback: the received object is the view itself — a
    # read-only buffer exporter that also carries __arrow_c_array__ (not a
    # memoryview)
    import subprocess
    import sys

    script = r"""
import sys
sys.modules["numpy"] = None   # the import probe fails, the view stays bare
import pyrei

ch = pyrei.Channel.create('''
import pyrei
while True:
    x = ch.recv()
    if x is pyrei.CLOSED or x is pyrei.PEER_GONE:
        break
    ch.send(x)
''')
ch.send(bytes(range(256)) * 2000)   # past the raw floor: a view crosses
v = ch.recv(timeout=30)
assert type(v).__name__ == "_ShmView", type(v)
mv = memoryview(v)                  # buffer-compatible
assert len(mv) == 512000 and mv[0] == 0 and mv[-1] == 255
import pyarrow as pa
arr = pa.array(v)                   # the dunder is reachable without numpy
assert arr.type == pa.uint8() and len(arr) == 512000
ch.close()
print("OK")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "OK" in proc.stdout
