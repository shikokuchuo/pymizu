"""Channel tests: echo round-trips, batching, spill, and the sentinel
discipline, over real spawned peers (``python -m pymizu.child``)."""

import gc
import os
import shutil
import signal
import subprocess
import sys
import threading

import pytest
from tests.helpers import StrSubclass

import pymizu

ECHO_PEER = """
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(x)
"""

BATCH_PEER = """
import pymizu
while True:
    xs = ch.recv_batch(16)
    if xs is pymizu.CLOSED or xs is pymizu.PEER_GONE:
        break
    ch.send_batch(xs)
"""


@pytest.fixture
def echo():
    ch = pymizu.Channel.create(ECHO_PEER)
    yield ch
    ch.close()


def test_version():
    assert pymizu.__version__ == "0.1.0.dev0"
    assert pymizu.abi_version() == 1


def test_echo_roundtrip(echo):
    payloads = [
        None,
        True,
        42,
        3.14,
        "hello",
        "héllo",
        [1, "a", None],
        {"k": (1, 2)},
        list(range(100000)),  # past the inline budget: ARENA / SHM_RAW
    ]
    for x in payloads:
        assert echo.send(x) is True
        assert echo.recv(timeout=5) == x


def test_codec_roundtrip(echo):
    # the compact-codec subset: scalars and one flat container level
    payloads = [
        True,
        False,
        0,
        -1,
        2**63 - 1,
        -(2**63),
        3.14,
        -0.0,
        float("inf"),
        "",
        "hello",
        "héllo ☃",
        [],
        (),
        {},
        [1, -2, 3],
        (True, 1, 2.5, "x", b"y"),
        ["a"] * 64,  # at the container cap
        {"k": 1, 2: "v", 3.5: (b"z",)[0]},
        list(range(1000)),  # codec stream past the inline budget
    ]
    for x in payloads:
        assert echo.send(x) is True
        out = echo.recv(timeout=5)
        assert out == x
        assert type(out) is type(x)


def test_str1_roundtrip(echo):
    # Python str stages as STR1 within the inline budget (UTF-8 payload,
    # cetype aux) — the mirror of R's length-1 string tier
    for x in ["", "hello", "héllo ☃", "x" * 4096]:
        assert echo.send(x) is True
        out = echo.recv(timeout=5)
        assert out == x
        assert type(out) is str


def test_str_fallbacks(echo):
    # past the inline budget a str rides the codec; a str subclass keeps
    # its pickle semantics (importable from tests.helpers: stock pickle
    # sends it by reference, and the echo peer must import it too)
    big = "abc123 ☃" * 5000  # ~45 KB, past the inline budget
    assert echo.send(big) is True
    assert echo.recv(timeout=5) == big
    sub = StrSubclass("subclass")
    assert echo.send(sub) is True
    out = echo.recv(timeout=5)
    assert out == sub
    assert type(out) is StrSubclass


def test_codec_fallback(echo):
    # outside the subset: rides pickle, still round-trips
    payloads = [
        None,
        2**63,  # past int64
        [1, [2]],  # nested container
        {"k": {"v": 1}},
        [None, 1],  # None is not a codec scalar
        list(range(65)),  # over the container cap
        "\ud800",  # lone surrogate: no UTF-8 encoding
    ]
    for x in payloads:
        assert echo.send(x) is True
        assert echo.recv(timeout=5) == x


def test_bytes_roundtrip(echo):
    data = bytes(range(256)) * 16
    assert echo.send(data) is True
    out = echo.recv(timeout=5)
    # bytes ride the raw tier and arrive as a uint8 array (numpy) or a
    # memoryview copy — width-compatible with RAWSXP by design
    assert bytes(out) == data


def test_send_recv_batch():
    ch = pymizu.Channel.create(BATCH_PEER)
    try:
        xs = list(range(10))
        assert ch.send_batch(xs) == 10
        assert ch.recv_batch(10, timeout=5) == xs
    finally:
        ch.close()


def test_recv_timeout(echo):
    assert echo.recv(timeout=0.05) is pymizu.TIMEOUT
    assert echo.recv_batch(4, timeout=0.05) is pymizu.TIMEOUT


def test_sentinels():
    for s in (pymizu.FULL, pymizu.TIMEOUT, pymizu.CLOSED, pymizu.PEER_GONE):
        assert pymizu.is_sentinel(s)
        assert not s
        assert repr(s).startswith("pymizu.")
    assert not pymizu.is_sentinel("timeout")
    assert not pymizu.is_sentinel(None)


def test_send_full():
    ch = pymizu.Channel.create("import time; time.sleep(5)", capacity=2)
    try:
        assert ch.send(1) is True
        assert ch.send(2) is True
        assert ch.send(3) is pymizu.FULL
    finally:
        ch._proc.kill()
        assert ch.close() is True  # rendezvous on the peer's death
        ch._proc.wait()


def test_closed_sentinel():
    ch = pymizu.Channel.create("pass")
    try:
        assert ch.recv(timeout=5) is pymizu.CLOSED
        assert ch.send(1) is pymizu.CLOSED
    finally:
        assert ch.close() is True
    assert ch.close() is True  # idempotent
    with pytest.raises(ValueError):
        ch.send(1)


def test_peer_gone():
    ch = pymizu.Channel.create("import os; os._exit(0)")
    try:
        assert ch.recv(timeout=5) is pymizu.PEER_GONE
        assert ch.recv(timeout=5) is pymizu.PEER_GONE  # sticky
        assert not ch.alive()
    finally:
        ch.close()


def test_startup_error():
    def launcher(token):
        return subprocess.Popen([sys.executable, "-c", "pass"])

    with pytest.raises(pymizu.StartupError):
        pymizu.Channel.create("pass", startup_timeout=1.0, launcher=launcher)


def test_info(echo):
    info = echo.info()
    assert info["side"] == "host"
    assert info["capacity"] == 16384
    assert info["slot_size"] == 256
    assert info["inline_max"] == 240
    assert info["ready"] == 1


def test_validation():
    with pytest.raises(ValueError):
        pymizu.Channel.create("pass", capacity=3)
    with pytest.raises(ValueError):
        pymizu.Channel.create("pass", slot_size=100)
    with pytest.raises(TypeError):
        pymizu.Channel.create("")


@pytest.mark.skipif(os.name == "nt", reason="no fork on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fork_guard(echo):
    pid = os.fork()
    if pid == 0:
        # the child's copy of the handle must refuse every verb
        try:
            echo.send(1)
        except Exception:
            os._exit(0)
        os._exit(1)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert echo.send(1) is True  # the parent's handle is unaffected
    assert echo.recv(timeout=5) == 1


@pytest.mark.skipif(os.name == "nt", reason="SIGINT differs on Windows")
def test_recv_interrupt():
    ch = pymizu.Channel.create("import time; time.sleep(30)")
    timer = threading.Timer(0.3, lambda: signal.raise_signal(signal.SIGINT))
    timer.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            ch.recv()
    finally:
        timer.cancel()
        ch._proc.kill()
        ch.close()
        ch._proc.wait()


# -- numpy (the raw tiers) ----------------------------------------------------

np = pytest.importorskip("numpy", reason="numpy not installed")

from tests.helpers import CarryArray  # noqa: E402


@pytest.mark.parametrize("dtype", ["float64", "int32", "complex128", "uint8"])
def test_numpy_rawvec(echo, dtype):
    a = np.arange(12).astype(dtype)
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert isinstance(b, np.ndarray)
    assert b.dtype == np.dtype(dtype)
    assert np.array_equal(b, a)


def test_numpy_rawspill(echo):
    a = np.arange(200000, dtype=np.float64)  # 1.6 MB: past the inline budget
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert b.dtype == np.float64
    assert np.array_equal(b, a)


def test_numpy_gate_fallbacks(echo):
    # 2-D, non-contiguous, and unmappable dtypes fall to pickle, which
    # preserves shape and dtype exactly. (Mappable-but-non-identity
    # dtypes — uint64, bool — convert instead: see test_arrow.py.)
    cases = [
        np.arange(12, dtype=np.float64).reshape(3, 4),
        np.arange(20, dtype=np.float64)[::2],
    ]
    for a in cases:
        assert echo.send(a) is True
        b = echo.recv(timeout=5)
        assert b.dtype == a.dtype and b.shape == a.shape
        assert np.array_equal(b, a)
    # a big-endian array is never misread as native: it falls to pickle,
    # and numpy's pickle normalizes the byte order (values stay correct)
    a = np.arange(5, dtype=np.int16).astype(">i2")
    assert echo.send(a) is True
    assert np.array_equal(echo.recv(timeout=5), a)


def test_ndarray_subclasses_keep_pickle(echo):
    # a strict ndarray subclass leaves the buffer branch for pickle: the
    # raw tier would carry the base buffer only — a MaskedArray would lose
    # its mask, a units-carrying subclass its attribute
    m = np.ma.MaskedArray([1.0, 2.0, 3.0], mask=[True, False, False])
    assert echo.send(m) is True
    r = echo.recv(timeout=5)
    assert isinstance(r, np.ma.MaskedArray)
    assert list(r.mask) == [True, False, False]
    assert list(r.data) == [1.0, 2.0, 3.0]

    c = CarryArray([1.0, 2.0], tag="kept")
    assert echo.send(c) is True
    r = echo.recv(timeout=5)
    assert type(r) is CarryArray
    assert r.tag == "kept"
    assert list(r) == [1.0, 2.0]


def test_memmap_roundtrips_as_memmap(echo, tmp_path):
    # its pickle yields a memmap (numpy embeds the data), so the subclass
    # rejection costs it nothing against raw staging
    a = np.memmap(tmp_path / "m.dat", dtype=np.float64, mode="w+", shape=4)
    a[:] = [1.5, 2.5, 3.5, 4.5]
    a.flush()
    assert echo.send(a) is True
    r = echo.recv(timeout=5)
    assert isinstance(r, np.memmap)
    assert list(r) == [1.5, 2.5, 3.5, 4.5]


def test_exact_ndarray_stays_zero_copy(echo):
    # the gate rejects subclasses only: an exact ndarray past the floor
    # still arrives as a view over the shared pages
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert type(b) is np.ndarray
    assert _exporter(b) is not None
    assert np.array_equal(b, a)


# -- zero-copy views (SHM_VEC) ----------------------------------------------


def _exporter(arr):
    base = getattr(arr, "base", None)
    while base is not None and not hasattr(base, "refcount"):
        base = getattr(base, "base", None)
    return base


def test_shm_vec_roundtrip(echo):
    # past MIZU_ZC_FLOOR_RAW the copy tiers give way to a zero-copy view
    a = np.arange(100000, dtype=np.float64)  # 800 KB
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert isinstance(b, np.ndarray)
    assert b.dtype == np.float64
    assert np.array_equal(b, a)
    assert not b.flags.writeable
    assert _exporter(b) is not None  # a view over the shared pages, not a copy


def test_shm_vec_bytes(echo):
    a = bytes(range(256)) * 2000  # 512 KB
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert bytes(b) == a
    assert _exporter(b) is not None


def test_shm_vec_refcount(echo):
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    exp = _exporter(b)
    # our view's loan; the producer's drops at its next reap point
    assert exp.refcount >= 1
    del b, exp
    gc.collect()
    assert echo.send(a) is True  # the region recycles; the peer lives on
    assert np.array_equal(echo.recv(timeout=5), a)


def test_shm_vec_small_stays_copy(echo):
    a = np.arange(1000, dtype=np.float64)  # 8 KB: below the zc floor
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert np.array_equal(b, a)
    assert _exporter(b) is None  # a plain copy


REF_VARIANTS_PEER = """
import numpy as np
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(x[1:])  # a slice: by value
    ch.send(x.view(np.int32))  # a dtype view: by value
    ch.send(x.base)  # the _ShmView itself: REF
"""


def test_view_echo_crosses_by_reference(echo):
    # a received view sent on whole is a REF — the region's name, no
    # payload bytes — and the peer marks the region REFHELD first
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    exp = _exporter(b)
    assert exp is not None
    assert exp.flags & 1
    assert np.array_equal(b, a)


def test_view_slice_and_dtype_view_go_by_value():
    ch = pymizu.Channel.create(REF_VARIANTS_PEER)
    try:
        a = np.arange(100000, dtype=np.float64)
        assert ch.send(a) is True
        sl = ch.recv(timeout=5)
        dv = ch.recv(timeout=5)
        whole = ch.recv(timeout=5)
        assert np.array_equal(sl, a[1:])
        assert np.array_equal(dv, a.view(np.int32))
        assert np.array_equal(whole, a)
        # the copies land in fresh regions of the peer's own; only the
        # whole re-send names the sent region, now REFHELD
        assert not (_exporter(sl).flags & 1)
        assert not (_exporter(dv).flags & 1)
        assert _exporter(whole).flags & 1
    finally:
        ch.close()


# -- the consumer-side view cache -------------------------------------------


def test_view_cache_recycled_name(echo):
    # a released region rejoins the producer's free list and reuses its
    # name: repeat reads hit the cached mapping and must see new content
    for i in range(10):
        a = np.full(100000, i, dtype=np.float64)
        assert echo.send(a) is True
        b = echo.recv(timeout=5)
        assert _exporter(b) is not None
        assert np.array_equal(b, a)
        del b
        gc.collect()


def test_view_cache_eviction_with_live_views(echo):
    # more distinct live regions than MIZU_OPEN_CACHE_MAX (16): evicted
    # owners keep their mappings until the last view is gone
    xs = [np.full(100000, i, dtype=np.float64) for i in range(20)]
    views = []
    for a in xs:
        assert echo.send(a) is True
        views.append(echo.recv(timeout=5))
    for a, b in zip(xs, views, strict=True):
        assert np.array_equal(b, a)
        if sys.platform != "linux":
            assert _exporter(b) is not None
    # Linux: holding this many live views trips the producer's churn flag
    # (a free-list miss with an unreclaimable lent ledger), and staging
    # falls back to the copy tiers by design — the view assertion above
    # would fail on the copies, so it is macOS/Windows-only


def test_view_survives_channel_destroy(echo):
    a = np.arange(100000, dtype=np.float64)
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert _exporter(b) is not None
    echo.destroy()
    assert np.array_equal(b, a)  # the live view pins the mapping


def test_r_codec_stream_dispatch():
    # mizu's compact codec magic is 'R' (0x52): the read side declines it
    # with the informative foreign-payload error, not "unrecognized"
    with pytest.raises(pymizu.MizuError, match="R payload"):
        pymizu._pymizu._read_stream(b"R")


def test_unpickle_failure_is_consumed():
    # unpickling raises a plain Exception on the host — a content failure:
    # the read consumes the slot, so the next message arrives instead of
    # the ring wedging behind the decline
    ch = pymizu.Channel.create(
        "from tests.helpers import FailOnUnpickle\n"
        "ch.send('a')\n"
        "ch.send(FailOnUnpickle())\n"
        "ch.send('c')\n"
    )
    try:
        assert ch.recv(timeout=10) == "a"
        with pytest.raises(ValueError, match="unpickle failure"):
            ch.recv(timeout=10)
        assert ch.recv(timeout=10) == "c"
    finally:
        ch.close()


def test_control_flow_raise_keeps_the_slot():
    # unpickling raises SystemExit (the BaseException control-flow tier,
    # which describes the process, never the message bytes): never
    # consumed — the same failure reproduces on the next receive
    ch = pymizu.Channel.create(
        "from tests.helpers import ExitOnUnpickle\n"
        "ch.send('a')\n"
        "ch.send(ExitOnUnpickle())\n"
        "ch.send('c')\n"
    )
    try:
        assert ch.recv(timeout=10) == "a"
        with pytest.raises(SystemExit):
            ch.recv(timeout=10)
        with pytest.raises(SystemExit):
            ch.recv(timeout=10)
    finally:
        # no orderly close: a control-flow raise from payload code is a
        # sender bug; the exclusion serves the transient case (a real
        # Ctrl-C whose retry succeeds)
        ch.destroy()


def test_recv_batch_keeps_prefix():
    # a read failure past the batch's first message ends it with the
    # consumed prefix; the next receive reproduces the failure and
    # consumes, and the one after reads on. send_batch's single tail store
    # publishes both messages atomically, so the batch provably reaches
    # the failing read.
    ch = pymizu.Channel.create(
        "from tests.helpers import FailOnUnpickle\n"
        "ch.send_batch(['a', FailOnUnpickle()])\n"
        "ch.send('c')\n"
    )
    try:
        assert ch.recv_batch(4, timeout=10) == ["a"]
        with pytest.raises(ValueError, match="unpickle failure"):
            ch.recv(timeout=10)
        assert ch.recv(timeout=10) == "c"
    finally:
        ch.close()


def test_recv_batch_interrupt_propagates():
    # a BaseException-only raise out of the unpickle is not cleared with
    # the prefix: the batch propagates it (the prefix is lost), and the
    # unconsumed slot reproduces it on the next receive
    ch = pymizu.Channel.create(
        "from tests.helpers import ExitOnUnpickle\n"
        "ch.send_batch(['a', ExitOnUnpickle()])\n"
        "ch.send('c')\n"
    )
    try:
        with pytest.raises(SystemExit):
            ch.recv_batch(4, timeout=10)
        with pytest.raises(SystemExit):
            ch.recv(timeout=10)
    finally:
        ch.destroy()


# -- R interop (the view tier against an mizu peer) --------------------------

_RSCRIPT = shutil.which("Rscript")


def _r_available():
    if _RSCRIPT is None:
        return False
    return (
        subprocess.run(
            [_RSCRIPT, "-e", "library(mizu)"],
            capture_output=True,
            check=False,
        ).returncode
        == 0
    )


r_only = pytest.mark.skipif(
    not _r_available(), reason="R with mizu not installed"
)

_R_ECHO = """
repeat {
  x <- mizu_recv(ch, timeout = 30)
  if (inherits(x, "mizu_sentinel")) break
  mizu_send(ch, x)
}
"""


def _r_channel(expr_src):
    # the drop is the peer's bootstrap expression as an MIZU_DROP_R-tagged
    # serialize stream; the launcher runs the R-side peer entry
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".rds", delete=False) as f:
        path = f.name
    try:
        subprocess.run(
            [
                _RSCRIPT,
                "-e",
                f'con <- file("{path}", "wb");'
                f" serialize(quote({expr_src}), con, xdr = FALSE); close(con)",
            ],
            capture_output=True,
            check=True,
        )
        with open(path, "rb") as f:
            drop = f.read()
    finally:
        os.unlink(path)
    h = pymizu._pymizu._channel_new(
        16384, 256, 4 * 1024 * 1024, False, b"R" + drop
    )
    proc = subprocess.Popen([_RSCRIPT, "-e", f'mizu:::peer_main("{h.token}")'])
    if not h.ready_wait(30):
        h.destroy()
        proc.kill()
        raise pymizu.StartupError("pymizu: R peer failed to attach")
    ch = pymizu.Channel._wrap(h)
    ch._proc = proc
    return ch


@r_only
def test_r_interop_shm_vec_echo():
    # Python produces SHM_VEC; R receives a view and re-sends it, which
    # crosses back as REF — resolved to a view over the same region
    ch = _r_channel(_R_ECHO)
    try:
        a = np.arange(1000000, dtype=np.float64)  # 8 MB
        assert ch.send(a) is True
        b = ch.recv(timeout=10)
        assert isinstance(b, np.ndarray)
        assert np.array_equal(b, a)
        assert _exporter(b) is not None
    finally:
        ch.close()


@r_only
def test_r_interop_view_cache_sharing():
    # R re-sends its view twice: both cross as REF naming the same region,
    # so the second read hits the view cache — one mapping, one address
    ch = _r_channel(
        "{ x <- mizu_recv(ch, timeout = 30)\n"
        "  mizu_send(ch, x)\n"
        "  mizu_send(ch, x)\n" + _R_ECHO + " }"
    )
    try:
        a = np.arange(1000000, dtype=np.float64)
        assert ch.send(a) is True
        b1 = ch.recv(timeout=10)
        b2 = ch.recv(timeout=10)
        assert _exporter(b1) is not None and _exporter(b2) is not None
        assert np.array_equal(b1, a) and np.array_equal(b2, a)
        assert b1.ctypes.data == b2.ctypes.data
    finally:
        ch.close()


@r_only
def test_r_interop_r_produces():
    # R produces SHM_VEC (a plain vector stages as a layout region)
    ch = _r_channel(
        "{ mizu_send(ch, cumsum(rep(1.0, 1000000)))\n" + _R_ECHO + " }"
    )
    try:
        b = ch.recv(timeout=10)
        assert isinstance(b, np.ndarray)
        assert b.dtype == np.float64
        assert np.array_equal(b, np.arange(1, 1000001, dtype=np.float64))
        assert _exporter(b) is not None
    finally:
        ch.close()


@r_only
def test_r_interop_named_vector_declined_at_send():
    # a named atomic vector has no portable home: the interchange writer
    # raises at R send time (the identity exchange tells R what pymizu
    # can read), and the R side reports the classed decline
    src = (
        "{ y <- cumsum(rep(1.0, 1000000));"
        " names(y) <- paste0('n', seq_along(y));"
        " e <- tryCatch({ mizu_send(ch, y); 'no error' },"
        "   error = function(e) conditionMessage(e));"
        " mizu_send(ch, e)\n"
        + _R_ECHO
        + " }"
    )
    ch = _r_channel(src)
    try:
        got = ch.recv(timeout=10)
        assert "not portable" in got and "named atomic vector" in got
    finally:
        ch.close()


@r_only
def test_r_interop_list_crosses():
    # an R list is the interchange stream now ('I' magic): it crosses to a
    # Python list, no longer a declined "R payload"
    ch = _r_channel("{ mizu_send(ch, list(1L, 2.5, 'x'))\n" + _R_ECHO + " }")
    try:
        assert ch.recv(timeout=10) == [1, 2.5, "x"]
    finally:
        ch.close()


@r_only
def test_r_interop_string_vector_crosses():
    # a character vector past the zero-copy floor: pymizu declares
    # MIZU_CAP_MIZS, so R stages MIZS — a region-backed string view whose
    # to_list() is the explicit copy (below the floor, the strv copy)
    ch = _r_channel(
        "{ mizu_send(ch, rep('x', 100000))\n" + _R_ECHO + " }"
    )
    try:
        v = ch.recv(timeout=10)
        assert type(v).__name__ == "_ShmStrView"
        assert v.to_list() == ["x"] * 100000
    finally:
        ch.close()


def test_peer_error_sends_an_err_stream_before_exit():
    ch = pymizu.Channel.create("ch.send(1.5)\nraise ValueError('boom')\n")
    try:
        assert ch.recv(timeout=10) == 1.5
        v = ch.recv(timeout=10)
        assert pymizu.is_remote_error(v)
        assert v.remote_type == "ValueError"
        assert str(v) == "ValueError: boom"
        # the traceback truncates to its share of the inline budget
        assert v.remote_traceback.startswith(
            "Traceback (most recent call last):"
        )
        assert len(v.remote_traceback.encode()) <= 201
        assert ch.recv(timeout=10) is pymizu.CLOSED
    finally:
        ch.close()


def test_peer_system_exit_sends_no_error_value():
    ch = pymizu.Channel.create("import sys\nch.send(1.5)\nsys.exit(3)\n")
    try:
        assert ch.recv(timeout=10) == 1.5
        assert ch.recv(timeout=10) is pymizu.CLOSED
    finally:
        ch.close()
