"""Channel tests: echo round-trips, batching, spill, and the sentinel
discipline, over real spawned peers (``python -m pyrei.child``)."""

import os
import signal
import subprocess
import sys
import threading

import pytest

import pyrei

ECHO_PEER = """
import pyrei
while True:
    x = ch.recv()
    if x is pyrei.CLOSED or x is pyrei.PEER_GONE:
        break
    ch.send(x)
"""

BATCH_PEER = """
import pyrei
while True:
    xs = ch.recv_batch(16)
    if xs is pyrei.CLOSED or xs is pyrei.PEER_GONE:
        break
    ch.send_batch(xs)
"""


@pytest.fixture
def echo():
    ch = pyrei.Channel.create(ECHO_PEER)
    yield ch
    ch.close()


def test_version():
    assert pyrei.__version__ == "0.1.0.dev0"
    assert pyrei.abi_version() == 1


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
        list(range(100000)),   # past the inline budget: ARENA / SHM_RAW
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
    ch = pyrei.Channel.create(BATCH_PEER)
    try:
        xs = list(range(10))
        assert ch.send_batch(xs) == 10
        assert ch.recv_batch(10, timeout=5) == xs
    finally:
        ch.close()


def test_recv_timeout(echo):
    assert echo.recv(timeout=0.05) is pyrei.TIMEOUT
    assert echo.recv_batch(4, timeout=0.05) is pyrei.TIMEOUT


def test_sentinels():
    for s in (pyrei.FULL, pyrei.TIMEOUT, pyrei.CLOSED, pyrei.PEER_GONE):
        assert pyrei.is_sentinel(s)
        assert not s
        assert repr(s).startswith("pyrei.")
    assert not pyrei.is_sentinel("timeout")
    assert not pyrei.is_sentinel(None)


def test_send_full():
    ch = pyrei.Channel.create("import time; time.sleep(5)", capacity=2)
    try:
        assert ch.send(1) is True
        assert ch.send(2) is True
        assert ch.send(3) is pyrei.FULL
    finally:
        ch._proc.kill()
        assert ch.close() is True   # rendezvous on the peer's death
        ch._proc.wait()


def test_closed_sentinel():
    ch = pyrei.Channel.create("pass")
    try:
        assert ch.recv(timeout=5) is pyrei.CLOSED
        assert ch.send(1) is pyrei.CLOSED
    finally:
        assert ch.close() is True
    assert ch.close() is True   # idempotent
    with pytest.raises(ValueError):
        ch.send(1)


def test_peer_gone():
    ch = pyrei.Channel.create("import os; os._exit(0)")
    try:
        assert ch.recv(timeout=5) is pyrei.PEER_GONE
        assert ch.recv(timeout=5) is pyrei.PEER_GONE   # sticky
        assert not ch.alive()
    finally:
        ch.close()


def test_startup_error():
    def launcher(token):
        return subprocess.Popen([sys.executable, "-c", "pass"])

    with pytest.raises(pyrei.StartupError):
        pyrei.Channel.create("pass", startup_timeout=1.0, launcher=launcher)


def test_info(echo):
    info = echo.info()
    assert info["side"] == "host"
    assert info["capacity"] == 16384
    assert info["slot_size"] == 256
    assert info["inline_max"] == 240
    assert info["ready"] == 1


def test_validation():
    with pytest.raises(ValueError):
        pyrei.Channel.create("pass", capacity=3)
    with pytest.raises(ValueError):
        pyrei.Channel.create("pass", slot_size=100)
    with pytest.raises(TypeError):
        pyrei.Channel.create("")


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
    assert echo.send(1) is True   # the parent's handle is unaffected
    assert echo.recv(timeout=5) == 1


@pytest.mark.skipif(os.name == "nt", reason="SIGINT differs on Windows")
def test_recv_interrupt():
    ch = pyrei.Channel.create("import time; time.sleep(30)")
    timer = threading.Timer(
        0.3, lambda: signal.raise_signal(signal.SIGINT)
    )
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


@pytest.mark.parametrize(
    "dtype", ["float64", "int32", "complex128", "uint8"]
)
def test_numpy_rawvec(echo, dtype):
    a = np.arange(12).astype(dtype)
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert isinstance(b, np.ndarray)
    assert b.dtype == np.dtype(dtype)
    assert np.array_equal(b, a)


def test_numpy_rawspill(echo):
    a = np.arange(200000, dtype=np.float64)   # 1.6 MB: past the inline budget
    assert echo.send(a) is True
    b = echo.recv(timeout=5)
    assert b.dtype == np.float64
    assert np.array_equal(b, a)


def test_numpy_gate_fallbacks(echo):
    # 2-D, non-contiguous, and unmappable dtypes fall to pickle, which
    # preserves shape and dtype exactly
    cases = [
        np.arange(12, dtype=np.float64).reshape(3, 4),
        np.arange(20, dtype=np.float64)[::2],
        np.arange(5, dtype=np.int64),
        np.array([True, False]),
    ]
    for a in cases:
        assert echo.send(a) is True
        b = echo.recv(timeout=5)
        assert b.dtype == a.dtype and b.shape == a.shape
        assert np.array_equal(b, a)
