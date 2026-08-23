"""Cross-language round-trips: real R and Python peer processes in both
directions, with real user programs crossing as REI_DROP_SOURCE drops.
Skipped unless Rscript and the installed rei package (with the source-drop
path) are present — the skip_if_no_child_rei() mirror."""

import os
import pathlib
import shutil
import subprocess
import sys

import pytest

import pyrei

RSCRIPT = shutil.which("Rscript")
REPO_PY = str(pathlib.Path(pyrei.__file__).resolve().parent.parent)

# The R echo peer: a real user program crossing as a source drop.
R_ECHO = """
repeat {
  x <- rei::rei_recv(ch, timeout = 30)
  if (inherits(x, "rei_sentinel")) break
  rei::rei_send(ch, x)
}
"""


@pytest.fixture(scope="module")
def r_rei():
    """The shipped R-peer launcher; its probe is the
    skip_if_no_child_rei() mirror."""
    try:
        return pyrei.r_launcher()
    except pyrei.ReiError:
        pytest.skip("Rscript with the rei package (source-drop support) "
                    "not available")


def test_r_launcher_missing_rscript():
    with pytest.raises(pyrei.ReiError):
        pyrei.r_launcher(rscript="/nonexistent/Rscript")


def test_r_peer_echo_roundtrip(r_rei):
    np = pytest.importorskip("numpy")
    ch = pyrei.Channel.create(R_ECHO, launcher=r_rei)
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


def test_r_peer_zero_copy_view(r_rei):
    np = pytest.importorskip("numpy")
    src = """
x <- as.numeric(seq_len(2000000)) + 0   # materialize: an ALTREP sequence
i <- seq_len(2000000) + 0L              # would serialize, never SHM_VEC
rei::rei_send(ch, x)
rei::rei_send(ch, i)
"""
    ch = pyrei.Channel.create(src, launcher=r_rei)
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


def test_r_peer_string_and_na(r_rei):
    src = """
rei::rei_send(ch, "hello world")
rei::rei_send(ch, NA_character_)
"""
    ch = pyrei.Channel.create(src, launcher=r_rei)
    try:
        assert ch.recv(30) == "hello world"
        assert ch.recv(30) is None
    finally:
        ch.close()


def test_r_peer_python_payload_error(r_rei):
    src = """
x <- tryCatch(rei::rei_recv(ch, timeout = 30),
              error = function(e) conditionMessage(e))
rei::rei_send(ch, if (is.character(x)) x else "no error")
"""
    ch = pyrei.Channel.create(src, launcher=r_rei)
    try:
        assert ch.send({1, 2, 3}) is True   # a pickled set
        got = ch.recv(30)
        assert "Python payload" in got
    finally:
        ch.close()


def test_r_host_python_peer(r_rei):
    """The other direction: an R host, a Python peer spawned through
    python -m pyrei.child, assertions on the R side."""
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
