"""Facade-level tests: constructor and argument validation, warning
branches, context managers, the low-level attach path, and the
spawned-process entry points' error branches."""

import re
import subprocess
import sys

import pytest
from tests.helpers import square

import pymizu
from pymizu import _pymizu

ECHO_PEER = """
import pymizu
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(x)
"""

# A peer that attaches through the low-level facade path itself, the way
# ``python -m pymizu.child`` does: consume the drop, then ready_set.
ATTACH_PEER = """
import sys

import pymizu

ch = pymizu.Channel.attach(sys.argv[1])
ch.drop
ch._h.ready_set()
while True:
    x = ch.recv()
    if x is pymizu.CLOSED or x is pymizu.PEER_GONE:
        break
    ch.send(x)
ch._h.close_signal()
"""


def run_module(module, *args):
    return subprocess.run(
        [sys.executable, "-m", module, *args],
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_channel_direct_init():
    with pytest.raises(TypeError, match="Channel.create"):
        pymizu.Channel()


def test_pool_direct_init():
    with pytest.raises(TypeError, match="Pool.create"):
        pymizu.Pool()


def test_channel_attach_bad_token():
    with pytest.raises(ValueError, match="malformed join token"):
        pymizu.Channel.attach("nope")


def test_pool_attach_bad_token():
    with pytest.raises(ValueError, match="malformed join token"):
        pymizu.Pool.attach("nope!")


def test_channel_attach_peer():
    def launch(token):
        return subprocess.Popen([sys.executable, "-c", ATTACH_PEER, token])

    ch = pymizu.Channel.create("pass", launcher=launch)
    try:
        ch.send("ping")
        assert ch.recv() == "ping"
    finally:
        ch.close()


def test_channel_token_alive_info():
    ch = pymizu.Channel.create(ECHO_PEER)
    try:
        assert re.fullmatch(r"[0-9a-f]+_[0-9a-f]+", ch.token)
        assert ch.alive()
        assert isinstance(ch.info(), dict)
    finally:
        ch.close()


def test_channel_context_manager():
    with pymizu.Channel.create(ECHO_PEER) as ch:
        ch.send("x")
        assert ch.recv() == "x"


def test_channel_close_signal_destroy():
    ch = pymizu.Channel.create(ECHO_PEER)
    ch.close_signal()
    assert ch.recv(timeout=5) is pymizu.CLOSED
    ch.destroy()


def test_channel_close_timeout_warns():
    ch = pymizu.Channel.create("import time; time.sleep(30)")
    try:
        with pytest.warns(UserWarning, match="close timed out"):
            assert ch.close(timeout=0.2) is False
    finally:
        ch.destroy()
        if ch._proc is not None:
            ch._proc.kill()


def test_pool_create_validation():
    with pytest.raises(ValueError, match="at least 1"):
        pymizu.Pool.create(0)
    with pytest.raises(ValueError, match="exceeds max_workers"):
        pymizu.Pool.create(2, max_workers=1)
    # no int() coercion: a float count fails at the C boundary
    with pytest.raises(TypeError):
        pymizu.Pool.create(2.5)


def test_pool_submit_validation():
    with pymizu.Pool.create(1) as pool:
        with pytest.raises(TypeError, match="must be callable"):
            pool.submit(42)
        with pytest.raises(TypeError, match="must be callable"):
            pool.submit_batch([square, 42])


def test_pool_spawn_workers_validation():
    with pymizu.Pool.create(1) as pool:
        with pytest.raises(ValueError, match="at least 1"):
            pool.spawn_workers(0)
        with pytest.raises(pymizu.MizuError, match="free worker slots"):
            pool.spawn_workers(1)


def test_pool_status_dump():
    with pymizu.Pool.create(1) as pool:
        assert isinstance(pool.status(), dict)
        assert isinstance(pool.dump(), dict)


def test_current_pool_outside_task():
    assert pymizu.current_pool() is None


def test_child_entry_bad_args():
    assert run_module("pymizu.child").returncode == 2
    assert run_module("pymizu.child", "not-a-token").returncode == 2


def test_child_entry_attach_failed():
    r = run_module("pymizu.child", "deadbeef_1")
    assert r.returncode == 2
    assert "attach failed" in r.stderr


def test_child_entry_foreign_drop():
    h = _pymizu._channel_new(1024, 256, 1 << 20, False, b"\x00raw")
    try:
        r = run_module("pymizu.child", h.token)
        assert r.returncode == 2
        assert "foreign channel drop" in r.stderr
    finally:
        h.destroy()


def test_child_entry_compile_error():
    with pytest.raises(pymizu.StartupError):
        pymizu.Channel.create("def (:", startup_timeout=2.0)


def test_child_entry_peer_exception():
    ch = pymizu.Channel.create("raise RuntimeError('boom')")
    try:
        assert ch.recv(timeout=5) in (pymizu.CLOSED, pymizu.PEER_GONE)
    finally:
        ch.destroy()


def test_worker_entry_bad_args():
    assert run_module("pymizu.worker").returncode == 2
    assert run_module("pymizu.worker", "deadbeef_1", "abc").returncode == 2


def test_worker_entry_join_failed():
    r = run_module("pymizu.worker", "deadbeef_1", "0")
    assert r.returncode == 2
    assert "join failed" in r.stderr
