"""prune() over the vendored reaper: reclaims /rei_ regions orphaned by
dead creators (a hard-killed process runs no finalizers)."""

import os
import signal
import subprocess
import sys
import time

import pytest

import pyrei

ORPHAN_MAKER = """
import os
import sys
import time

import pyrei

ch = pyrei.Channel.create("import time; time.sleep(300)")
sys.stdout.write(f"{os.getpid()}\\n")
sys.stdout.flush()
time.sleep(300)
"""


def wait_for(pred, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.05)
    return False


def test_prune_returns_empty_when_nothing_to_reap():
    pyrei.prune()  # clear leftovers of earlier crashed runs
    assert pyrei.prune() == []


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Win32 mappings cannot outlive their creator",
)
def test_prune_reaps_orphans_of_hard_killed_process():
    # the child creates a channel (its regions carry the child's PID) and
    # holds it; killing the whole process group leaves no survivor to run
    # the automatic cleanup. The child is reaped by wait() below, so its
    # PID is free for the reaper regardless of the PID-1 reaper.
    proc = subprocess.Popen(
        [sys.executable, "-c", ORPHAN_MAKER],
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        pid = int(proc.stdout.readline())
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()

        marker = f"rei_{pid:x}_"
        reaped = []

        def gone():
            reaped.extend(pyrei.prune())
            return any(marker in name for name in reaped)

        assert wait_for(gone)
        assert pyrei.prune() == []
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
