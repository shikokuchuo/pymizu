"""The stock-pickle fallback: without cloudpickle, task callables are
importable references only (cloudpickle lifts that when installed). The
suite's environment has cloudpickle, so this runs in a subprocess whose
PYTHONPATH shim makes ``import cloudpickle`` raise ImportError — the
extension's init then falls back to stock pickle in both the driver and
its spawned workers (the test_no_numpy.py pattern)."""

import os
import pathlib
import subprocess
import sys
import textwrap


def _run_without_cloudpickle(tmp_path, script):
    (tmp_path / "cloudpickle.py").write_text(
        'raise ImportError("cloudpickle blocked by test")\n'
    )
    env = {**os.environ, "PYTHONPATH": str(tmp_path)}
    res = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        env=env,
        cwd=pathlib.Path(__file__).parent.parent,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr


def test_submit_without_cloudpickle(tmp_path):
    _run_without_cloudpickle(
        tmp_path,
        """
        import pymizu
        from tests.helpers import identity

        p = pymizu.Pool.create(1)
        try:
            # an importable reference pickles fine under stock pickle
            assert p.submit(identity, 41).collect(timeout=10) == 41
            # a closure does not: stock pickle sends by reference only
            try:
                p.submit(lambda x: x + 1, 1)
            except Exception:
                pass
            else:
                raise AssertionError("a lambda must not pickle")
        finally:
            p.stop()
        """,
    )
