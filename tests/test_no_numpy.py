"""The numpy-less paths: the ``_TAG_MV`` rows serve the memoryview casts
where ``_TAG_NP`` would serve numpy. The suite's environment has numpy, so
these run in a subprocess whose PYTHONPATH shim makes ``import numpy``
raise ImportError — pymizu's optional-numpy detection then falls back in
both the driver and its spawned workers."""

import os
import pathlib
import subprocess
import sys
import textwrap


def _run_without_numpy(tmp_path, script):
    (tmp_path / "numpy.py").write_text(
        'raise ImportError("numpy blocked by test")\n'
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


def test_map_int64_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import array

        import pymizu
        from pymizu import _map
        from tests.helpers import square

        assert _map._np is None  # the shim took; else this covers nothing

        x = array.array("q", range(50))
        p = pymizu.Pool.create(2)
        try:
            # generic collect: workers index the raw x section through
            # _TAG_MV (memoryview.cast("q"))
            assert p.map(square, x) == [i * i for i in range(50)]
            # template collect: _wrap_out's memoryview path for tag 32
            out = p.map(square, x, template=array.array("q", [0]))
            assert type(out) is memoryview and out.format == "q"
            assert list(out) == [i * i for i in range(50)]
        finally:
            p.stop()
        """,
    )
