"""Auto-imported in spawned pyrei child/worker processes when the test
suite runs under coverage: the repo-root conftest.py puts this directory
on PYTHONPATH and sets COVERAGE_PROCESS_START for subprocesses."""

import os

if os.environ.get("COVERAGE_PROCESS_START"):
    try:
        import coverage

        coverage.process_startup()
    except ImportError:
        pass
