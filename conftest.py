# Present so pytest prepends the repo root to sys.path: the task callables
# import as `tests.helpers`, and the spawned worker processes (whose
# sys.path[0] is the repo root) resolve the same reference.

import os as _os
import pathlib as _pathlib

# When the suite runs under `coverage run`, arm the spawned child/worker
# processes to measure themselves too (they are separate interpreters, so
# the in-process measurement never sees them).
if _os.environ.get("COVERAGE_RUN"):
    _root = _pathlib.Path(__file__).parent
    _os.environ["COVERAGE_PROCESS_START"] = str(_root / "pyproject.toml")
    _site = str(_root / "tests" / "cov_sitecustomize")
    _os.environ["PYTHONPATH"] = (
        _site + _os.pathsep + _os.environ.get("PYTHONPATH", "")
    )
