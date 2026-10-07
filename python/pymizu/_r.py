"""Launching R channel peers (the R package ``mizu``).

`r_launcher()` returns a [`Channel.create()`](`pymizu.Channel.create`)
launcher that spawns the peer as an R process through the package's
static Rscript runner — the same spawn path an R host uses: no
per-spawn command file (Rscript -e writes one), the entry expression
and the probed library paths carried hex-encoded in argv.
"""

from __future__ import annotations

import shutil as _shutil
import subprocess as _subprocess
from collections.abc import Callable as _Callable
from typing import Any as _Any

from pymizu._pymizu import MizuError

# Feature probe: an installed mizu whose mizu_channel takes a source string
# (the MIZU_DROP_SOURCE path). Prints the static runner path and the
# library paths, one per line; exit status 1 when unsupported.
_PROBE = (
    'ok <- requireNamespace("mizu", quietly = TRUE) && '
    'grepl("source string", paste(deparse(mizu::mizu_channel), '
    'collapse = " ")); '
    "if (ok) cat("
    'system.file("scripts", "mizu-child.R", package = "mizu"), "\\n", '
    "paste(.libPaths(), collapse = .Platform$path.sep), "
    '"\\n", sep = ""); '
    "quit(status = !ok)"
)

_cache: dict[str, tuple[str, str]] = {}


def _find_mizu(rscript: str) -> tuple[str, str] | None:
    """(child script, libpaths) of an installed mizu with the source-drop
    path, else None. Successful probes are cached per Rscript."""
    if rscript in _cache:
        return _cache[rscript]
    try:
        probe = _subprocess.run(
            [rscript, "--vanilla", "-e", _PROBE],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, _subprocess.TimeoutExpired):
        return None
    if probe.returncode != 0:
        return None
    lines = probe.stdout.splitlines()
    if len(lines) < 2 or not lines[0]:
        return None
    found = (lines[0], lines[1])
    _cache[rscript] = found
    return found


def _resolve(rscript: str | None, verb: str) -> tuple[str, str, str]:
    """Resolve Rscript and probe it for an installed mizu with the
    source-drop path; MizuError before any channel region exists
    otherwise. Returns (rscript, child script, libpaths)."""
    if rscript is None:
        rscript = _shutil.which("Rscript")
        if rscript is None:
            raise MizuError(
                f"pymizu: {verb}() needs Rscript on the PATH "
                "(or pass rscript=)"
            )
    found = _find_mizu(rscript)
    if found is None:
        raise MizuError(
            f"pymizu: {verb}() needs the R package 'mizu' (with source "
            f"string support) installed for {rscript}"
        )
    script, libs = found
    return rscript, script, libs


def _spawn(
    rscript: str,
    script: str,
    expr: str,
    libs: str,
    stdout: _Any,
    stderr: _Any,
) -> _subprocess.Popen:
    """Spawn the static runner with the entry expression and library
    paths hex-encoded in argv."""
    return _subprocess.Popen(
        [
            rscript,
            script,
            expr.encode("utf-8").hex(),
            libs.encode("utf-8").hex(),
        ],
        stdout=stdout,
        stderr=stderr,
    )


def r_launcher(
    *,
    rscript: str | None = None,
    stdout: _Any = None,
    stderr: _Any = None,
) -> _Callable[[str], _subprocess.Popen]:
    """Return a [`Channel.create()`](`pymizu.Channel.create`) launcher
    spawning an R peer.

    The peer runs the R package `mizu`: the returned `callable(token)`
    spawns `rscript` on the package's static child runner with
    `mizu:::peer_main(<token>)` as the entry expression and the probed
    library paths propagated in argv.

    Parameters
    ----------
    rscript
        Path to Rscript; the default searches the PATH.
    stdout
        Forwarded to `subprocess.Popen`; the default inherits the
        console, where the peer's error epilogue lands.
    stderr
        Forwarded to `subprocess.Popen`.

    Returns
    -------
        A `callable(token)` launcher for
        [`Channel.create()`](`pymizu.Channel.create`).

    Raises
    ------
    MizuError
        Raised here, before the channel is created, when Rscript is
        not found or no installed `mizu` with source string support is
        available.

    """
    rscript, script, libs = _resolve(rscript, "r_launcher")

    def launch(token: str) -> _subprocess.Popen:
        return _spawn(
            rscript,
            script,
            f'mizu:::peer_main("{token}")',
            libs,
            stdout,
            stderr,
        )

    return launch


def r_pool_launcher(
    *,
    rscript: str | None = None,
    stdout: _Any = None,
    stderr: _Any = None,
) -> _Callable[[str, int], _subprocess.Popen]:
    """Return a [`Pool.create()`](`pymizu.Pool.create`) launcher
    spawning R workers.

    Each worker runs the R package `mizu`: the returned
    `callable(token, slot)` spawns `rscript` on the package's static
    child runner with `mizu:::worker_main(<token>, <slot>)` as the
    entry expression and the probed library paths propagated in argv
    (without them the workers cannot `library(mizu)` from the host's
    libraries). The mirror of the R package's `mizu_py_pool_launcher()`.

    Parameters
    ----------
    rscript
        Path to Rscript; the default searches the PATH.
    stdout
        Forwarded to `subprocess.Popen`.
    stderr
        Forwarded to `subprocess.Popen`.

    Returns
    -------
        A `callable(token, slot)` launcher for
        [`Pool.create()`](`pymizu.Pool.create`).

    Raises
    ------
    MizuError
        Raised here, before any pool exists, when Rscript is not found
        or no installed `mizu` with source string support is available.

    Notes
    -----
    The first worker's join records the workers' language in the pool,
    so the launcher carries no language attribute: a pool of R workers
    takes [`pymizu.call()`](`pymizu.call`) specifications through
    [`Pool.submit()`](`pymizu.Pool.submit`), and a plain callable
    errors locally naming the spec verb. A launcher that spawns the
    wrong language fails at join, not at the first task.

    Examples
    --------
    ```python
    import pymizu

    with pymizu.Pool.create(4, launcher=pymizu.r_pool_launcher()) as pool:
        spec = pymizu.call(source="summary(cars$speed)")
        print(pool.submit(spec).collect())
    ```
    """
    rscript, script, libs = _resolve(rscript, "r_pool_launcher")

    def launch(token: str, slot: int) -> _subprocess.Popen:
        return _spawn(
            rscript,
            script,
            f'mizu:::worker_main("{token}",{slot}L)',
            libs,
            stdout,
            stderr,
        )

    return launch
