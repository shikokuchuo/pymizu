"""Launching R channel peers (the R package ``rei``).

``r_launcher()`` returns a ``Channel.create`` launcher that spawns the
peer as an R process through the package's static Rscript runner — the
same spawn path an R host uses: no per-spawn command file (Rscript -e
writes one), the entry expression and the probed library paths carried
hex-encoded in argv.
"""

from __future__ import annotations

import shutil as _shutil
import subprocess as _subprocess
from collections.abc import Callable as _Callable
from typing import Any as _Any

from pyrei._pyrei import ReiError

# Feature probe: an installed rei whose rei_channel takes a source string
# (the REI_DROP_SOURCE path). Prints the static runner path and the
# library paths, one per line; exit status 1 when unsupported.
_PROBE = (
    'ok <- requireNamespace("rei", quietly = TRUE) && '
    'grepl("source string", paste(deparse(rei::rei_channel), '
    'collapse = " ")); '
    "if (ok) cat("
    'system.file("scripts", "rei-child.R", package = "rei"), "\\n", '
    "paste(.libPaths(), collapse = .Platform$path.sep), "
    '"\\n", sep = ""); '
    "quit(status = !ok)"
)

_cache: dict[str, tuple[str, str]] = {}


def _find_rei(rscript: str) -> tuple[str, str] | None:
    """(child script, libpaths) of an installed rei with the source-drop
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


def r_launcher(
    *,
    rscript: str | None = None,
    stdout: _Any = None,
    stderr: _Any = None,
) -> _Callable[[str], _subprocess.Popen]:
    """Return a ``Channel.create`` launcher spawning an R peer.

    The peer runs the R package ``rei``: the returned ``callable(token)``
    spawns ``rscript`` on the package's static child runner with
    ``rei:::peer_main(<token>)`` as the entry expression and the probed
    library paths propagated in argv. Requires Rscript on the PATH (or
    passed as ``rscript``) and an installed ``rei`` with source string
    support — ReiError is raised here, before the channel is created,
    otherwise. ``stdout`` and ``stderr`` forward to subprocess.Popen;
    the default inherits the console, where the peer's error epilogue
    lands.
    """
    if rscript is None:
        rscript = _shutil.which("Rscript")
        if rscript is None:
            raise ReiError(
                "pyrei: r_launcher() needs Rscript on the PATH "
                "(or pass rscript=)"
            )
    found = _find_rei(rscript)
    if found is None:
        raise ReiError(
            "pyrei: r_launcher() needs the R package 'rei' (with source "
            f"string support) installed for {rscript}"
        )
    script, libs = found

    def launch(token: str) -> _subprocess.Popen:
        expr = f'rei:::peer_main("{token}")'
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

    return launch
