"""Channel peer entry point: ``python -m pymizu.child <token>``.

Mirrors the R package's peer_main: attaches to the channel named by the
join token, consumes the drop *before* ready_set (the host holds everything
the drop references alive exactly until ready), execs a source-tagged drop
(MIZU_DROP_SOURCE) with ``ch`` bound to the peer-side handle, and runs the
peer half of the close protocol as its epilogue.
"""

import re
import sys
import traceback

import pymizu
from pymizu import _pymizu

_DROP_SOURCE = 0x53  # 'S'


def main() -> int:
    """Run the channel peer; exit 0 on clean completion, 1 if the peer
    source raised, 2 on bad arguments, attach failure, compile failure,
    or interrupt."""
    _pymizu._tune_malloc()
    if len(sys.argv) != 2 or not re.fullmatch(
        r"[0-9a-f]+_[0-9a-f]+", sys.argv[1]
    ):
        sys.stderr.write("pymizu.child: expected a single join token\n")
        return 2
    try:
        handle, drop = _pymizu._channel_attach(sys.argv[1])
    except Exception as exc:
        sys.stderr.write(f"pymizu.child: attach failed: {exc}\n")
        return 2
    if not drop or drop[0] != _DROP_SOURCE:
        sys.stderr.write(
            "pymizu.child: foreign channel drop (not Python source)\n"
        )
        return 2
    try:
        code = compile(drop[1:].decode("utf-8"), "<pymizu-peer>", "exec")
    except Exception:
        traceback.print_exc()
        handle.close_signal()
        return 2
    handle.ready_set()
    ch = pymizu.Channel._wrap(handle)
    status = 0
    try:
        exec(code, {"__name__": "__pymizu_peer__", "ch": ch})
    except KeyboardInterrupt:
        status = 2
    except BaseException as exc:
        traceback.print_exc()
        # the err stream crosses ahead of the close signal, whatever the
        # peer's language; a full ring drops it (the traceback stands).
        # Exception subclasses only: SystemExit is an orderly exit, not a
        # remote error.
        if isinstance(exc, Exception):
            ch._send_error(exc)
        status = 1
    handle.close_signal()
    return status


if __name__ == "__main__":
    sys.exit(main())
