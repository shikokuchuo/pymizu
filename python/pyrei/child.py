"""Channel peer entry point: ``python -m pyrei.child <token>``.

Mirrors the R package's peer_main: attaches to the channel named by the
join token, consumes the drop *before* ready_set (the host holds everything
the drop references alive exactly until ready), execs a source-tagged drop
(REI_DROP_SOURCE) with ``ch`` bound to the peer-side handle, and runs the
peer half of the close protocol as its epilogue.
"""

import re
import sys
import traceback

import pyrei
from pyrei import _pyrei

_DROP_SOURCE = 0x53  # 'S'


def main() -> int:
    if len(sys.argv) != 2 or not re.fullmatch(
        r"[0-9a-f]+_[0-9a-f]+", sys.argv[1]
    ):
        sys.stderr.write("pyrei.child: expected a single join token\n")
        return 2
    try:
        handle, drop = _pyrei._channel_attach(sys.argv[1])
    except Exception as exc:
        sys.stderr.write(f"pyrei.child: attach failed: {exc}\n")
        return 2
    if not drop or drop[0] != _DROP_SOURCE:
        sys.stderr.write(
            "pyrei.child: foreign channel drop (not Python source)\n"
        )
        return 2
    try:
        code = compile(drop[1:].decode("utf-8"), "<pyrei-peer>", "exec")
    except Exception:
        traceback.print_exc()
        handle.close_signal()
        return 2
    handle.ready_set()
    ch = pyrei.Channel._wrap(handle)
    status = 0
    try:
        exec(code, {"__name__": "__pyrei_peer__", "ch": ch})
    except KeyboardInterrupt:
        status = 2
    except BaseException:
        traceback.print_exc()
        status = 1
    handle.close_signal()
    return status


if __name__ == "__main__":
    sys.exit(main())
