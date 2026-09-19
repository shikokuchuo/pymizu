"""Pool worker entry point: ``python -m pymizu.worker <token> <slot>``.

Mirrors the R package's worker_main: joins the pool as its host-assigned
slot (registering the binding's exec callback), runs the worker loop, and
drives the lame-duck linger on MIZU_EXIT_RETIRED — a retired worker lingers
as the lifetime anchor for its uncollected results until shutdown or owner
death ends the linger.
"""

import re
import sys
import time
import traceback

import pymizu
from pymizu import _pymizu

_EXIT_RETIRED = 2


def main() -> int:
    """Run the pool worker; exit 0 on clean shutdown, 1 on an
    infrastructure failure or a task's BaseException (hard crash), 2 on
    bad arguments, join failure, or interrupt."""
    _pymizu._tune_malloc()
    if len(sys.argv) != 3 or not re.fullmatch(
        r"[0-9a-f]+_[0-9a-f]+", sys.argv[1]
    ):
        sys.stderr.write("pymizu.worker: expected a join token and a slot\n")
        return 2
    try:
        slot = int(sys.argv[2])
    except ValueError:
        sys.stderr.write("pymizu.worker: slot must be an integer\n")
        return 2
    try:
        handle = _pymizu._pool_worker_join(sys.argv[1], slot)
    except Exception as exc:
        sys.stderr.write(f"pymizu.worker: join failed: {exc}\n")
        return 2
    pymizu._worker_local.pool = pymizu.Pool._wrap(handle)
    status = 0
    retired = False
    try:
        retired = handle.run() == _EXIT_RETIRED
    except KeyboardInterrupt:
        status = 2
    except BaseException:
        # an infrastructure failure or a task's BaseException (the
        # hard-crash semantics): the worker goes down and the reaper's
        # verdict fails the in-flight task
        traceback.print_exc()
        status = 1
    try:
        handle.leave()
    except Exception:
        pass
    # a retired worker lingers as a lifetime anchor for its uncollected
    # results: plain bounded sleeps, since no unpark can reach a released
    # slot; shutdown or owner death ends the linger
    while retired and not handle.lame_duck():
        time.sleep(1)
    return status


if __name__ == "__main__":
    sys.exit(main())
