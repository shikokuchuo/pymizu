"""Pool worker entry point: ``python -m pyrei.worker <suffix> <slot>``.

Mirrors the R package's worker_main: joins the pool, runs the worker loop
with the binding's exec callback, and drives the lame-duck linger on
REI_EXIT_RETIRED. Not yet implemented.
"""

import sys


def main() -> int:
    sys.stderr.write("pyrei.worker: not yet implemented\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
