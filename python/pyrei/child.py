"""Channel peer entry point: ``python -m pyrei.child <token>``.

Mirrors the R package's child script: attaches to the channel named by the
join token, consumes the drop before ready_set, and execs a source-tagged
drop (REI_DROP_SOURCE). Not yet implemented.
"""

import sys


def main() -> int:
    sys.stderr.write("pyrei.child: not yet implemented\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
