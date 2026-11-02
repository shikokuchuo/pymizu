"""Package-level tests: the version and the public API surface."""

import pathlib
import re

from packaging.version import Version

import pymizu

DOCS = pathlib.Path(__file__).parent.parent / "docs" / "great-docs.yml"

# the exports with no entry on the API reference page: the sentinel
# singletons are documented under Sentinel, and the version is metadata
UNDOCUMENTED = {"CLOSED", "FULL", "PEER_GONE", "TIMEOUT", "__version__"}


def documented_names():
    # the `- Name` entries under reference.sections[].contents, read
    # without a YAML parser (not a test dependency): the block runs from
    # the top-level `reference:` key to the next top-level line
    text = DOCS.read_text()
    block = re.search(r"^reference:\n(.*?)(?=^\S|\Z)", text, re.M | re.S)
    assert block, "no reference: block in great-docs.yml"
    return set(re.findall(r"^\s+- ([A-Za-z_]\w*)\s*$", block[1], re.M))


def test_version_is_set():
    # "0.0.0" is the PackageNotFoundError fallback: no installed metadata
    assert pymizu.__version__ != "0.0.0"
    Version(pymizu.__version__)  # PEP 440, else InvalidVersion


def test_public_api():
    # changing the public API takes a deliberate edit here
    assert set(pymizu.__all__) == {
        "__version__",
        # channels
        "Channel",
        # task pools
        "Pool",
        "Task",
        "Map",
        "PreparedMap",
        "PoolExecutor",
        # cross-language interop
        "call",
        "launcher",
        "r_launcher",
        "Frame",
        "DimArray",
        # outcomes and errors
        "CLOSED",
        "FULL",
        "PEER_GONE",
        "TIMEOUT",
        "Sentinel",
        "is_sentinel",
        "is_remote_error",
        "MizuError",
        "ShmError",
        "StartupError",
        "SubmitTimeoutError",
        "SlotsExhaustedError",
        "StoppedError",
        "CancelledError",
        "TaskError",
        "WorkerDiedError",
        "TaskLostError",
        "DeclinedError",
        "ForeignPayloadError",
        "ConcurrentUseError",
        # housekeeping
        "default_pool",
        "set_default_pool",
        "using_pool",
        "current_pool",
        "current_rng",
        "prune",
    }


def test_public_api_matches_docs_reference():
    # every export has an entry on the reference page; the docs build
    # catches the reverse (an entry naming something that no longer
    # exists), so a new export lands on the page or fails here
    assert set(pymizu.__all__) - UNDOCUMENTED == documented_names()


def test_all_has_no_duplicates():
    assert len(pymizu.__all__) == len(set(pymizu.__all__))


def test_public_api_names_exist():
    # Map, PreparedMap, and DimArray resolve through the lazy __getattr__,
    # as `from pymizu import *` would resolve them
    for name in pymizu.__all__:
        assert hasattr(pymizu, name), f"{name!r} is in __all__ but undefined"
