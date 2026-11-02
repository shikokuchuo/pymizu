"""Package-level tests: the version and the public API surface."""

import pymizu


def test_version_is_set():
    # "0.0.0" is the PackageNotFoundError fallback: no installed metadata
    assert pymizu.__version__ != "0.0.0"


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


def test_all_has_no_duplicates():
    assert len(pymizu.__all__) == len(set(pymizu.__all__))


def test_public_api_names_exist():
    # Map, PreparedMap, and DimArray resolve through the lazy __getattr__,
    # as `from pymizu import *` would resolve them
    for name in pymizu.__all__:
        assert hasattr(pymizu, name), f"{name!r} is in __all__ but undefined"
