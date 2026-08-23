"""pyrei — lock-free shared-memory IPC: SPSC channels and work-stealing
task pools (the Python binding for librei)."""

from pyrei._pyrei import __core_version__, abi_version

__version__ = "0.1.0"

__all__ = ["__core_version__", "__version__", "abi_version"]
