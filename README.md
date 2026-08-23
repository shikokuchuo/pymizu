# pyrei れい

Lock-free shared-memory channels and task pools for Python.

pyrei moves data between Python processes on the same machine.
It provides lock-free channels and work-stealing task pools over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).
The hot path stays entirely in user space.
Communication between processes becomes cheap enough that you can divide work at granularities usually reserved for threads.

pyrei is the Python binding for [librei](https://github.com/shikokuchuo/librei).
The extension compiles the vendored C core, so no system library is necessary.
A channel can also connect a Python process to an R process that uses the `rei` package.

Pre-release (v0.1.0).
Channels are implemented; the task-pool binding lands next (see the project plan in the librei repo).

## Channels

```python
import pyrei

ch = pyrei.Channel.create("""
import pyrei
while True:
    x = ch.recv()
    if x is pyrei.CLOSED:
        break
    ch.send(x)
""")
ch.send([1, "a", None])
print(ch.recv(timeout=5))
ch.close()
```

`Channel.create()` spawns a peer process (`python -m pyrei.child <token>`) and connects both ends over a lock-free ring pair.
The peer program is a Python source string, evaluated with `ch` bound to the peer-side handle.
Sends never block for ring space.
Receives report terminal states as sentinel singletons — `pyrei.FULL`, `pyrei.TIMEOUT`, `pyrei.CLOSED`, `pyrei.PEER_GONE` — tested by identity (`x is pyrei.TIMEOUT`), never raised.
`None` crosses as an immediate.
`bytes` and 1-D contiguous numpy arrays of float64, int32, complex128, or uint8 ride a serialization-free raw tier (they arrive as arrays; `bytes` arrives as uint8).
Everything else crosses as a pickle protocol 4 stream.
A channel can also connect a Python process to an R process that uses the `rei` package (R sends numeric vectors and strings; Python reads them as arrays and `str`).

## Requirements

- Python 3.10 or later, on a 64-bit platform.
- Linux: kernel 5.3 or later (`pidfd_open`, no fallback).
- Windows: the build uses clang-cl.
  MSVC does not support the C11 atomics that the core needs.
  `setup.py` forces clang-cl through a `build_ext` override.

## Install

```sh
pip install .
```

Optional extras:

- `pyrei[numpy]`: zero-copy array views and raw-tier array staging.
- `pyrei[cloudpickle]`: lambdas, closures, and local functions as pool tasks.

## Layout

- `src/_pyrei.c`: the extension module.
  It uses the raw CPython C API (no pybind11, Cython, or cffi).
- `src/vendor/librei/`: the vendored librei core.
  `tools/vendor-librei.sh` generates this directory.
  Do not edit these files by hand.
- `python/pyrei/`: the Python package.
  `child.py` and `worker.py` are the entry points for spawned processes (`python -m pyrei.child <token>`, `python -m pyrei.worker <suffix> <slot>`).
- `tests/`: the pytest suite.
  If `Rscript` and the installed `rei` package are not present, the cross-language tests skip.

## License

MIT.
The vendored librei core carries third-party RngStreams attribution (upstream `LICENSE.note`).
