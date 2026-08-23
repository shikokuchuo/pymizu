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
The extension module stands up the build and packaging only.
The channel and pool verbs land next (see the project plan in the librei repo).

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
