# Contributing

Contributions are welcome -- bug reports and pull requests alike. pymizu is pre-release and the API can change at any time, so open an issue to discuss larger changes before doing the work.


# Reporting bugs

Open a GitHub issue with a minimal reproducer, your platform, and your Python version. For a pool that hangs, include the output of `pool.dump()` -- it is the first diagnostic tool.


# Pull requests

Set up and run the checks before submitting:

``` sh
pip install -e .
python -m pytest tests/
ruff check python tests benchmarks
pyrefly check
```

`pre-commit install` wires the hygiene hooks plus `ruff-check --fix` and `ruff-format` into each commit.

Conventions:

- Commit messages are a single-line subject, no body.
- New behavior needs a test. Task callables used in pool tests belong in `tests/helpers.py`, not the test module -- pickle sends them by reference, so spawned workers must import them.
- Never edit `src/vendor/libmizu/` by hand. Changes go upstream to [libmizu](https://github.com/shikokuchuo/libmizu) and land here via `tools/vendor-libmizu.sh`.
- Cross-language tests skip unless `Rscript` and the R `mizu` package are installed.


# Repository layout

- `src/_pymizu.c`: the extension module. It uses the raw CPython C API (no pybind11, Cython, or cffi).
- `src/vendor/libmizu/`: the vendored libmizu core. `tools/vendor-libmizu.sh` generates this directory. Do not edit these files by hand.
- `python/pymizu/`: the Python package. `child.py` and `worker.py` are the entry points for spawned processes (`python -m pymizu.child <token>`, `python -m pymizu.worker <suffix> <slot>`).
- `tests/`: the pytest suite. `tests/helpers.py` holds the task callables (pickle sends them by reference, so the workers must import them).
- `docs/`: the documentation site, built with [Great Docs](https://posit-dev.github.io/great-docs/). `great-docs build` from the repo root builds it (config in `docs/great-docs.yml`, guides in `docs/user_guide/`).


# Code of conduct

This project is released with a [Contributor Code of Conduct](https://github.com/shikokuchuo/pymizu/blob/main/.github/CODE_OF_CONDUCT.md). By participating, you agree to abide by its terms.


# License

By contributing, you agree that your contributions are licensed under the project's [MIT license](LICENSE).
