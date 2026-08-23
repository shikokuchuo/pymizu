"""Task callables for the benchmark.

Pickle sends callables by reference, so the pool workers must import them:
the bench's launcher spawns the workers with the repo root as their
working directory, where ``benchmarks.tasks`` resolves.
"""


def const():
    """The canonical trivial task (R's `1L`)."""
    return 1


def identity(x):
    return x


def bench_sum(i):
    """The fan-out compute task: sum of 1e4 uniform draws (R's
    `sum(runif(1e4))`)."""
    import numpy as np

    return np.random.random(10000).sum()
