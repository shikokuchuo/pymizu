"""Task callables for the benchmark.

Pickle sends callables by reference, so the pool workers must import them:
the bench's launcher spawns the workers with the repo root as their
working directory, where ``benchmarks.tasks`` resolves.
"""

import numpy as np

# Fixed stand-in for the R benchmark's `ggplot2::diamonds$price` (53,940
# reals): generated once at import, so each worker loads it once and a map
# payload carries only the window index.
PRICES = np.random.default_rng(42).random(53_940) * 5000


def const():
    """The canonical trivial task (R's `1L`)."""
    return 1


def identity(x):
    return x


def winsum(i):
    """The map task: mean and sd of a sliding 1,000-wide window over
    PRICES (R's `winsum` over diamond prices)."""
    w = PRICES[i : i + 1000]
    return (w.mean(), w.std())


def winsum_t(i):
    """winsum as a length-2 float64 buffer (the template-map spelling)."""
    w = PRICES[i : i + 1000]
    return np.array([w.mean(), w.std()])


def plus_one(v):
    """The trivial map task (R's `function(v) v + 1`): in the overhead
    regime the per-element cost is the whole story."""
    return v + 1


def skewed(i):
    """The skew-regime task: 1% of elements (clustered at the head) cost
    ~100x the rest (R's `sum(runif(if (i <= 40) 2e5 else 200))`)."""
    return np.random.random(200_000 if i <= 40 else 200).sum()
