"""The labeled array: an ndarray carrying R labels for round trips.

[`DimArray`](`pymizu.DimArray`) is the Python home of R's labeled
shapes — a named vector, a matrix or array with `dimnames` (axis names
included), and the `table` class. The labels attach at read and
validate at send; numpy operations drop them rather than propagate
them onto data they no longer describe (the no-propagation doctrine:
`a.T`, `a + 1`, a slice, and `copy` all arrive unlabeled).
"""

from __future__ import annotations

from typing import Any as _Any

import numpy as _np


class DimArray(_np.ndarray):
    """An `ndarray` carrying R labels (`names` / `dimnames` /
    `axis_names` / `r_class`) for faithful R ↔ Python round trips.

    A received labeled vector or array arrives as a DimArray; an
    unmodified echo restores every label in R (`identical()`).
    Attach labels explicitly to send a labeled value: `names` for a
    1-D array, `dimnames` (plus optional `axis_names`, R's
    `names(dimnames(x))`) for a matrix or n-D array, and
    `r_class = "table"` for R's table class. Use either `names` or
    `dimnames`, not both; `axis_names` requires `dimnames` and
    has one entry per axis.

    Labels never carry over on their own — any numpy operation that
    builds a new array (a transpose, arithmetic, a slice, a copy)
    drops them, so a reshaped or permuted array never inherits labels
    its data no longer matches.
    """

    names: tuple[str | None, ...] | None
    dimnames: tuple[tuple[str | None, ...] | None, ...] | None
    axis_names: tuple[str | None, ...] | None
    r_class: str | None

    def __array_finalize__(self, obj: _Any) -> None:
        # Never propagate: a new array from a numpy operation starts
        # unlabeled — labels left behind would mislabel reshaped data.
        self.names = None
        self.dimnames = None
        self.axis_names = None
        self.r_class = None
