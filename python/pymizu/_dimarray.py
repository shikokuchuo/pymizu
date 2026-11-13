"""An `ndarray` subclass for R dimension and element labels."""

from __future__ import annotations

from typing import Any as _Any

import numpy as _np


class DimArray(_np.ndarray):
    """An `ndarray` carrying R labels.

    A received labeled vector or array arrives as a DimArray. An unmodified
    echo restores every label in R (`identical()`). The labels are `names`,
    `dimnames`, `axis_names`, and `r_class`.

    To send a labeled value, set `names` on a 1-D array. Set `dimnames` on a
    matrix or n-D array, with optional `axis_names` for R's
    `names(dimnames(x))`. For R's table class, set `r_class = "table"`. Use
    either `names` or `dimnames`, not both. `axis_names` requires `dimnames`
    and has one entry per axis.

    Numpy operations that build a new array drop all labels. This includes
    transposes, arithmetic, slices, and copies. A new array never inherits
    labels that no longer match its data.
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
