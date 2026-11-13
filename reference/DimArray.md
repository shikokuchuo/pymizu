# DimArray

An `ndarray` carrying R labels.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_dimarray.py#L10-L39)

``` python
DimArray()
```

A received labeled vector or array arrives as a DimArray. An unmodified echo restores every label in R (`identical()`). The labels are `names`, `dimnames`, `axis_names`, and `r_class`.

To send a labeled value, set `names` on a 1-D array. Set `dimnames` on a matrix or n-D array, with optional `axis_names` for R's `names(dimnames(x))`. For R's table class, set `r_class = "table"`. Use either `names` or `dimnames`, not both. `axis_names` requires `dimnames` and has one entry per axis.

Numpy operations that build a new array drop all labels. This includes transposes, arithmetic, slices, and copies. A new array never inherits labels that no longer match its data.
