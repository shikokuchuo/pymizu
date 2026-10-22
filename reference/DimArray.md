# DimArray

An `ndarray` carrying R labels ([names](../reference/Frame.names.md#pymizu.Frame.names) / `dimnames` /

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_dimarray.py#L18-L48)

``` python
DimArray()
```

`axis_names` / `r_class`) for faithful R ↔ Python round trips.

A received labeled vector or array homes as a DimArray; an unmodified echo restores every label in R (`identical()`). Attach labels explicitly to send a labeled value: [names](../reference/Frame.names.md#pymizu.Frame.names) for a 1-D array, `dimnames` (plus optional `axis_names`, R's `names(dimnames(x))`) for a matrix or n-D array, and `r_class = "table"` for R's table class. The two label slots are mutually exclusive at send; `axis_names` requires `dimnames` and has one entry per axis.

Labels never propagate implicitly -- any numpy operation that builds a new array (a transpose, arithmetic, a slice, a copy) drops them, so a reshaped or permuted array never inherits labels its data no longer matches.
