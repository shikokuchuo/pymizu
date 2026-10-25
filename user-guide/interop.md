# R interop

The process at the other end of a channel does not have to be Python. [mizu](https://github.com/shikokuchuo/mizu) is the R binding of the same shared-memory core, and a channel or pool connects the two languages as directly as two Python processes.

## R channel peers

A channel peer can be an R process with the `mizu` package installed. Pass the peer program as R source, and set the launcher to [pymizu.r_launcher()](../reference/r_launcher.md#pymizu.r_launcher):

``` python
import pymizu

ch = pymizu.Channel.create(
    """
library(mizu)
repeat {
  x <- mizu_recv(ch, timeout = 30)
  if (inherits(x, "mizu_sentinel")) break
  mizu_send(ch, x)
}
""",
    launcher=pymizu.r_launcher(),
)

import numpy as np
ch.send(np.array([1.5, 2.5, 3.5]))   # arrives in R as a numeric vector
print(ch.recv(timeout=5))            # echoes back as a float64 array
ch.close()
```

[r_launcher()](../reference/r_launcher.md#pymizu.r_launcher) needs R and the `mizu` R package installed. If either is missing, it raises [MizuError](../reference/MizuError.md#pymizu.MizuError) before the channel is created. Pass `rscript=` to use a specific R installation.

To spawn the peer differently, write your own launcher: one callable that takes the channel's token and starts the peer process. The reverse direction works too: an R session spawns a Python peer with `mizu::mizu_py_launcher()`.

The exchange is easiest to take in tiers, from the everyday to the involved: most values cross as an ordinary copy and round-trip unchanged, past a size floor larger data crosses without copying at all, and a handful of types have no faithful home on the other side and are declined. A closing fine-print section collects the few round-trip normalizations and what your R collaborator sees.

## Everyday values round-trip

Most Python objects cross as an ordinary copy -- converted once, at send time -- and round-trip exactly. R sees the natural equivalent -- an R vector for an array, a named list for a dict -- and whatever R sends back arrives as the natural Python object:

- Python scalars (`bool`, `int`, `float`, `complex`, `str`, `None`), lists, and dicts with `str` keys cross both ways; a `tuple` arrives as a `list` in R.
- A numpy array of any fixed-width numeric dtype arrives as the matching R vector, and an R vector arrives as a numpy array -- the [dtype matrix](#the-dtype-matrix) has the exact per-dtype correspondence. `bytes` arrives as an R raw vector.
- Strings cross both ways: R's `NA_character_` arrives as `None`, and a string vector as `list[str | None]`.
- An Arrow array arrives as an R vector, Arrow nulls as R missing values: `ch.send(pa.array([1, None, 3]))`. Anything with `__arrow_c_array__` works (pyarrow, polars, a duckdb result column).
- An Arrow stream -- a polars or pandas DataFrame, a pyarrow Table or ChunkedArray, a polars Series, a duckdb relation, anything with [\_\_arrow_c_stream\_\_](../reference/Frame.__arrow_c_stream__.md#pymizu.Frame.__arrow_c_stream__) -- arrives as an R `data.frame`. Dictionary-encoded columns become factors, `date32`/`timestamp` columns become `Date`/`POSIXct`, and a Series arrives as a plain vector.
- Dates and times keep their meaning: a `Date` crosses as `datetime64[D]` or Arrow `date32`, a `POSIXct` as naive `datetime64[us]` or `timestamp[us]` (`NaT` read as `NA`), and a `difftime` as `timedelta64[us]`. `datetime.date` and `datetime.datetime` scalars arrive as the length-1 forms.
- A factor crosses as `list[str | None]` on its own, or as a dictionary-encoded column inside a frame.
- Labels cross with the values they name: a named R vector arrives as a [`DimArray`](../reference/DimArray.md#pymizu.DimArray) carrying its names, a matrix or array with its `dimnames` -- axis names and the `table` class included -- and labels you attach in Python ride to R the same way. An unmodified echo is `identical()` in R. Names ride on the plain atomic types; the exceptions are listed in what doesn't cross.

The few cases where a round trip normalizes the container (never the values) are listed in the fine print.

## Large data crosses without copying

Past a size floor, the data itself never moves: it is written to shared memory once, and both sides read it in place -- no copy, no parsing. Everything you *send* large is read by R over the shared pages, and everything you *receive* large arrives as a view over them -- usually wrapped as a numpy array:

- A large numpy array of an identity dtype (`float64`, `int32`, `int64`, `complex128`, `uint8`), and a large R vector coming back. Without numpy, a received view is a buffer exporter: `memoryview(view)` works, and any Arrow consumer wraps the same pages zero-copy through the Arrow PyCapsule protocol -- `pa.array(view)`, `pl.from_arrow(view)`.
- A large array of any shape crosses as a view too, in any memory order -- C order, Fortran order, or strided -- with `dim` set on the R side (a matrix is the 2-D case). A large `datetime64` array arrives in R as a `Date` or `POSIXct` view, `NaT` as `NA`.
- A large `list[str | None]` arrives in R as a character-vector view, `None` as `NA`.
- A large nested tree of lists, dicts, arrays, and strings arrives in R as a list view over one shared region (dict keys become the list's names).
- A large data frame crosses as one shared-memory region in both directions -- see [`pymizu.Frame`](#pymizu.frame).

The floors: a flat contiguous vector on a channel crosses as a view from 256 KB -- below that it takes one quick copy into the channel's spill arena, cheaper than region machinery at that size. Everywhere else -- pools, string lists, frames, arrays with `dim` -- the floor is 32 KB. Arrays of non-identity dtypes (booleans, the narrow ints, `uint64`, `float32`, …) cross as a converted copy at any size; the [dtype matrix](#the-dtype-matrix) has the per-dtype rule.

Views compose: a received view sent back crosses as its region's name alone -- zero payload bytes -- and R resolves a view of the original region. A received view nested inside a tree crosses by reference the same way, while a slice or dtype view of one crosses by value.

An unmodified frame echoed back moves no data at all, and this holds through polars or pyarrow (`pl.from_arrow(f)`, `pa.table(f)`) as long as no column was modified; modify some columns and only those columns cross as data, the rest still by reference, so a relay that adds one computed column pays for one column. A polars or pyarrow object that wraps a received frame keeps the frame's region alive until it is deleted and garbage-collected -- holding many at once can force later sends back to copying, so delete them when they are no longer needed.

One exception: R's ALTREP vectors (say, `1:1e9`, which R stores as a compact recipe rather than data) cross by value. Values stay exact, but the compact representation is not preserved.

## What doesn't cross

A value with no faithful home on the other side is declined at send time with [pymizu.DeclinedError](../reference/DeclinedError.md#pymizu.DeclinedError) (a subclass of `TypeError`), its `path` and `reason` attributes naming what was rejected and why. The channel stays usable.

- From Python: Arrow nested, decimal, `date64`, `time`, and `interval` columns, and ordered dictionaries -- cast a polars `Enum` to `pl.Categorical` first. Arrow `uint8` *with nulls* is a `TypeError`: an R raw vector has no `NA`.
- From R: `POSIXlt`, a character matrix, and S4 objects -- and a named vector with a class or of character type (a named character vector, factor, `Date`, or `POSIXct`), standalone or as a frame column, since names ride only on the plain atomic vectors. A dict of strings crosses fine as the labeled alternative. Among data frames: data.table, a groupedData-style frame (a formula attribute), an ordered-factor column, and a column that is itself a labeled matrix.

## The fine print

A round trip can normalize the container, never the values:

- A standalone R factor comes back as strings (inside a data frame it stays a factor).
- A length-1 `NA` arrives as `None`; sent back, it is R's `NULL`.
- An R logical vector with `NA`s comes back as integer -- `c(TRUE, NA)` returns as `c(1L, NA)` -- numpy having no missing-aware bool dtype.
- A `POSIXct` timezone normalizes to UTC on a standalone vector (the instant is exact); a named timezone is kept as frame-column metadata. Naive Python datetimes arrive in R with `tzone = "UTC"`.
- `difftime` units normalize to seconds: R's five units cross by value, and a minute comes back as 60 seconds.
- `-2147483648` in an int32 array and `-9223372036854775808` in an int64 array collide with R's own `NA` sentinels and read as `NA` in R; a `uint64` value past 2^53 crosses as `NA` with a warning at send.

And for what your R collaborator sees on their side:

- An array arrives as an R vector of the matching type -- `float64` as `numeric`, `int32` as `integer`, `uint8` as `raw`, `int64` as `integer64` -- and an array with `dim` as a matrix or array.
- Labels arrive as [names](../reference/Frame.names.md#pymizu.Frame.names) on a vector and `dimnames` on a matrix or array. NumPy operations drop a [DimArray](../reference/DimArray.md#pymizu.DimArray)'s labels rather than risk mislabeling reshaped data, so a modified array arrives as a plain array -- re-attach labels and they ride to R as labels again.
- `timedelta64` arrays of any non-calendar unit arrive as a `difftime` (numpy scalars and `datetime.timedelta` included); inside a frame, a difftime column exports back to Arrow as `duration[us]`.
- A data frame arrives as an R `data.frame` -- the [`pymizu.Frame`](#pymizu.frame) section has the details, including how R classes and attributes are preserved.

## pymizu.Frame

A [pymizu.Frame](../reference/Frame.md#pymizu.Frame) is Python's home for an R `data.frame`: named columns with a row count. `frame.to_dict()` returns the columns as a dict: numpy arrays when numpy is installed, memoryviews otherwise, strings and factors as `list[str | None]`. `frame.names` and `frame.row_names` carry the metadata.

Any Arrow consumer takes a frame in one line: `pl.from_arrow(f)`, `pa.table(f)`, `pd.DataFrame.from_arrow(f)` (polars \>= 2.0 returns a struct Series for a streaming producer, and `.struct.unnest()` restores the columns). Factors become dictionary columns, logicals Arrow `bool`, `Date` `date32`, and `POSIXct` `timestamp[us]`.

A [Frame](../reference/Frame.md#pymizu.Frame) pickles, so it crosses Python-to-Python channels. Sent to R, it becomes a `data.frame` again. If the other side is too old to read shared-memory frames, the frame crosses as a copy instead.

Frame metadata rides along verbatim. The full class vector of a data.frame (a tibble's `c("tbl_df", "tbl", "data.frame")`, any class ending in `"data.frame"`) and extra attributes -- a `reference` note, readr's column specification, user attributes -- cross opaquely. Python stashes them undecoded on the [`Frame`](../reference/Frame.md#pymizu.Frame) (its `_r_attrs`), and an unmodified echo restores them in R. The stash rides the pristine received frame only: a frame rebuilt through pyarrow, polars, pandas, or [from_arrow](../reference/Frame.from_arrow.md#pymizu.Frame.from_arrow) arrives in R as a plain `data.frame`.

## Mixed-language pools

A pool's workers can be R processes. [pymizu.r_pool_launcher()](../reference/r_pool_launcher.md#pymizu.r_pool_launcher) spawns them, and [pymizu.call()](../reference/call.md#pymizu.call) builds the task: a qualified function name (`"pkg::fn"` for R, `"mod.fn"` for Python) or a `source=` string, plus constant arguments:

``` python
import numpy as np
import pymizu

pool = pymizu.Pool.create(4, launcher=pymizu.r_pool_launcher())
t = pool.submit(pymizu.call("stats::quantile", np.arange(101.0),
                            probs=np.array([0.25, 0.5, 0.75]), names=False))
print(t.collect())                  # [ 25.  50.  75.]
t2 = pool.submit(pymizu.call(source="y <- x * 2\ny + 1", x=20))
print(t2.collect())                 # 41
pool.stop()
```

Unnamed arguments become positional arguments, and keyword arguments become named ones. This matches R's calling convention. A `source=` task evaluates in a fresh environment with the keyword arguments bound as names and the positional arguments bound as `_1`, `_2`, … The result is the value of the last expression.

The workers' language comes from the pool itself: there is no language argument and no override. On an R-worker pool, submitting a plain Python callable raises an error naming [pymizu.call()](../reference/call.md#pymizu.call).

An unqualified name errors at submit, and an argument that cannot cross the boundary raises [DeclinedError](../reference/DeclinedError.md#pymizu.DeclinedError) at submit. An error in the R code crosses as [TaskError](../reference/TaskError.md#pymizu.TaskError) with the R error type preserved in `remote_type`, and a result with no Python equivalent fails the task with an error naming the type.

The reverse direction mirrors: `mizu::mizu_pool()` with `mizu::mizu_py_pool_launcher()`, and `mizu::mizu_submit_call()` with `mizu::mizu_call()` specs.

Arguments follow the same rules as channel payloads: a fresh array past the 32 KB floor crosses as one shared-memory view rather than a copy, and an argument that is already a shared view (a `_ShmView`, or a numpy array whose `.base` chain ends in one) crosses as its identifier alone -- zero payload bytes. If the pool runs workers too old to read such references, the submit fails with an error naming the remedy.

A pool's workers all run one language: mixing R and Python workers in one pool is not supported.

## Mapping over a foreign pool

You can also pass a [pymizu.call()](../reference/call.md#pymizu.call) spec as the `fn` of [Pool.map()](../reference/Pool.map.md#pymizu.Pool.map) or [Pool.map_prepare()](../reference/Pool.map_prepare.md#pymizu.Pool.map_prepare) on an R-worker pool. `mizu::mizu_map()` mirrors this in R:

``` python
pool = pymizu.Pool.create(4, launcher=pymizu.r_pool_launcher())
pool.map(pymizu.call("stats::median"), x)
pool.map(pymizu.call(source="summary(mgcv::gam(x$y ~ s(x$x)))$r.sq"),
         folds, template=0.0)   # each fold a dict; the element binds as x
pool.stop()
```

Each element becomes the spec's first positional argument (a qualified-name spec) or binds as `x` (a `source=` spec). Put constant arguments in the spec itself: the map's own `args=`/`kwargs=` must stay empty. `template=` and `collect=` work unchanged, and a per-element error crosses with its `index`.

Seeds must be integers -- with R workers, within R's 32-bit range, with the offset at most 2^53. Each language derives its own random streams from the seed, so results are reproducible for a given worker language but not identical across languages.

## The dtype matrix

The exact correspondence between Python and R dtypes. Conversion happens once, at send time. The identity rows (float64, int32, int64, complex128, uint8) are a plain memcpy.

| Python sends | R receives | Notes |
|----|----|----|
| `bytes` / uint8 | raw | Arrow uint8 with nulls: `TypeError` (R raw has no NA) |
| int8 / int16 / uint16 | integer | widened, exact |
| int32 | integer |  |
| uint32 | double | widened, exact |
| int64 | integer64 (bit64's layout) | bit-exact; `INT64_MIN` reads as `NA` |
| uint64 | double + `NA` past 2^53 (warned) | the frame column and `mizu_as_arrow()` are bit-exact |
| float16 / float32 / float64 | double | float16 and float32 widened, exact |
| bool | logical |  |
| complex64 / complex128 | complex | (buffer protocol only; Arrow has no standard complex) |
| Arrow bool / numeric with nulls | logical / numeric with `NA` | the validity bitmap is honored, slices included |
| Arrow strings (utf8 / large_utf8 / string_view) | character | a Series as `list[str \| None]`; a column as a frame column |
| Arrow `date32` / `timestamp[u]` | `Date` / `POSIXct` | the zone name rides a frame column's metadata |
| Arrow `duration[u]` | `difftime` (secs) | the frame column exports back as `duration[us]` |
| timedelta64 (any non-calendar unit) | `difftime` (secs) | `NaT` reads as `NA`; the unit converts to seconds |
| Arrow dictionary-encoded | factor | ordered dictionaries decline (`polars Enum`: cast to `pl.Categorical`) |
| Arrow ChunkedArray / Table / Series | vector / `data.frame` | chunks are concatenated |
| Arrow nested, decimal, time32/64, interval | -- | [DeclinedError](../reference/DeclinedError.md#pymizu.DeclinedError) at send time |

Missing values:

- R marks missing values with sentinels inside the data: `INT_MIN` for integer and logical, `INT64_MIN` for integer64, and a special NaN value (`NA_real_`) for double. Arrow nulls convert to these sentinels, so R sees correct `NA`s.
- Read from Python, the result depends on the accessor:
  - Copied reads (`list(...)`, [Frame.to_dict()](../reference/Frame.to_dict.md#pymizu.Frame.to_dict)) give exact dtypes: a logical arrives as `bool_` when it has no `NA`, else int32. An integer arrives as int32 when `NA`-free, else float64 with NaN (every int32 value is exact in float64).
  - Zero-copy views keep the sentinel values in the buffer. `.to_numpy()` applies the same rule and reads NA-freeness off the region's validity section before any data scan.
  - int64 keeps its dtype in every numpy form and warns when an `NA` is detected on a copied read or `.to_numpy()`. Use `.to_arrow()` for an NA-exact read. A genuine Python `-2^63` value round-trips without a warning.
  - Every Arrow export of a logical is Arrow `bool` with a validity bitmap, whatever the path. Integer exports are int32 or int64 with a validity bitmap. A double exports as float64 with `NA_real_` slots as Arrow nulls (a genuine NaN stays a valid value).
- A genuine int32 value of `-2147483648` collides with the `NA` sentinel and reads as `NA` in R. Likewise an int64 value of `-9223372036854775808` reads as `NA_integer64_`. numpy sends stay silent. An Arrow int32 or int64 send with a validity bitmap warns once, to an R peer. A Python peer reads the identical columns exactly and stays silent.
- Python compute treats `NA_real_` as an ordinary NaN. Whether the exact NaN value survives arithmetic is platform-dependent. Do not rely on it.

Round trips are stable after the first hop, and a pass-through echo is bit-exact. An untouched received view sent back crosses as a reference to its region, so no data moves and even the `NA_real_` value survives a relay. A slice or a dtype view of a received view crosses by value.

| Python sends | R sees | Back in Python | Notes |
|----|----|----|----|
| uint8 | raw | uint8 | exact |
| int8 / int16 / uint16 | integer | int32 | widened, values exact |
| int32 | integer | int32 | exact |
| uint32 | double | float64 | exact |
| int64 | integer64 | int64 | exact |
| uint64 | double + `NA` past 2^53 (warned) | uint64 | the column and R's Arrow export are bit-exact |
| float16 | double | float64 | widened, values exact |
| float32 | double | float64 | widened, values exact |
| float64 | double | float64 | exact |
| bool | logical | `bool_` (int32 with NAs) | dtype lost |
| complex64 / complex128 | complex | complex128 | exact |
| timedelta64 | `difftime` | timedelta64\[us\] | unit normalizes to us, values exact |

| R sends | Python sees | Back in R | Notes |
|----|----|----|----|
| integer | int32 (float64 with NAs) | integer | exact |
| integer64 (bit64) | int64 (warns on `NA`) | integer64 | exact; a genuine Python `-2^63` arrives as `NA` |
| double | float64 | double | exact |
| raw | uint8 | raw | exact |
| complex | complex128 | complex | exact |
| logical | `bool_` (int32 with NAs) | **integer** | tag lost on a Python round trip (values kept); a whole view sent back stays logical |
| difftime | timedelta64\[us\] | difftime (secs) | units normalize to seconds, values exact; `NA` crosses as `NaT` |

Between two Python processes, on a channel or a pool, the conversions on this page never run. The identity dtypes cross unchanged, and everything else pickles. An exact Python-to-Python send of a non-identity dtype needs no work. They apply only when the other end runs R.
