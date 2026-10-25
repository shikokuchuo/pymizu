# R interop

The process at the other end of a channel does not have to be Python. [mizu](https://github.com/shikokuchuo/mizu) is the R counterpart of pymizu, built on the same shared-memory core. A channel or pool connects the two languages as directly as two Python processes.

## R channel peers

A channel peer can be an R process with the `mizu` package installed. Pass the peer program as R source, and set the launcher to [pymizu.r_launcher()](../reference/r_launcher.md#pymizu.r_launcher). Here, an R process doubles the array you send:

``` python
import numpy as np
import pymizu

ch = pymizu.Channel.create(
    """
library(mizu)
repeat {
  x <- mizu_recv(ch, timeout = 30)
  if (inherits(x, "mizu_sentinel")) break
  mizu_send(ch, x * 2)
}
""",
    launcher=pymizu.r_launcher(),
)

ch.send(np.array([1.5, 2.5, 3.5]))
print(ch.recv(timeout=5))            # [3. 5. 7.]
ch.close()
```

[r_launcher()](../reference/r_launcher.md#pymizu.r_launcher) needs R and the `mizu` R package installed. If either is missing, it raises [MizuError](../reference/MizuError.md#pymizu.MizuError) before the channel is created. Pass `rscript=` to use a specific R installation.

To spawn the peer differently, write your own launcher: one callable that takes the channel's token and starts the peer process. The reverse direction works too: an R session spawns a Python peer with `mizu::mizu_py_launcher()`.

## Large data crosses without copying

Past a size floor, the data itself never moves. pymizu writes it to shared memory once, and both sides read it in place: no serialization, no parsing, no further copies. A large value you send is read by R directly over the shared pages. A large value you receive arrives as a view over those pages, usually wrapped as a numpy array. The peer below echoes back whatever it receives. This channel stays open for the next few examples:

``` python
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

big = np.random.default_rng(0).normal(size=1_250_000)  # ten megabytes
ch.send(big)
back = ch.recv(timeout=5)
np.array_equal(back, big)                              # True
```

Apart from that one write into the shared region, nothing moved at all. R's echo came back as a reference with zero payload bytes. If you send a received view onward, it crosses the same way: as the region's name alone. The zero-copy tier covers every large layout: a flat array of an identity dtype (`float64`, `int32`, `int64`, `complex128`, `uint8`), an array of any shape and memory order, or a nested tree of lists, dicts, arrays, and strings. A `datetime64` array crosses the same way, with `NaT` as `NA`, and a `list[str | None]` with `None` as `NA`. A large data frame crosses as one shared region in both directions. The floor is low: 256 KB for a flat contiguous array on a channel, and 32 KB everywhere else.[^1]

Arrays of other numeric dtypes (booleans, the narrow ints, `uint64`, `float32`, …) cross as a converted copy at any size. The [dtype matrix](#the-dtype-matrix) has the per-dtype rule. Without numpy, a received view is a plain buffer: `memoryview(view)` works, and `pa.array(view)` or `pl.from_arrow(view)` wrap the same pages without copying. One exception: R's ALTREP vectors (say, `1:1e9`, stored as a recipe rather than data) cross by value, values exact. A polars or pyarrow object that wraps a received frame keeps the frame's region alive until garbage collection, so delete these wrappers when you no longer need them.

## What crosses

Small values round-trip just as exactly. Each form below comes back from R unchanged:

``` python
def echo(x):
    ch.send(x)
    return ch.recv(timeout=5)

x = np.array([1.5, 2.5, 3.5])
np.array_equal(echo(x), x)     # a float64 vector: True

m = np.arange(6, dtype=np.int32).reshape(2, 3)
np.array_equal(echo(m), m)     # an int32 matrix, dim included: True

echo("hello") == "hello"       # a string: True

d = np.array(["2024-01-01", "2024-01-03"], dtype="datetime64[D]")
np.array_equal(echo(d), d)     # dates keep their meaning: True
```

A data frame crosses as well, with its column types intact. Here a polars frame with a categorical column; pandas and pyarrow frames work the same way:

``` python
import polars as pl
df = pl.DataFrame({
    "total_bill": [16.99, 10.34, 21.01],
    "day": pl.Series(["Sun", "Sun", "Mon"], dtype=pl.Categorical),
})
ch.send(df)        # arrives in R as a data.frame, the categorical column as a factor
back = ch.recv(timeout=5)
back.names         # ('total_bill', 'day')
```

R sees the natural equivalent throughout: a vector for an array, a factor for a category column, a named list for a dict, a `Date` or `POSIXct` for a `datetime64` array. An Arrow array arrives as an R vector, with Arrow nulls as R missing values: `ch.send(pa.array([1, None, 3]))`. Labels cross with the values they name: a named R vector arrives as a [`DimArray`](../reference/DimArray.md#pymizu.DimArray) carrying its names, and labels you attach in Python ride to R as [names](../reference/Frame.names.md#pymizu.Frame.names) or `dimnames`.

## What doesn't cross

Some values have no portable home on the other side. `pymizu` declines these at send time with a [DeclinedError](../reference/DeclinedError.md#pymizu.DeclinedError) (a subclass of `TypeError`) that names the offending value. The channel stays usable:

``` python
import decimal
import pyarrow as pa

tbl = pa.table({"price": pa.array([decimal.Decimal("1.5")])})
try:
    ch.send(tbl)
except pymizu.DeclinedError as e:
    print(e)
    # pymizu: value is not portable to the peer (column 'price': an Arrow type
    # with no portable home (format 'd:2,1'))
ch.close()
```

From Python, also declined: Arrow nested, `date64`, `time`, and `interval` columns, and ordered dictionaries. Cast a polars `Enum` to `pl.Categorical` first. An Arrow `uint8` column *with nulls* is a `TypeError`: an R raw vector has no `NA`.

From R: a `POSIXlt` time, a character matrix, and S4 objects stay on R's side. So does a named vector with a class or of character type (a named character vector, factor, `Date`, or `POSIXct`), since names ride only on the plain atomic vectors. Among data frames: a data.table, a groupedData-style frame (a formula attribute), an ordered-factor column, and a column that is itself a labeled matrix.

## pymizu.Frame

A [pymizu.Frame](../reference/Frame.md#pymizu.Frame) is Python's home for an R `data.frame`: named columns with a row count. `frame.to_dict()` returns the columns as a dict: numpy arrays when numpy is installed, memoryviews otherwise, strings and factors as `list[str | None]`. `frame.names` and `frame.row_names` carry the metadata.

Any Arrow consumer takes a frame in one line: `pl.from_arrow(f)`, `pa.table(f)`, or `pd.DataFrame.from_arrow(f)`. (With polars \>= 2.0, a streaming producer gives a struct Series; `.struct.unnest()` restores the columns.) Factors become dictionary columns, logicals Arrow `bool`, `Date` `date32`, and `POSIXct` `timestamp[us]`.

A [Frame](../reference/Frame.md#pymizu.Frame) pickles, so it crosses Python-to-Python channels. Sent to R, it becomes a `data.frame` again.

Frame metadata rides along verbatim: the full class vector of a data.frame (a tibble's `c("tbl_df", "tbl", "data.frame")`, any class ending in `"data.frame"`) and extra attributes cross opaquely, stashed undecoded on the [Frame](../reference/Frame.md#pymizu.Frame). An unmodified echo restores them in R. A frame rebuilt through pyarrow, polars, pandas, or [from_arrow](../reference/Frame.from_arrow.md#pymizu.Frame.from_arrow) arrives in R as a plain `data.frame`.

## Mixed-language pools

A pool's workers can be R processes. [pymizu.r_pool_launcher()](../reference/r_pool_launcher.md#pymizu.r_pool_launcher) spawns the workers. [pymizu.call()](../reference/call.md#pymizu.call) builds a language-neutral task: a qualified `"pkg::fn"` name for R or `"mod.fn"` for Python, or a `source=` string, plus constant arguments:

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

Unnamed arguments become positional arguments, and keyword arguments become named ones, matching R's calling convention. A `source=` task runs in a fresh environment with the keyword arguments bound as names and the positional arguments bound as `_1`, `_2`, … The result is the value of the last expression.

On an R-worker pool, submitting a plain Python callable raises an error naming [pymizu.call()](../reference/call.md#pymizu.call). Task errors cross as [TaskError](../reference/TaskError.md#pymizu.TaskError) with the R error type preserved in `remote_type`. Arguments follow the same rules as channel payloads, zero-copy included: a large array crosses as one shared view, and an array that is already a received view crosses as its name alone. A pool's workers all run one language: mixing R and Python workers in one pool is not supported. The reverse direction mirrors this: `mizu::mizu_pool()` with `mizu::mizu_py_pool_launcher()`, and `mizu::mizu_submit_call()` with `mizu::mizu_call()` specs.

## Mapping over a foreign pool

To map an R function over Python data, pass a [pymizu.call()](../reference/call.md#pymizu.call) spec as the `fn` of [Pool.map()](../reference/Pool.map.md#pymizu.Pool.map) or [Pool.map_prepare()](../reference/Pool.map_prepare.md#pymizu.Pool.map_prepare) on an R-worker pool. `mizu::mizu_map()` mirrors this in R:

``` python
pool = pymizu.Pool.create(4, launcher=pymizu.r_pool_launcher())
pool.map(pymizu.call("stats::median"), x)
pool.map(pymizu.call(source="summary(mgcv::gam(x$y ~ s(x$x)))$r.sq"),
         folds, template=0.0)   # each fold a dict; the element binds as x
pool.stop()
```

Each element becomes the spec's first positional argument, or binds as `x` for a `source=` spec. Put constant arguments in the spec itself: the map's own `args=` and `kwargs=` stay empty. `template=` and `collect=` work unchanged, and a per-element error crosses with its `index`. Seeds must be integers within R's 32-bit range. Each worker language derives its own random streams from the seed, so results are reproducible for a given worker language but not identical across languages.

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

[^1]: Below the floor, a value crosses as one quick copy. At any size, these types cross: Python scalars (`bool`, `int`, `float`, `complex`, `str`, `None`), lists, dicts with `str` keys (a tuple arrives as a `list` in R), `bytes` (an R raw vector), and `datetime.date` / `datetime.datetime` scalars (length-1 dates and times in R). A few round trips change the container but never the values. A standalone R factor comes back as strings, a length-1 `NA` arrives as `None` and sent back is R's `NULL`, and an R logical vector with `NA`s comes back as integer. A `POSIXct` timezone normalizes to UTC (the instant is exact), and `difftime` units normalize to seconds. The bit patterns `-2147483648` (int32) and `-9223372036854775808` (int64) collide with R's own `NA` sentinels and read as `NA` in R. A `uint64` past 2^53 crosses as `NA` with a warning at send.
