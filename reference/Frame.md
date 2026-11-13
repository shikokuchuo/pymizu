# Frame

A data.frame's Python home: named columns with a row count.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_pymizu.pyi#L42-L54)

``` python
Frame()
```

to_dict() returns the columns as a dict without an Arrow library, and **arrow_c_stream** exports to polars, pyarrow, or pandas. from_arrow() constructs one from an Arrow producer, the opt-in zero-copy path for Python-to-Python channels. R frame metadata (classes, extra attributes) survives an unmodified round trip, while a rebuilt frame arrives in R as a plain data.frame.

## Methods

| Name | Description |
|----|----|
| [\_\_arrow_c_stream\_\_()](#__arrow_c_stream__) | **arrow_c_stream**(requested_schema=None) -\> capsule |
| [\_\_len\_\_()](#__len__) | Return len(self). |
| [from_arrow()](#from_arrow) | from_arrow(obj) -\> Frame |
| [to_dict()](#to_dict) | to_dict() -\> dict |

### \_\_arrow_c_stream\_\_()

**arrow_c_stream**(requested_schema=None) -\> capsule

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_pymizu.pyi#L52)

``` python
__arrow_c_stream__(requested_schema=None)
```

The Arrow C Stream Interface export: the frame as one struct batch. Consumers take it in one line: pl.from_arrow(f), pa.table(f), or pd.DataFrame.from_arrow(f). A complex column raises (no Arrow type).

### \_\_len\_\_()

Return len(self).

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_pymizu.pyi#L53)

``` python
__len__()
```

### from_arrow()

from_arrow(obj) -\> Frame

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_pymizu.pyi#L50-L51)

``` python
from_arrow(obj)
```

Construct a frame from an Arrow producer, adopting its buffers wherever a column's frame layout is the Arrow layout (a single-batch producer on offset-0 boundaries), so construction copies only what it has to, nulls included.

This is the opt-in zero-copy frame path for Python-to-Python channels: past the zero-copy floor the frame crosses as one shared-memory region and is read in place, where a plain Arrow object pickles (type-exact, but a full copy). The frame reads back with the same values but normalized types: a chunked or sliced producer's columns concatenate, uints widen, nanosecond timestamps rescale to microseconds, and the received value is a Frame, not the producer's type.

obj Any [\_\_arrow_c_stream\_\_](../reference/Frame.md#pymizu.Frame.__arrow_c_stream__) producer: a pyarrow `Table` or `RecordBatchReader`, a polars `DataFrame`, a duckdb relation. A chunked table adopts after `Table.combine_chunks()`.

    The frame. An adopted column keeps the producer's buffers alive
    for the frame's lifetime (numpy-view semantics: a frame from a
    4 GB table pins it).

MizuError The Arrow type has no portable equivalent (the error names the column), or the producer's export failed.

``` python
>>> import pyarrow as pa
>>> import pymizu
>>> with pymizu.Channel.create(
...     """
... import pymizu
... while True:
...     x = ch.recv()
...     if x is pymizu.CLOSED:
...         break
...     ch.send(x)
... """
... ) as ch:
...     f = pymizu.Frame.from_arrow(
...         pa.table({"x": pa.array([1.5, None] * 100000)})
...     )
...     ch.send(f)
...     echoed = pa.table(ch.recv(timeout=5))
>>> echoed.column("x").null_count
100000
```

### to_dict()

to_dict() -\> dict

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/_pymizu.pyi#L49)

``` python
to_dict()
```

The frame as a dict of columns: numpy arrays for numeric columns (or memoryviews without numpy), datetime64 for Date / POSIXct, and list\[str \| None\] for string and factor columns. Complex columns carry complex128 here (the Arrow export has no complex type).
