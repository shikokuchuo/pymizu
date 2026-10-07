# Frame.from_arrow()


from_arrow(obj) -\> Frame


Usage


``` python
Frame.from_arrow(obj)
```


Construct a frame from an Arrow producer, adopting its buffers wherever a column's frame layout is the Arrow layout (a single-batch producer on offset-0 boundaries), so construction copies nothing it does not have to -- nulls included.

This is the opt-in zero-copy frame path for Python-to-Python channels: past the zero-copy floor the frame crosses as one shared-memory region and arrives region-backed, where a plain Arrow object pickles (container-exact, but a full copy). The frame reads back value-exact but type-normalized: a chunked or sliced producer's columns concatenate, uints widen, nanosecond timestamps rescale to microseconds, and the received value is a Frame, not the producer's type.


obj Any [__arrow_c_stream__](Frame.__arrow_c_stream__.md#pymizu.Frame.__arrow_c_stream__) producer: a pyarrow `Table` or `RecordBatchReader`, a polars `DataFrame`, a duckdb relation. `Table.combine_chunks()` is the route to the adopt path for a chunked table.


    The frame. An adopted column keeps the producer's buffers alive
    for the frame's lifetime (numpy-view semantics: a frame from a
    4 GB table pins it).


MizuError The Arrow type has no portable home (the column is named), or the producer's export failed.


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
