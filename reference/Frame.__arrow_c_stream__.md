# Frame.\_\_arrow_c_stream\_\_()


**arrow_c_stream**(requested_schema=None) -\> capsule


Usage


``` python
Frame.__arrow_c_stream__(requested_schema=None)
```


The Arrow C Stream Interface export: the frame as one struct batch. Consumers take it in one line: pl.from_arrow(f), pa.table(f), or pd.DataFrame.from_arrow(f). A complex column raises (no Arrow type).
