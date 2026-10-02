# Frame.to_dict()


to_dict() -\> dict


Usage


``` python
Frame.to_dict()
```


The frame as a dict of columns: numpy arrays for numeric columns (or memoryviews without numpy), datetime64 for Date / POSIXct, and list\[str \| None\] for string and factor columns. Complex columns carry complex128 here (the Arrow export has no complex type).
