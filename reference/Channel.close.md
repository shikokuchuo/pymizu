# Channel.close()


Orderly close: signal the peer and wait up to `timeout`


Usage

``` python
Channel.close(timeout=5.0)
```


seconds for it to observe the close. True on a clean handshake; False (with a warning) on timeout, in which case resources release when the handle is garbage collected.
