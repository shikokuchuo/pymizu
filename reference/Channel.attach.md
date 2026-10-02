# Channel.attach()


Attach to the channel named by a join token (the peer side).


Usage

``` python
Channel.attach(token)
```


Low-level: the caller consumes `ch.drop` before signalling `ch.ready_set()`. `python -m pymizu.child` is the reference peer entry.
