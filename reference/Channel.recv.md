# Channel.recv()


Receive one payload.


Usage

``` python
Channel.recv(timeout=None)
```


## Parameters


`timeout: float | None = None`  
Seconds to wait; None waits indefinitely.


## Returns


`The payload, or the ``TIMEOUT`` / ``CLOSED`` / ``PEER_GONE`  
sentinel on the non-payload outcomes.
