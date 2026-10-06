# Channel.\_\_iter\_\_()


Iterate over received payloads until CLOSED or PEER_GONE.


Usage

``` python
Channel.__iter__()
```


Blocks indefinitely between payloads ([recv()](Channel.recv.md#pymizu.Channel.recv) with no timeout); use :meth:[recv](Channel.recv.md#pymizu.Channel.recv) directly when a bound is needed.
