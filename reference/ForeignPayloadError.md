# ForeignPayloadError

A received payload in a language-private or unrecognized format: the peer sent a value outside the portable interchange subset. The message is consumed, so the ring is never blocked, and under recv_async() only the Future waiting on that message rejects.

Usage

``` python
ForeignPayloadError()
```
