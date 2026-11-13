# ShmError

A shared-memory region operation failed. Also an OSError: the failure is at the OS layer. Carries a 'bytes' attribute: the requested size of a region that was not created, None when a region was not opened.

Usage

``` python
ShmError()
```
