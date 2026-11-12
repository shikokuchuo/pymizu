# Changelog

This changelog is generated automatically from [GitHub Releases](https://github.com/shikokuchuo/pymizu/releases).

## pymizu 0.1.1

*2026-11-12* · [GitHub](https://github.com/shikokuchuo/pymizu/releases/tag/v0.1.1)

### What's Changed

- Cache numpy entry points and trim small redundancies by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/15
- Deduplicate repeated C idioms by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/16
- Hot-path efficiency by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/17
- Fix a Pool.stop deadlock with an in-flight notify drain by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/18
- Refactor C near the byte-identity surface by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/19
- De-thread pymizu from \_map.py and dedup its Python structure by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/20
- Faster masked int64 Arrow conversions to R peers by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/21
- Send Pool.map() results over a typed result codec instead of pickle by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/22
- Speed up the unseeded map element loop by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/23
- Speed up Task.collect_async() and PoolExecutor submits by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/25
- Gate streaming spec seed offsets and inform numpy-less complex map errors by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/26
- Stage top-level scalars directly as 'I' tags on foreign handles by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/27
- Fix async pool collections hanging when a pool is reused across event loops by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/28
- Add Channel.recv_batch_async() and batch iterators by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/29
- Linux performance pass and vendored libmizu update by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/30
- Speed up send staging, map loops, and asyncio waiter detach by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/31
- Speed up task decode, spin waits, and buffer task staging by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/32
- Speed up small-container sends, frame resends, and view Arrow exports by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/33
- Faster large frame relay and batch task collection by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/34
- Fix PoolExecutor submit/drain collect race by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/35
- Build with pinned -O3, LTO, and hidden visibility; strip release wheels by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/36
- Resolve task fns through sys.modules; drop the fn caches by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/37
- Speed up C boundary calls by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/38
- Speed up small task collections by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/39
- Defer package version lookup until first access by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/40
- Fix Arrow RecordBatch and vector staging by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/41
- Re-vendor libmizu Arrow conversion kernels by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/42
- Speed up foreign Frame sends by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/43
- Re-vendor libmizu Arrow null handling speedups by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/44
- Speed up Frame.from_arrow copies and send Frame task arguments by name by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/46
- Improve executor performance by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/47
- Remove dead code, fold duplicated paths and re-vendor libmizu by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/48
- Remove the TestPyPI publish leg by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/49
- Fix sdist tests and guard PyPI publishing by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/50
- Elide the channel batch-receive API by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/51
- pymizu 0.1.1 by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/52

**Full Changelog**: [https://github.com/shikokuchuo/pymizu/compare/v0.1.0…v0.1.1](https://github.com/shikokuchuo/pymizu/compare/v0.1.0...v0.1.1)

## pymizu 0.1.0

*2026-11-04* · [GitHub](https://github.com/shikokuchuo/pymizu/releases/tag/v0.1.0)

### What's Changed

- Build the docs site with Great Docs by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/2
- Add a process-wide default pool by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/4
- Silence the test suite's expected warnings by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/5
- Add streaming Pool.map with stream = TRUE by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/7
- Adapt the frame-export tests to polars 2.0's stream ingest by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/8
- Integration into `asyncio` event loop by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/10
- ci: add package-level tests for `__all__` and `__version__` by [@rich-iannone](https://github.com/rich-iannone) in https://github.com/shikokuchuo/pymizu/pull/11
- Make the pytest config a bit more strict by [@rich-iannone](https://github.com/rich-iannone) in https://github.com/shikokuchuo/pymizu/pull/12
- pymizu 0.1.0 by [@shikokuchuo](https://github.com/shikokuchuo) in https://github.com/shikokuchuo/pymizu/pull/14

### New Contributors

- [@shikokuchuo](https://github.com/shikokuchuo) made their first contribution in https://github.com/shikokuchuo/pymizu/pull/2
- [@rich-iannone](https://github.com/rich-iannone) made their first contribution in https://github.com/shikokuchuo/pymizu/pull/11

**Full Changelog**: https://github.com/shikokuchuo/pymizu/commits/v0.1.0
