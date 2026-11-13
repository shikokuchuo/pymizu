# map()

Map `fn` over `x` on the pool.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1136-L1324)

``` python
map(
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    template=None,
    n_chunks=None,
    seed=None,
    timeout=None,
    stream=False,
    collect=None,
)
```

## Parameters

`fn: _Callable[…, _Any]`  
The callable to apply, or a [`call`](../reference/call.md#pymizu.call) spec.

`x: _Iterable[_Any]`  
The iterable of elements.

`args: _Iterable[_Any] = ()`  
Constant positional arguments appended to every call.

`kwargs: dict[str, _Any] | None = None`  
Constant keyword arguments of every call.

`template: _Any = None`  
An example buffer declaring each result's length and dtype, such as `numpy.empty(m, dtype=...)`. See Details.

`n_chunks: int | None = None`  
Overrides the number of batches the elements are split into.

`seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None = None`  
An int or bytes seeding a deterministic random stream per element, or a `(seed, offset)` tuple. See Details.

`timeout: float | None = None`  
Seconds to wait for submissions. None waits indefinitely. The handle's `collect(timeout)` sets the collection deadline.

`stream: bool = False`  
Stream slices of `x` to workers instead of writing the whole of `x` to shared memory first; see Details.

`collect: str | None = None`  
`"copy"` (the default) or `"view"`; governs how a template-backed output area is returned.

## Returns

`A Map handle for the run. See`  
[`Map.collect()`](../reference/Map.md#pymizu.Map.collect) for the result forms and [`Map`](../reference/Map.md#pymizu.Map) for async collection and cancellation. If submission times out, `map()` returns the `TIMEOUT` sentinel without a handle.

## Raises

`TaskError`  
A task error re-raises from the collect as TaskError carrying the failing element's 0-based `index`; the map fails fast (workers stop within about one batch).

`WorkerDiedError`  
Worker death carries the lost element ranges as `lost` (0-based half-open `(lo, hi)` pairs, possibly wider than what was lost; mizu's `mizu_error_worker_died$lost` carries the same set as a two-column matrix of inclusive 1-based ranges).

`TypeError`  
`collect="view"` with a streaming [`call`](../reference/call.md#pymizu.call) spec.

`DeclinedError`  
With a spec, a non-portable constant or element at stage time.

## Details

One call writes `fn`, the constant `args`/`kwargs`, and `x` to shared memory once (or sends them inside chunk tasks when small), then submits one *runner* task per live worker. Runners claim adaptively sized element batches off a shared counter. A C-contiguous buffer of a supported dtype (float64/int32/int64/complex128/uint8) travels as bare bytes, and each worker wraps it once and indexes into it per element.

`seed` gives every element a deterministic stream of the stdlib `random` module. Element `i` runs under `random.seed(SHA-256(seed_bytes + key.to_bytes(8, "little")))`, where `key = i + offset`. The default `offset` is 0. Pass `seed=(seed, offset)` to shift every stream for maps split across runs or processes. Chunking, worker count, and steal order do not change the streams.

For deterministic numpy draws, call [`current_rng()`](../reference/current_rng.md#pymizu.current_rng) inside the task. It returns the element's `numpy.random.Generator`, created on first call from the same seed. Its derivation is separate from the stdlib stream. The legacy `np.random.*` functions draw from the worker's shared global RandomState and stay order-dependent. The map does not seed other RNG libraries.

A seeded map element must not submit and collect nested tasks. While the worker waits, it can run another map's batches and clear this element's [`current_rng()`](../reference/current_rng.md#pymizu.current_rng) state. A later call rebuilds the generator from the seed and restarts its stream. The stdlib streams survive.

`fn` can be a [`call`](../reference/call.md#pymizu.call) specification instead of a callable. Use a spec to map over a foreign pool, such as one spawned with [`r_launcher()`](../reference/r_launcher.md#pymizu.r_launcher). A spec always uses a shared-memory region: the description crosses in a language-neutral format that any worker language reads. The element fills the spec's first positional slot (name kind) or binds as `x` (source kind), and the spec's own constant arguments travel with it, so `args` and `kwargs` must be empty with a spec. Constants and elements must be portable values, and a non-portable one raises [`DeclinedError`](../reference/DeclinedError.md#pymizu.DeclinedError) at the `map()` call. `seed=` crosses as a language-neutral pair and each worker language derives its own streams, so a spec map takes int seeds only (on R workers, the seed within 32-bit range and the offset at most 2^53; with `stream=True`, the offset plus `len(x)` must stay within 2^53). Results are reproducible within a worker language but not identical across languages.

`template` declares that every `fn` result is `m` values of the example buffer's dtype. Results are written in place into a shared `n x m` output area and never serialized. Each result must be a matching buffer or, for `m == 1`, a plain Python scalar. `collect="copy"` gathers the output area. `collect="view"` returns it zero-copy, and the shared memory is released when the view is gone. See [`Map.collect()`](../reference/Map.md#pymizu.Map.collect) for the returned shape.

With `stream=True`, slices travel as ordinary chunk tasks under a sliding submit/collect window of at most `min(n_chunks, 2 * live workers, free result slots)` outstanding tasks. Shared-memory use is bounded by `window x slice` instead of `sizeof(x)`. With the default chunk count (`min(len(x), 32 * live workers)`), that is roughly `(2 * workers) / n_chunks` of the serialized `x`. `n_chunks=` overrides the chunk count. `n_chunks=len(x)` uses one element per task. Result order, `template`, `collect`, `seed` reproducibility, and errors are the same as for a non-streaming map.

Streaming has three costs. Failure detection coarsens from about one adaptive batch to about one chunk, with the bound set by `n_chunks`. Adaptive batch sizing is lost, so raise `n_chunks` to reduce skew. Slices are serialized like ordinary task payloads instead of using the zero-copy path for a raw array.

A streaming map with Python workers shares one description of the map with every worker, each read at most once. A [`call`](../reference/call.md#pymizu.call) spec as `fn` streams too, over workers of any language. Each chunk in the window then writes a small shared-memory region of its own: the slice plus its task, with one runner task per chunk. The region is removed when the chunk completes. Shared-memory use stays bounded by the window plus a small per-worker cache. Each chunk writes its results separately, so they cannot combine into one view. `collect="view"` raises `TypeError` on a streaming spec map, as does preparing one with [`map_prepare()`](../reference/pymizu.Pool.map_prepare.md).

[`starmap()`](../reference/pymizu.Pool.starmap.md) is the unpacking variant, calling `fn(*element, *args, **kwargs)` in the `multiprocessing.Pool.starmap` convention.

Threading: a map is a producer on the handle, so it raises [ConcurrentUseError](../reference/ConcurrentUseError.md#pymizu.ConcurrentUseError) when another map or a submit is in flight from a second thread (see the class docstring).

## Examples

``` python
>>> import pymizu
>>> with pymizu.Pool.create(2) as pool:
...     results = pool.map(abs, range(-5, 5)).collect()
>>> results
[5, 4, 3, 2, 1, 0, 1, 2, 3, 4]
```
