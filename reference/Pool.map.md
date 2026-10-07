# Pool.map()


Map `fn` over the elements of `x` on the pool; return the


Usage

``` python
Pool.map(
    fn,
    x,
    *,
    args=(),
    kwargs=None,
    n_chunks=None,
    seed=None,
    timeout=None,
    template=None,
    collect=None,
    stream=False,
)
```


results as a list in input order.


## Parameters


`fn: _Callable[…, _Any]`  
The callable to apply, or a <a href="../reference/call.html#pymizu.call" class="gdls-link"><code>call</code></a> spec.

`x: _Iterable[_Any]`  
The iterable of elements.

`args: _Iterable[_Any] = ()`  
Constant positional arguments appended to every call.

`kwargs: dict[str, _Any] | None = None`  
Constant keyword arguments of every call.

`n_chunks: int | None = None`  
Overrides the morsel count (the scheduling granularity).

`seed: int | bytes | bytearray | tuple[int | bytes | bytearray, int] | None = None`  
An int or bytes deriving deterministic per-element streams of the stdlib `random` module; see Details. Pass `seed=(seed, offset)` to shift every element's stream by `offset` positions, for maps split across runs or processes.

`timeout: float | None = None`  
Seconds to wait for completion; None waits indefinitely.

`template: _Any = None`  
An exemplar buffer (e.g. `numpy.empty(m, dtype=...)`) declaring that every `fn` result is `m` values of that dtype; see Details.

`collect: str | None = None`  
`"copy"` (the default) or `"view"`; governs how a template-backed output area is returned.

`stream: bool = ``False`  
Stream slices of `x` to workers instead of staging the whole of `x` in shared memory; see Details.


## Returns


`The results as a list in input order; with ``template``, the`  
gathered output area; on `timeout` expiry, the `TIMEOUT` sentinel.


## Raises


`TaskError`  
A task error re-raises as TaskError carrying the failing element's 0-based `index`; failure is fail-fast (peers stop within about one batch).

`WorkerDiedError`  
Worker death carries the lost element ranges as `lost` (0-based half-open `(lo, hi)` pairs, conservative).

`TypeError`  
A <a href="../reference/call.html#pymizu.call" class="gdls-link"><code>call</code></a> spec as `fn` with `stream=True`.

`DeclinedError`  
With a spec, a non-portable constant or element at stage time.


## Details

One call stages `fn`, the constant `args`/`kwargs`, and `x` exactly once (a shared region, or inline in chunk tasks when small), then submits one *runner* task per live worker; runners self-schedule adaptively sized element batches off a shared cursor. A C-contiguous buffer of a supported dtype (float64/int32/int64/complex128/uint8) travels as bare bytes -- workers wrap it once and index per element.

`seed` derives deterministic per-element streams of the stdlib `random` module: element `i` runs under `random.seed(SHA-256(seed_bytes + i.to_bytes(8, "little")))`, identical for any chunking, worker count, or steal order. `seed` covers the stdlib `random` module only: a task drawing from numpy calls <a href="../reference/current_rng.html#pymizu.current_rng" class="gdls-link"><code>current_rng()</code></a> inside the task (the element's own memoized `numpy.random.Generator`, derived from the same seed material); other RNG universes are out of scope. The legacy `np.random.*` module functions draw from the worker's shared global RandomState and stay order-dependent. A seeded map element must not nested-submit and collect: worker helping can run another seeded map's batches mid-element, wiping this element's <a href="../reference/current_rng.html#pymizu.current_rng" class="gdls-link"><code>current_rng()</code></a> stash -- a later call rebuilds from the digest, restarting the stream instead of continuing it (the stdlib streams survive: the helped batch's save/restore nests inside this batch's own).

`fn` may be a <a href="../reference/call.html#pymizu.call" class="gdls-link"><code>call</code></a> specification instead of a callable -- the way to map over a foreign pool (one spawned with <a href="../reference/r_pool_launcher.html#pymizu.r_pool_launcher" class="gdls-link"><code>r_pool_launcher()</code></a>). A spec always stages a shared region: the descriptor crosses in the interchange format and each runner task carries a region reference any worker language reads. The element fills the spec's first positional slot (name kind) or binds as `x` (source kind), and the spec's own constant arguments ride with it -- so `args` and `kwargs` must be empty with a spec. Constants and elements must be portable values; a non-portable one raises <a href="../reference/DeclinedError.html#pymizu.DeclinedError" class="gdls-link"><code>DeclinedError</code></a> at stage time. `seed=` carries as a language-neutral pair and each worker language derives its own streams, so a spec map takes int seeds only (32-bit-ranged on R workers); invariance holds within a worker language, never identical draws across languages.

`template` declares that every `fn` result is `m` values of the exemplar's dtype: results are written in place into a shared `n x m` output area and never serialized. Each result must be a matching buffer -- or, for `m == 1`, a plain Python scalar. With a template, `collect="copy"` (the default) returns the area as one gathered numpy array (a memoryview without numpy) of shape `(n, m)` -- `(n,)` for `m == 1`; `collect="view"` returns it zero-copy, with the map region's teardown deferred to the view's.

With `stream=True`, the map never stages the whole of `x` into shared memory: it streams slices of `x` to workers as they take work; the return value is unchanged. Fixed x-slices ride ordinary chunk tasks under a sliding submit/collect window of at most `min(n_chunks, 2 * live workers, free result slots)` outstanding tasks, so shared-memory residency is bounded by `window x slice` instead of `sizeof(x)` -- with the default chunk count (`min(len(x), 32 * live workers)`) that is roughly `(2 * workers) / n_chunks` of the serialized `x`. `n_chunks=` overrides the chunk count outright (`n_chunks=len(x)` is the mirai-style extreme of one element per task). Everything else -- result order, `template` and [collect](Task.md#pymizu.Task.collect), `seed` invariance, the error taxonomy -- is exactly the non-streaming map's. Fail-fast latency coarsens from about one adaptive morsel batch to about one chunk (the bound moves with `n_chunks`), the adaptive batch sizing of the morsel machinery is lost (skew mitigation is to raise `n_chunks`), and slices cross via the serialized tiers, so per-chunk staging costs an ordinary submit's serialization rather than the raw section's zero-copy slicing. A streaming map always stages its descriptor region and needs same-language workers: a <a href="../reference/call.html#pymizu.call" class="gdls-link"><code>call</code></a> spec as `fn` raises TypeError.

On `timeout` expiry the outstanding work is cancelled and the `TIMEOUT` sentinel is returned, never raised.

<a href="../reference/Pool.starmap.html#pymizu.Pool.starmap" class="gdls-link"><code>starmap()</code></a> is the unpacking variant -- `fn(*element, *args, **kwargs)`, the `multiprocessing.Pool.starmap` convention.


## Examples

``` python
>>> import pymizu
>>> with pymizu.Pool.create(2) as pool:
...     results = pool.map(abs, range(-5, 5))
>>> results
[5, 4, 3, 2, 1, 0, 1, 2, 3, 4]
```
