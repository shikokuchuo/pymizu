# Benchmark records

Report-only numbers from `benchmarks/mizu-bench.py` (Apple M4 Pro unless
noted); asserts nothing, so records live here.

Two parts: the best-known table below is the headline — swap a row's
value in only when a run beats it, with the date of that measurement
(lower is better on the latency/wall rows, higher on the rate rows) —
and the dated run log at the bottom is the source of truth, carrying the
bands (several rows swing wide run-to-run; a regression read is "outside
the band", not "slower than the best") and each change's context. Row
definitions evolve with the suite; a row's date says which definition
the value is from, and the log's entry carries it. The
`mizu-stdlib-bench.py` comparison columns live in the log only. Append
new dated outcomes at the bottom of the log; update the table only on a
new best.

Log entries follow one format (shared with the libmizu and mizu bench
notes):

```
## YYYY-MM-DD — <what changed or what was run> [(<commit>[, ...])]

<1-4 sentences: the change or the run's purpose, plus any caveat a
reader needs to interpret the numbers — host drift, a re-measure, a
definition change.>

Results: a short list, or a small table when the entry is an A/B.
Status: <suite/lint state when recorded> — omit when nothing was run.
```

## Best-known results

The canonical suite (`mizu-bench.py`, plus the cross-language map's
`crosslang-map-bench.py` rows), best recorded value per row. One-off
probes and A/B isolations stay in the log.

| row | best | measured |
|---|---|---|
| sequential rt, channel | 0.3 us/rt | 2026-10-03 |
| sequential rt, pool | 0.7 us/task | 2026-09-28 |
| pipelined, channel | 4.68M rt/s | 2026-10-03 |
| pipelined, channel batch | 10.1M rt/s | 2026-08-24 |
| pipelined, pool | 2.48M tasks/s | 2026-09-28 |
| pipelined, pool batch | 4.09M tasks/s | 2026-11-08 |
| payload 8,000 B | 2.1 us/task | 2026-11-08 |
| payload 800,000 B | 55.4 us/task | 2026-11-08 |
| payload 8,000,000 B | 101.7 us/task | 2026-11-08 |
| fan-out x 2000, 4 workers | 296.4k tasks/s | 2026-11-08 |
| streaming, channel batch | 21.6M msg/s | 2026-10-03 |
| map trivial f | 0.06 us/elt | 2026-11-06 |
| map trivial f, template copy / view | 752k / 742k tasks/s | 2026-10-03 |
| map trivial f, seed | 1.7 us/elt | 2026-11-06 |
| map trivial f, stream | 0.17 us/elt | 2026-11-08 |
| map trivial f, prepared | 0.05 us/elt | 2026-11-06 |
| map winsum x 2000 | 715.0k tasks/s | 2026-11-08 |
| map winsum x 20000 | 759.3k tasks/s | 2026-11-06 |
| map skewed f x 4000 | 5.1 ms wall | 2026-11-08 |
| stage memcpy | 100.5 us/send | 2026-08-30 |
| stage identity | 101.6 us/send | 2026-08-30 |
| stage int64 | 99.7 us/send | 2026-09-11 |
| frame 10col relay, unmodified REF | 5.3 us/rt | 2026-11-08 |
| frame 10col relay, one computed col | 201.0 us/rt | 2026-11-08 |
| frame strcol relay, unmodified string REF | 2,410.8 us/rt | 2026-10-03 |
| stage widen (foreign pair) | 241.5 us/send | 2026-10-03 |
| stage masked (foreign pair) | 97.3 us/send | 2026-10-01 |
| stage masked int64 (foreign pair) | 97.6 us/send | 2026-10-03 |
| stage masked+scan (foreign pair) | 136.0 us/send | 2026-10-03 |
| stage str list MIZS (foreign pair) | 5,350.6 us/send | 2026-10-03 |
| stage f64 matrix MIZH (foreign pair) | 104.1 us/send | 2026-10-03 |
| echo f64 matrix MIZH (R echo) | 432.6 us/rt | 2026-10-03 |
| crosslang map trivial fn, spec | 0.1 us/elt | 2026-10-01 |
| crosslang map trivial fn, spec template | ~0 us/elt | 2026-10-01 |
| crosslang map trivial fn, spec seed | 0.2 us/elt | 2026-10-01 |
| crosslang map trivial fn, spec prepared | 0.1 us/elt | 2026-10-01 |
| crosslang map compute, spec | 335k tasks/s | 2026-10-03 |
| submit plan k = 0 (W2 row) | 82.7 us/task | 2026-11-08 |
| submit plan k = 1 (W2 row) | 99.1 us/task | 2026-11-08 |
| submit plan k = 4 (W2 row) | 97.6 us/task | 2026-11-06 |
| submit plan k = 16 (W2 row) | 147.8 us/task | 2026-11-08 |
| event-driven, channel armed | 2.67M rt/s | 2026-11-08 |
| event-driven, channel recv_async | 834k rt/s | 2026-11-08 |
| event-driven, channel recv_async cancel | 1.00M cancel/s | 2026-11-08 |
| event-driven, pool collect_async | 1.65M tasks/s | 2026-11-08 |
| event-driven, pool collect_all_async | 3.67M tasks/s | 2026-11-08 |
| event-driven, pool 64 waiters | 3.44M tasks/s | 2026-11-08 |
| executor outstanding 500 / 1,000 / 2,000 / 4,000 | 80.6k / 79.7k / 82.1k / 83.2k tasks/s | 2026-11-08 |
| executor sync | 4.5 us/task | 2026-11-08 |

## Run log

Dated outcome records, oldest first — the source of truth for the table
above. Append new dated outcomes at the bottom.

## 2026-08-24 — fast-path codec + STR1 producer (phases 1-2)

The C codec (bool/int/float/str/bytes + flat containers) and the STR1
string tier landed; payload tiers unchanged.

Results (before -> after):

| scenario | before | after |
|----|----|----|
| sequential rt, pool | 3.7 µs/task | 2.4 µs/task |
| sequential rt, channel | — | 0.4 µs/rt |
| pipelined, pool | 470,000 tasks/s | 512,295 tasks/s |
| pipelined, pool batch | — | 541,327 tasks/s |
| pipelined, channel | — | 4,707,341 rt/s |
| pipelined, channel batch | — | 10,140,103 rt/s |
| payload 8,000 B | — | 11.3 µs/task |
| payload 800,000 B | — | 94.8 µs/task |
| payload 8,000,000 B | 1.0 ms/task | 976.4 µs/task |
| fan-out x 2000, 4 workers | — | 179,539 tasks/s |
| streaming, channel batch | — | 21,529,298 msg/s |
| map x 2000, 4 workers | 12 ms | 11.4 ms |

Channel ping-pong micro-benchmark (send + recv, best of 3,000),
isolating the staging path before/after the codec:

| payload | before (pickle) | after (codec) |
|----|----|----|
| `42` | 2.51 µs/rt | 0.48 µs/rt |
| `"hello"` | 2.59 µs/rt | 0.49 µs/rt |
| `[1, 2.5, "x"]` | 2.61 µs/rt | 0.59 µs/rt |

## 2026-08-24 — SHM_VEC zero-copy view tier (phase 3)

The producer stages eligible buffers past the zc floor as an REIH layout
region (channel: past the raw floor, the arena copy serving below); the
consumer wraps the region as a read-only buffer view (numpy
`frombuffer` / memoryview) instead of copying out. The 8 MB channel
round trip halves (two memcpy elided); the 800 KB channel case regresses
slightly — each view pays a fresh open/mmap/munmap per receive, no
consumer-side view cache yet (phase 4's follow-up).

Results (best of 5 x 30 round-trips, identity task / echo peer):

| scenario | before (copy tiers) | after (views) |
|----|----|----|
| pool, 8 MB payload | ~848 µs/task | ~731 µs/task |
| channel, 8 MB payload | ~1,210 µs/rt | ~539 µs/rt |
| channel, 800 KB payload | ~38 µs/rt | ~49 µs/rt |

## 2026-08-24 — consumer-side view cache (phase 4)

A per-handle name-keyed LRU cache of shared mapping owners: repeat
receives of a live region pay a counted add instead of a fresh
open/fstat/mmap, and an evicted owner's mapping closes only when its
last view is gone.

Results (same ad-hoc measurement as phase 3, echo peer): channel 800 KB
payload ~19 µs/rt; channel 8 MB payload ~215 µs/rt.

## 2026-08-24 — structured task-frame codec (phase 5)

Pool task payloads stage as a compact-codec task stream: fn by (module,
qualname) reference — or its own protocol-4 pickle when not
referenceable — and args/kwargs as codec scalars, None, one flat
container level, or buffer leaves (past the zc floor a SHM_VEC region
referenced by name, the BUFREF leaf, one per frame). A BUFREF argument
arrives as a read-only view. The 8 MB row lands at the channel's
zero-copy figure (~215 µs), as designed.

Results (phase 1-2 -> phase 5):

| scenario | phase 1-2 | phase 5 |
|----|----|----|
| sequential rt, pool | 2.4 µs/task | 0.7 µs/task |
| pipelined, pool | 512,295 tasks/s | 1,923,786 tasks/s |
| pipelined, pool batch | 541,327 tasks/s | 1,223,383 tasks/s |
| payload 8,000 B | 11.3 µs/task | 2.5 µs/task |
| payload 800,000 B | 94.8 µs/task | 58.3 µs/task |
| payload 8,000,000 B | 976.4 µs/task | 209.5 µs/task |
| fan-out x 2000, 4 workers | 179,539 tasks/s | 189,943 tasks/s |
| map x 2000, 4 workers | 11.4 ms wall | 174,129 tasks/s |

## 2026-08-24 — template output area (map improvements, phase 1)

`Pool.map(template=...)` stages an n x m output area in the map region:
runners write results in place and collect is one gather memcpy — or
none with `collect="view"`. No regression on the plain path (442k/478k
vs the 458k full-suite baseline; run-to-run jitter). The remaining gap
to in-process scaling is the Python element loop — phase 3's target.

Results (best of 3, 4 workers, winsum x n -> n x 2 float64):

| scenario | baseline (plain) | phase 1 |
|----|----|----|
| map n=2,000 | 442,188 elts/s | 645,604 copy / 651,935 view |
| map n=20,000 | 477,594 elts/s | 696,252 copy / 689,039 view |

## 2026-08-24 — prepared maps (map improvements, phase 2)

`Pool.map_prepare` stages once into a persistent region;
`Pool.map_run` re-arms in O(1) (the generation bump fences a prior
run's stragglers) and reuses the workers' name-keyed context cache; a
view-collected run restages fresh. The fixed stage/unlink cost was the
whole call at small n with a trivial fn; with the ~5 µs winsum task,
compute dominates and the gain is the expected ~46 µs/map of staging
overhead.

Results (best of 5 x reps, 4 workers):

| scenario | plain | prepared |
|----|----|----|
| identity n=50 (x50) | 101.0 µs/map | 19.6 µs/map |
| identity n=200 (x50) | 74.1 µs/map | 31.2 µs/map |
| winsum n=200 (x20) | 620 µs/map | 574 µs/map |

## 2026-08-25 — stdlib comparison

`mizu-stdlib-bench.py` (then rei-stdlib-bench.py) against the stdlib
concurrent.futures executors, matched scenarios; most favourable of
four runs per row (the thread-pool column best of two, added same day).
Threads take the payload rows (an in-process handoff serializes
nothing) and the trivial-task rows beat the process pool (no pickling),
but the GIL caps CPU-bound work at one core: fan-out and map land at
~40k tasks/s against 277k/468k on 4 workers.

Results (Python 3.14.2, macOS arm64):

| scenario | pyrei | cf process pool | cf thread pool |
|----|----|----|----|
| sequential rt | 0.7 µs/task | 90.2 µs/task | 9.5 µs/task |
| pipelined, pool | 2,191,501 tasks/s | 18,728 tasks/s | 407,192 tasks/s |
| payload 8,000 B | 2.4 µs/task | 115.7 µs/task | 9.6 µs/task |
| payload 800,000 B | 59.4 µs/task | 385.3 µs/task | 9.4 µs/task |
| payload 8,000,000 B | 202.9 µs/task | 3,284.2 µs/task | 9.3 µs/task |
| fan-out x 2000, 4 workers | 277,047 tasks/s | 16,270 tasks/s | 39,921 tasks/s |
| map trivial f x 10000 | 0.4 µs/elt | 69.4 µs/elt | 2.5 µs/elt |
| map winsum x 2000 | 468,329 elts/s | 15,533 elts/s | 39,627 elts/s |
| map skewed f x 4000 | 6.7 ms wall | 261.2 ms wall | 49.6 ms wall |

## 2026-08-30 — conversion staging pass (Arrow / numpy interop)

The conversion pass (every fixed-width numeric dtype crosses; Arrow
validity bitmaps honored) fuses conversion into the stage copy. The
word-wise masked loop lands at ~1.1x memcpy on the all-valid-bitmap
shape (the ~1.2x target): the bitmap word reads are 1/64th of the data
volume and the 64-lane runs stay memcpy. The masked int32 row pays for
the genuine-INT_MIN count (a second vectorized pass over the
destination). Widening rows are scalar conversion loops — ~2.8x memcpy,
acceptable: those dtypes could not cross at all before.

Results (8 MB one-way sends against an acking sink peer, so the
measured cost is the send-side stage; best of 10 x 50 sends):

| row | us/send | vs memcpy |
|----|----|----|
| stage memcpy (bytes) | 100.5 | 1.00x |
| stage identity (float64) | 101.6 | 1.01x |
| stage widen (int64 -> float64, range-checked) | 278.4 | 2.77x |
| stage masked (float64, all-valid bitmap, null_count -1) | 113.5 | 1.13x |
| stage masked+scan (int32, + INT_MIN count) | 138.4 | 1.38x |

## 2026-09-11 — int64 is a native wire tag (REI_TYPE_INT64)

int64 buffers take the memcpy raw tier (`wire_type_of` maps 8-byte
'l'/'q'); the range-checked widening loop survives only for uint64, and
the masked-Arrow int64 fill writes the INT64_MIN sentinel via the new
`cvt_fill_na` case. Stage int64 lands at identity-tier parity, replacing
the per-element range-check loop whose cost the uint64 widen row still
shows.

Results (1e6-element sends): stage int64 99.7 us/send (float64 identity
102.5, bytes memcpy 107.8), masked int64 115.8 (masked float64 116.5),
uint64 widen 257.0.

Status: full suite 190 pass, 1 skip (1 pre-existing environment skip).

## 2026-09-11 — int64 map templates (regression check) (161b358)

int64 templates admitted (tag 32; "q" tag-format row; m == 1 scalar
conversion) — a no-op for the existing tiers. Section 7 settles into
the recorded bands across two runs (run 1's masked-float64 outlier
169.2 settled to 119.6 — transient).

Results: stage int64 111.9/102.0 us/send (memcpy 101.1/104.6, identity
108.2/106.5), masked int64 110.7/117.2, widen 306.5/290.9 (uint64, the
untouched loop), masked+scan 140.5/140.6; map winsum 2k 480,745
tasks/s, 20k 473,275, template copy/view 679k/692k, skew 7.3 ms wall.

## 2026-09-11 — stdlib comparison refresh

Most favourable of four runs per row, as on 2026-08-25. Pipelined
throughput improved (worst of the four runs 2.24M); all other rows
inside their recorded bands. README tables updated.

Results (Python 3.14.2):

| scenario | pyrei | cf process pool | cf thread pool |
|----|----|----|----|
| sequential rt | 0.7 µs/task | 94.2 µs/task | 8.9 µs/task |
| pipelined, pool | 2,391,677 tasks/s | 18,517 tasks/s | 405,020 tasks/s |
| payload 8,000 B | 2.7 µs/task | 111.9 µs/task | 9.6 µs/task |
| payload 800,000 B | 59.8 µs/task | 382.5 µs/task | 9.4 µs/task |
| payload 8,000,000 B | 212.2 µs/task | 3,532.3 µs/task | 8.6 µs/task |
| fan-out x 2000, 4 workers | 284,929 tasks/s | 16,168 tasks/s | 40,509 tasks/s |
| map trivial f x 10000 | 0.4 µs/elt | 70.3 µs/elt | 2.6 µs/elt |
| map winsum x 2000 | 453,446 elts/s | 15,333 elts/s | 40,399 elts/s |
| map skewed f x 4000 | 6.8 ms wall | 263.0 ms wall | 49.3 ms wall |

## 2026-09-13 — Phase B policy extraction (regression check) (librei f1296a7; pyrei 3a9c12b)

`rei_stage_raw` + the `rei_morsel_*` map protocol adopted. Golden
(hdr, payload) capture A/B: 264/264 rows identical pre/post.

Results: payload 8k 2.5 us/task, 800k 59.5, 8M 217.0; streaming 21.3M
msg/s; map trivial f 0.4 us/elt, template copy/view 679k/694k tasks/s,
skew 7.1 ms wall; stage rows identity 102.3 / int64 105.6 / widen
256.9 / masked 131.6 / masked+scan 149.2 us/send — all inside the
recorded bands.

Status: 197 pass (incl. crosslang against the Phase-B rei), ruff +
pyrefly clean.

## 2026-09-13 — aux decode pair + symbol drop (regression check) (librei 1cedd42/5d1b54c; pyrei 0a5417d)

`rei_aux_type` / `rei_aux_hi` decode pair adopted at the two read-side
aux sites. The stage column's ~13% absolute lift is this run's
bandwidth draw (identity 115.4 = memcpy 115.5 us/send); the normalized
costs sit inside their recorded spread.

Results: rt 0.4 channel / 0.7 pool us; pipelined pool 2.31M tasks/s;
payload 8k/800k/8M 2.7/58.7/211.3 us; streaming 21.1M msg/s; map
trivial 0.4 us/elt, template copy/view 690k/698k tasks/s, skew 7.3 ms
wall; fan-out 255.7k.

Status: 197 pass, ruff + pyrefly clean.

## 2026-09-26 — keeperless wire flag (after-measurement)

Every INLINE frame stamps the stager's keeperless claim
(MIZU_AUX_F_KEEPERLESS) — stage_bytes (pickle plus the spilled codec
and task streams), stage_codec's direct inline path, and the task-frame
inline path. The behavioral change is pickle-tier results only:
'P'-magic frames were already probe-keeperless, so pickle collects
alone stop waking the producing worker's keeper sweep.

Results (1 worker, sequential submit+collect): rt rows before
3.73-3.75 / after 4.03-4.05 us/task pickle, 2.56-2.60 / 2.71-2.77
codec — both rows up together, host drift (a concurrent rchk
container). The parked-collect isolation (3000 results staged, the
worker drained and parked, collects timed alone) carries the signal:
pickle collects 2.14-3.20 -> 0.93-1.55 us, the codec control flat at
0.40-0.99 on both builds — the spurious cross-process keeper wake is
gone.

Status: 207 pass.

## 2026-09-28 — decline and batch-receive fixes (after-measurement) (libmizu 151fcae)

recv_batch keeps its consumed prefix; collect_all claims only the
reported handle; leave fails an announced in-flight claim; pymizu-side:
py_chan_read consumes declined channel reads, buffer subclasses keep
the pickle path, recv_batch clears the deferred read's exception on a
prefix return. All changes touch failure branches only; the C.3 gate is
one pointer compare on the buffer branch.

Results: rt 0.4 channel / 0.7 pool us; pipelined pool 2.48M tasks/s;
payload 8k/800k/8M 2.9/65.2/219.0 us; streaming 20.8M msg/s; map
trivial 0.4 us/elt, template copy/view 692k/696k tasks/s, skew 7.0 ms
wall; fan-out 275.4k; stage masked rows 121.6/110.4/146.4 us/send
(their recorded spread).

Status: 218 pass, ruff + pyrefly clean.

## 2026-09-30 — the interchange codec (1.2, before/after acceptance)

The 'I' interchange codec landed (foreign-handle interop,
DeclinedError, Frame, the Arrow stream front-end); same-language
staging is unchanged by construction (one peer-language branch at
stage), with the conversion pass and Arrow front-ends moving to foreign
handles only.

Results (mizu-bench.py, before (5d7f001) -> after): sequential rt
0.4/0.4 channel, 0.7/0.7 pool us; pipelined 3.78M -> 4.34M rt/s, pool
2.24M -> 2.08M tasks/s (within the recorded jitter band); payloads
2.6/62.1/113.3 -> 2.4/59.9/108.3 us; streaming 20.4M -> 20.5M msg/s;
map rows flat; stage memcpy/identity/int64 112/114/108 -> 111/106/105
us; the widen/masked rows re-measured on a foreign pair (their new
home): widen 251.5, masked 107.0/106.1, masked+scan 166.1 us/send.

Status: 254 pass, 4 skip.

## 2026-10-01 — the cross-language map (Phase 5, acceptance record) (370df3a/17722cf)

A `pymizu.call` spec as `Pool.map()`'s `fn` on any pool: the 'I'
descriptor (`list[task, x | nil]`), kind-2 runner tasks off the
submit-private `_RunnerFrame`, and the R-runner-shape normalization
(`_runs_to_spans`) at collect (mizu a85ce19/b655992, libmizu c6f8cd2 —
DESIGN.md's kind-2 runner row). The dedicated comparison is
`crosslang-map-bench.py`, matched regime-for-regime with
`mizu-stdlib-bench.py`'s map scenario. Cross-language maps land at or
under the native map's per-element cost and two orders of magnitude
under the task-per-element baseline; the compute-row delta is
per-element language cost (R's runif+sum vs numpy's winsum), not
transport.

Results (trivial fn over 10k float64, 4 workers): in-process ~0 us/elt
(the anchor); cf process pool 71.1 (the task-per-element model's cost,
what the map amortizes); pymizu native 0.4; spec 0.1 — cheaper than the
native row here, whose per-element numpy scalars pay the pickle path;
spec template ~0; spec seed 0.2 (L'Ecuyer-CMRG per element on the R
side); spec prepared 0.1. Compute regime (n=2000, ~10 us elements):
in-process 216k, native 449k, spec 325k tasks/s.

Status: 351 pass, 5 skip; ruff + pyrefly clean.

## 2026-10-01 — Phase 5 full-bench no-regression run

Full mizu-bench.py at the cross-language map's landing
(370df3a/17722cf) against the 2026-09-30 record. The
only hot-path addition is one exact-type pointer check in stage_impl
(the `_RunnerFrame` branch), so the expectation was flat, and it is.

Results: sequential rt 0.4 channel / 0.8 pool us (the 0.7-0.8 band);
pipelined 4.53M rt/s channel (batch 9.28M) / 1.97M tasks/s pool (the
2.0-2.5M band, batch 1.25M); payloads 3.1/63.9/111.4 us (the 2.4-3.1
spread at 8 KB); streaming 20.7M msg/s; fan-out 259.4k; map trivial 0.4
us/elt, seed 1.9, prepared 0.4, template copy/view 695k/716k tasks/s,
skew 6.8 ms wall, 20k-element row 481k; stage rows within their
recorded spread (memcpy 124.3, identity 102.3, int64 133.5; foreign
widen 275.4, masked 97.3, masked int64 136.9, masked+scan 136.8).

Status: 351 pass, 5 skip; ruff + pyrefly clean.

## 2026-10-01 — task arguments by reference (F1)

The 'I' task stream gains the ref leaf (0x13): a received view re-sent
as an argument crosses as its identifier (REFHELD at emit, the spec
pinned via the new drop hook to the claim-side release), and one fresh
layout-eligible buffer argument stages a single SHM_VEC checkout (the
frame-plan discipline: the size pass picks the first candidate whose
remainder fits inline, abandon-and-replan on a checkout failure).
Emission gates on the new MIZU_CAP_TASKREF (bit 3); a spec carrying a
ref candidate to a pool without the bit declines locally. CPython's
refcounting frees argument views at exec completion (D5 is free here).

Results (8 MB float64 argument to a pool task, best-of-3): Python
submitter -> R workers — SHM_VEC (a fresh array) 1.14 ms/task and REF
(a received view re-sent) 1.03 ms against the pre-F1 copy's 1.59/1.51
ms (25-32% off, the R worker's per-task floor dominating); Python pool
flat at 0.23 ms (private frames untouched). R submitter -> Python
workers — REF 0.12 ms (the copy's ~0.38), SHM_VEC 0.50 ms against
0.38 ms for the copy (mizu's Arrow-ready validity scan, halved by its
new pre-scan gate).

Status: 358 pass, 5 skip; ruff + pyrefly clean.

## 2026-10-01 — the per-column frame REF (F2)

The MIZL writer gains per-column provenance (directory tag 33, the
remote leaf): a relayed frame whose columns match registered exports
same-index crosses with one remote leaf per matched column — the
identifier span, the referenced leaf's attrs size and validity claim as
resolved, no body, blob or bitmap — where pre-F2 every unmatched frame
wrote every column. The whole-frame REF fast path is unchanged (all
columns matched, no new bit needed); a partial match joins
MIZU_CAP_MIZL_REF (bit 4) to the frame conjunction, and a peer short of
it gets full layout leaves. The reader's frame path resolves a remote
column onto its own per-column hold (a fresh mapping + counted loan,
released pure-C), the generic tree walk through the checked resolve.

Results (10-column frame, 1e6 rows of float64, R -> polars -> R,
best-of-3): unmodified relay (the whole-frame REF, the regression
guard) 6.5 us/rt, one computed column via polars (1 layout leaf + 9
remote leaves) 434.5 us/rt against the pre-F2 ten-column layout write
(~4 ms/rt measured R-side, same-language flat 17.0 ms/send — pymizu's
per-send stream pull included). New rows on the best-known table.

Status: 363 pass, 5 skip; ruff + pyrefly clean.

## 2026-10-02 — the polars string-column REF (F3)

The export-provenance match verifies a re-viewed string column
read-only instead of demanding buffer identity: polars exports strings
only as string_view, and an unmodified column comes back with the
<= 12-byte values inline in the 16-byte views and the longer ones
pointing into the recorded leaf's packed bytes (the single data buffer
rebased to the first long row). Short rows verify by value, long rows
by row-byte pointer identity with the row's own span, the null set by
tail-masked bitmap equality, with an O(ndata) provenance gate on the
data buffers ahead of the row scan. Both REF paths take the result: the
whole-frame REF, and the per-column remote leaf — where a verified
column pins its record only when it carries a data buffer, an
all-inline hit standing only beside a pinning column of the frame (the
write pass dereferences the entry after the registry lock drops; the
prune is mutation-checked by the all-short provenance test). Failure
modes all degrade to the layout write.

Results (9 float64 columns + 1 string column, 1e6 rows, strings mixed
<=12B and >12B, R -> polars -> R, best-of-3): 4,996-5,114 us/rt pre-F3
(the string column's MIZL write per relay) -> 2,437-2,532 us/rt (the
verification scan + REF; the balance is the pre-existing 'I' size pass
ixs_run_frame runs ahead of any REF attempt). Regression guards flat:
unmodified 10-col REF relay 6.0-6.4 us/rt, one computed column
402-419 us/rt. New row on the best-known table.

Status: 372 pass, 5 skip; ruff + pyrefly clean.

## 2026-10-02 — the top-level MIZS string writer (F4)

A large `list[str | None]` sent to an R peer now stages as one MIZS
layout write into a spill region (the peer wraps a zero-copy
string-vector view), where the 'I' writer paid a full value copy of the
tree. The tier is foreign-only and gated on MIZU_CAP_MIZS, the zc floor
(on the whole block, not the string bytes alone), and no churn; every
decline form — a no-cap peer, a below-floor list, a non-str element, a
region failure — keeps the 0x0b copy. None is the validity-bitmap NA,
"" a zero span with the bit set; CE_UTF8 is the only encoding written.

Results (1e6-element list[str], ~20 MB block, in-process foreign pair,
best of 3 x 10 sends): stage str list 0x0b 96,976.3 us/send -> stage
str list MIZS 5,455.5 us/send (17.8x). New row on the best-known table.

Status: full suite + the new crosslang rows pass; ruff + pyrefly clean.

## 2026-10-02 — the attributed-MIZH dim-array writer (F5)

An F-contiguous dim array past the zc floor sent to an R peer now stages
as one flat MIZH layout write plus the {dim} attribute blob (the peer
wraps a zero-copy matrix view; R's re-send is a REF), where the 'I'
writer paid an attr-tag value copy at every size. The tier is
foreign-only and gated on MIZU_CAP_ATTRS, the floor, no churn, and a
strict F-contiguity walk — C-order and strided arrays keep the 'I'
writer's value-exact reordering copy (a zero-copy tier memcpys the
buffer, and a C-order flat stamped dim would arrive the transpose).

Results (8 MB float64 1024x1024, best of 3 x 10): stage f64 matrix 0x0f
324.1 us/send -> stage f64 matrix MIZH 111.5 us/send — at parity with
the 103 us plain-memcpy anchor, so no per-element work (the send-side
pair is not ~parity because the 0x0f row's acking read materializes the
copy while the MIZH row's wraps a view). The echo pair through an R
peer, the relation of record (the 0x0f row sends the C-order twin — a
real R peer always advertises ATTRS; the wire shape is the pre-F5
attr-tag copy either way, its gather standing in for the pre-F5 bulk
convert): echo f64 matrix 0x0f 5,675.2 us/rt -> echo f64 matrix MIZH
482.5 us/rt (11.8x — R's receive wraps a view, the return a REF). Two
new rows on the best-known table.

Status: full suite + the new crosslang rows pass; ruff + pyrefly clean.

## 2026-10-03 — the map-orchestration refactor (no-regression run)

The one-shot and prepared map entries unified behind shared helpers
(`_Plan` validation, `_probe_x`, `_write_desc`, `_morsel_geometry`,
`_gather_out`, the `_seed_pair` shape gate), the C handle constructors
behind shared alloc/token/ident helpers, and explicit bounds guards
ahead of both remote-leaf stack copies — no hot-loop change (the
element loop, stage_impl, the verbs), so the expectation was flat, and
it is; several rows landed new bests on a quiet machine. Full
mizu-bench.py (R rows included) plus crosslang-map-bench.py.

Results: sequential rt 0.3 channel / 0.7 pool us; pipelined 4.68M rt/s
channel (batch 9.44M) / 2.08M tasks/s pool (batch 1.23M); payloads
3.2/60.3/107.2 us — the 8 KB best-of-3 caught a slow set (a best-of-7
probe: 2.6 best, 2.8 median, inside the 2.4-3.1 spread); streaming
21.6M msg/s; fan-out 295.3k; map trivial 0.4 us/elt, seed 2.0,
prepared 0.4, template copy/view 752k/742k tasks/s, winsum 478.9k,
20k row 506.6k, skew 6.9 ms wall; stage rows within their spread
(memcpy 108.8, identity 107.4, int64 100.3; foreign widen 241.5,
masked 99.9, masked int64 97.6, masked+scan 136.0); spec map rows flat
to better (trivial 0.1 us/elt, template ~0, seed 0.2, prepared 0.1,
compute 335k tasks/s).

Status: 395 pass, 5 skip (pandas absent); ruff + pyrefly clean.

## 2026-10-03 — the core dedup + channel fork guard (no-regression A/B)

The libmizu core's duplicated blocks moved into internal.h static
inlines (mizu_binding_check, mizu_wait_ms, mizu_shm_set_name; the
submit_many double clock read hoisted) and the channel gained the fork
guard — one cached-pid compare per verb entry — vendored at libmizu
909563b. The binding side added the _TOKEN_RE share, the _r.py spawn
helper, and _run_batch's call_one closure (the only per-element
change: one Python call per map element). 2x2 A/B on one host: two
runs per build, baseline HEAD (core 1efb883) against the working tree
(core 909563b), means compared against each row's within-build spread.
The pure-Python in-process anchors swung up to 18% run-to-run (the
host drift floor); every A -> B delta landed smaller than its row's
spread, and the trivial-f map rows (the call_one site) read
bit-identical.

Results (A -> B, means of 2; spread = max within-build pairwise):
sequential rt 0.4 -> 0.4 us channel (spread 0%) / 0.9 -> 0.8 pool
(71.4% jitter); pipelined 4.47M -> 4.45M rt/s channel (batch 9.13M ->
9.20M), 2.158M -> 2.164M tasks/s pool (batch 1.218M -> 1.212M);
payloads 2.7 / 63.0 / 111.4 -> 2.7 / 60.6 / 109.4 us (spreads
3.8-6.0%); streaming 20.46M -> 20.84M msg/s; fan-out 289.0k -> 279.0k
(spread 10.4%; in-process anchor -0.4%); map trivial 0.4 -> 0.4 us/elt
(spread 0%), seed 1.9 -> 1.9, prepared 0.4 -> 0.4, template copy/view
739.9k/737.6k -> 721.1k/723.0k (spreads 2.9-4.7%), winsum 463.8k ->
463.5k, 20k row 486.3k -> 483.8k, skew 7.05 -> 7.25 ms wall (spread
5.7%); stage rows within spread (memcpy 105.8 -> 102.7, identity 103.7
-> 101.3, int64 106.1 -> 106.5; foreign widen 249.3 -> 258.9 vs 5.6%
spread, masked 106.0 -> 105.3, masked int64 112.4 -> 103.7, masked+scan
145.8 -> 155.6 vs 11.9% spread); arg/frame R rows flat (<=1.6%).

Status: 395 pass, 5 skip (pandas absent); ruff + pyrefly clean.

## 2026-10-04 — cross-language channel bench harness + difftime interchange

First run of benchmarks/crosslang-channel-bench.py (new: Python host
against an R echo peer — the mirror driver of mizu's dev/bench/
crosslang-channel-bench.R, which ran the same day) and the first
measurement of the difftime <-> timedelta64[us] interchange that landed
with it. Rows stay log-only as the suite's first record.

Results (median us/rt): None / scalars 0.3; 8 KB float64 0.8 (copy echo
1.0); 800 KB float64 13.6 (copy 59.9); 8 MB float64 108.3 (copy 281.8);
10k strings 24.4; 100k bool+None list 4195.0 (the object walk — numpy
bool_ arrays cross at memcpy cost); 10k datetime64[D] 22.2; 100k
timedelta64[us] 64.9; pipelined 8 KB float64 356.3k rt/s. Host-direction
asymmetries against the R-driven mirror (same host): R->Python string
layout writes run ~6x the Python->R ones (188 vs 24 us/10k — the
CHARSXP walk against PyUnicode's ready UTF-8) and R->Python vector
layout writes pay the NA validity scan a numpy buffer stamps free
(8 MB: 391 vs 108 us/rt).

Status: full suite 396 pass, 0 fail (5 skips); ruff + pyrefly clean.

## 2026-10-04 — difftime frame columns (FCOL_TD / Arrow duration[us])

The frame half of the difftime interchange: a difftime column is an
owned i64-us column (Arrow duration[us] on the Frame's export,
timedelta64[us] from to_dict()); Arrow duration[unit] columns read as
difftime. A pre-existing latent bug surfaced and is fixed:
fcol_ensure_export double-counted nulls for a lazy-scanned column with
NAs on the Arrow export. Numbers ride the R-driven mirror's log (mizu
bench notes, same date).

Status: full suite 396+3 pass (3 signal-timing flakes under a
concurrent benchmark load, green in isolation), 0 fail; ruff + pyrefly
clean.

## 2026-10-04 — MIZS write-asymmetry instrument rows + baseline (75f09c7)

The Python half of the send-only instrument (see the mizu bench notes,
same date): an acking R sink peer and the 10k-string send-only row in
benchmarks/crosslang-channel-bench.py. The R side's instrument rows
(sink, UTF-8-marked variant, same-language rows) and the full context
are in mizu's dev/bench/notes.md entry of the same date.

Baseline (the committed pre-fix build): Python-hosted 10k strings
21.7 us/send against the R sink (22.7 us/rt echo in the same run) —
the reference the R-side send-only cost (165.3 us/send, ~7.6x) is
measured against; the fixes' acceptance target is the R row within
~1.5x of this number.

Status: rows report-only; no suite run (no code change).

## 2026-10-04 — R-side MIZS write fixes land; the Python host is flat

The R side of the string-write asymmetry is fixed in mizu (the gate
flag read, the fused probe/gate walk, the bulk section fills, the
STRING_PTR_RO walks — the full account is mizu's dev/bench/notes.md
entry of the same date). R-sent MIZS regions now arrive with the
encoding section bulk-filled CE_UTF8 (NA bytes zero), so pymizu's
readers skip the CE_NATIVE validation defense; frame string columns
gate R-side now, so a latin1 column reaches Python translated via the
'I' copy instead of failing at the reader.

Results (this driver re-run against the fixed mizu): every row in band
— 10k strings 23.6 us/rt echo (22.7 baseline), 20.9 us/send sink
(21.7); the Python stage is unchanged code. The R-hosted mirror moved:
188 -> 87 us/rt echo, 165 -> 65 us/send sink.

Status: full suite 398 pass, 5 skip; crosslang 96 pass against the
fixed mizu; ruff + pyrefly not re-run (no code change).

## 2026-10-04 — W1: the 'I' value writer goes one-walk optimistic

`pymizu_ix_stage` (and the shared `IXW_PUT` walk discipline) ported to
mizu's count-past-limit pattern: the first walk writes into the slot
payload while sizing (inline-fitting values stage in one walk); past
the budget the writes stop at the limit and the ARENA/SHM_RAW
reservation takes the second walk directly — the `malloc(n)` temp and
the second full memcpy are gone (`stage_bytes` split: `stage_reserve` +
one memcpy). No wire change: the golden corpus passes byte-identical.

Results (interleaved A/B on this host, old -> new, best-of runs; the
in-process foreign pair measures the stage alone, the sink rows add
the R peer's recv + ack):

| row | old | new |
|----|----|----|
| stage-only, small nested (inline) | 911 ns | 528 ns |
| stage-only, 1 MB string (spill) | 27.7 us | 12.1 us |
| stage-only, 100 KB nested tree (spill) | 872.6 us | 870.2 us |
| stage-only, 10k strings 'I' caps=0 (spill) | 721.6 us | 709.8 us |
| crosslang sink, 100 KB nested tree (3 reps) | 1198.8 us | 1206.8 us |

The one-walk inline path gains ~40%; the memcpy removal gains ~2.3x
where the payload is memcpy-dominated (the 1 MB string; the suite's
f64 matrix 0x0f row likewise printed 311.0 -> 219.8 us on the
before/after full runs). The ~100 KB nested tree is flat: its cost is
the two remaining tree walks (12.6k nodes x 2, kept by design for
spilled values), not the removed malloc + memcpy (~2 us at 100 KB).
The full crosslang bench's before/after runs swung +-9-37% on
individual rows (an untouched MIZS row included); the interleaved A/B
shows the builds identical there — host drift, not a regression.

Status: full suite 402 pass, 5 skip (4 new rows: the carriers,
decline-mid-write, warnings-as-errors, arena-pressure spill); ruff +
pyrefly clean.

## 2026-10-04 — W2: the zc-selection fold (the task plan walks once)

The 'I' task submit's per-candidate size-pass loop folded into the one
record walk: the walk itself records the first 16 SHM_VEC candidates
with their by-value subtree sizes and flips to count-only at the
first, so selection is arithmetic (total - s_i + the conservative
ref-leaf reservation vs inline_max) and a candidate-free inline spec
stages in one walk, a by-reference one in two — was k+3 (the probe,
the per-candidate size passes, the retry-block count, the write).
ixp_probe and the retry: block are gone; the spill path writes
straight into mizu_py_stage_reserve's reservation (no malloc, no
second memcpy). The wire is unchanged; selection A/B-verified
identical to the pre-fold implementation over 60 randomized spec
trees plus the 9-row candidate matrix (R-worker view probes, the
adjusted == inline_max boundary inclusive both sides).

Results (plan-cost A/B on the filler+candidates row — a 3000-float
by-value list plus k 40 KB buffers, submit+collect):

| k | before | after |
|----|----|----|
| 0 | 307.3 | 301.4 |
| 1 | 433.6 | 296.3 |
| 4 | 778.3 | 312.0 |
| 16 | 2225.3 | 431.1 |

Flat in k (was linear — the per-candidate passes re-walked the whole
argument tree); the residual growth is the spill bytes themselves.
The F1 guard rows held (SHM_VEC 1,144.1 -> 1,149.6 us/task, REF
1,037.8 -> 1,052.0 us/task, inside host drift); the full bench's
other rows unmoved.

Status: full suite 403 pass, 5 skip (new candidate-matrix test in
test_crosslang_pool.py); ruff + pyrefly clean.

## 2026-10-05 — exact-type scalar pre-dispatch in stage_impl

Same-language exact-type bool/int/float now stage straight out of
stage_impl (stage_scalar_fast): the codec's scalar rows written
directly, ahead of the buffer probe, the frame-type checks, and the
codec's container-capable walk. bytes/numpy keep the raw tiers (the
wire keeps RAWVEC), a str past the STR1 budget still spills through
the codec, an int past int64 falls through to pickle, and foreign
scalars still route to the 'I' writer (the gate sits below the
foreign branch). The wire is unchanged — byte-identical to
stage_codec's rows.

Results (focused big-ring channel A/B/A on this host, 7 reps,
medians; the send-side rows isolate the staging share the
synchronous rt hides):

| row | before | after |
|----|----|----|
| sequential rt, channel | 0.371 us/rt | 0.371 / 0.386 us/rt |
| pipelined, channel | 4.26M rt/s | 4.70M / 4.62M rt/s |
| send-only (probe row) | ~10.0M msg/s | ~10.6-10.8M msg/s |

+8-10% pipelined, ~6-8% on the isolated send loop, nothing on the
synchronous rt (staging is a small share of the crossing). The full
suite's before/after runs swung the same rows inside suite noise
(pipelined 4.62M -> 4.46M on the pair, batch 9.36M -> 10.15M,
streaming 21.2M -> 21.4M); no best-known swap.

Status: full suite 403 pass, 5 skip; ruff + pyrefly clean.

## 2026-10-06 — streaming Pool.map (stream=True) [(0c0c29d)]

Validation rows for the streaming map (mirrors the R entry): the
footprint model confirms at W/C — descriptor pickle + W slice buffers
= 6.3% of x's 15.3 MB raw section (model 6.2%) — and the window keeps
workers fed: big-x and trivial-f rows run at parity or ahead of the
region path on this host (the region path pays one giant descriptor
pickle plus a per-worker unpickle for a list x; streaming amortizes
both). No loop pathology surfaced (the pymizu splice is a C-level
list slice assignment).

Results (big-x = 2e6 float64, 16 MB; trivial f = 1e6, 4 workers):
big-x region 0.949s | stream 0.633s (0.67x); SHM 15.3 MB region |
0.95 MB stream (6.3%, model 6.2%). trivial f region 0.066s | stream
0.036s (0.55x); trivial f template region 0.038s | stream 0.041s
(1.09x). Suite rows in band (map trivial f 0.4 us/elt, template
copy/view 724k/726k, skew 7.3 ms); pipelined pool 2.0-2.1M vs the
2.48M best is host drift (same-build re-runs swing ±6%).

Status: full suite 448 pass, 5 skip (pandas-absence); ruff + pyrefly
clean.

## 2026-10-07 — InterpreterPoolExecutor leg in mizu-stdlib-bench.py

The stdlib comparison gains a third executor flavour on Python 3.14+:
InterpreterPoolExecutor (per-interpreter GILs). Its payloads must be
shareable — builtin scalars, plain containers of them, or
buffer-protocol objects — so every numpy-shaped row reports
"unsupported" rather than measuring: the gate raises NotShareableError
on numpy arrays, numpy scalars, and tuples containing them (probed on
3.14.8). Only the const-task rows produce numbers; the shareability
gate, not speed, is this executor's defining constraint.

Results (first run of the leg, Apple M4 Pro, Python 3.14.8):

| row | pymizu pool | cf process | cf thread | cf interpreter |
|----|----|----|----|----|
| sequential rt | 0.7 us/task | 93.9 us/task | 9.7 us/task | 21.3 us/task |
| pipelined | 2.12M tasks/s | 19.0k/s | 385k/s | 59.6k/s |
| payload 8 KB / 800 KB / 8 MB | 2.6 / 61.4 / 107.5 us | 111 / 396 / 4,091 us | 9.9 / 9.3 / 9.5 us | unsupported |
| fan-out | 287.5k/s | 16.3k/s | 38.8k/s | unsupported |
| map trivial f | 0.4 us/elt | 71.6 us/elt | 3.1 us/elt | unsupported |
| map (winsum x 2000) | 454.7k/s | 15.0k/s | 38.7k/s | unsupported |
| map skewed f | 7.0 ms wall | 267.3 ms | 49.5 ms | unsupported |

Status: ruff clean (benchmarks); full suite not run (benchmark-only
change).

## 2026-10-12 — the frame writer's promotion to the core (arrow.c)

A/B of the MIZL frame staging path before/after the writer moved to
libmizu (same machine, back-to-back builds, the `frame 10col relay,
Python (flat)` probe: a heap-backed 10-col 1e6-row float64 Frame
same-language). Expect neutral: the foreign front-end already wrote the
region straight off the Arrow batches (no intermediate Frame to
remove), and the code moved rather than being rewritten.

Results (best of 3, us/send): before 4,513.9 / 4,510.8; after 4,541.3 /
4,571.5 — within ~1%; neutral.

Status: full suite 520 pass, 6 skip (pandas/duckdb-absence); ruff +
ruff format + pyrefly clean.

## 2026-10-12 — the frame read/export savings (Phase 2 follow-up)

A/B of the remote-leaf frame read before/after the deduped holds, the
recorded null counts and the shared NUL name store (same machine,
back-to-back builds). Read probe: a 16-col 20k-row f64 frame whose 15
unmodified columns cross as remote leaves over ONE region (the
provenance re-send shape), received in-process — 15 opens + loans
become 1. Export probe: repeat `pa.table()` on a received 16-col
null-heavy frame — the borrowed bitmaps' re-popcounts and the per-export
name-store copy collapse to serves.

Results (min of 300 / 100 reps, two runs, us): remote read before
44.6 / 47.0, after 4.8 / 4.8 (~9x); repeat export before 46.0 / 45.9,
after 11.9 / 12.1 (~4x). One-off probe, log only — no table row.

Status: full suite 524 pass, 6 skip (pandas/duckdb-absence); crosslang
legs 97 pass; ruff + ruff format + pyrefly clean.

## 2026-10-13 — layout-writer rows for the cross-language channel bench

First records for the reverse layout writers' rows in
benchmarks/crosslang-channel-bench.py (new: a C-order dim payload in
the round-trip section, a 100k datetime64[s] typed row, and the
send-only rows: the C-order gather, an MIZL tree of buffers + strings
off one region write, and the remote-leaf pair — a tree carrying a
received 1.6 MB view stages the ~25-byte identifier where the by-value
tree memcpys the leaf). Log-only rows. Two full runs back-to-back —
the spread is host drift (the bands, not a regression read).

Results (median, run 1 / run 2): C-order 600x800 f64 echo 117.0 /
105.4 us/rt (copy echo 248.4 / 268.4); 100k datetime64[s] echo 163.6 /
223.8; C-order sink 95.7 / 314.6 us/send (page-fault-sensitive — the
gather touches freshly mapped pages per send); MIZL tree 1.2 MB sink
42.1 / 133.6; tree 2x 1.6 MB copy sink 85.2 / 110.0 vs tree copy +
remote 73.1 / 105.2 — the remote leaf's send-side saving (the skipped
1.6 MB memcpy) is real but inside the run-to-run band at this size;
the structural win is the receive side (R wraps, never copies) and
residency, not the send number.

Status: full suite 562 pass, 6 skip (pandas/duckdb-absence); ruff +
ruff format + pyrefly clean.

## 2026-10-14 — the frame-tier span gate + the per-column writer dedup

The Arrow review's pymizu items: the frame tier now gates on the core's
memoized frame span (the 'I' sizing's per-element string walk runs only
on the by-value branches — it used to size and discard whenever the
region won), the inline column emitters delegate to the core's new
per-column writers (mizu_arrow_col_body / _col_blob; pymizu's copies
deleted, the dictionary levels no longer copied out at open — the core
reads batch 0's borrowed dictionary), and the memoized size re-runs
after an override (libmizu 0f172a8, re-vendored). The wire does not
change: byte identity holds across the cross-language suite.

Results (A/B probe, worktree at a7e7d68 vs this build, an in-process
foreign pair, a 2e5-row frame — double, int32, string, timestamp[us],
string dictionary — best of 5 x 30 sends): 5610 us/send -> 5481 us/send
(~130 us = the discarded sizing walk over one 2e5-row string column;
proportionally larger on string-heavier frames).

Status: full suite 590 pass, 6 skip (pandas/duckdb-absence).

## 2026-10-14 — the inline frame emitters promoted into the core

The small-table thorough form, pymizu half: the by-value 'I' frame and
single-column forms now write through libmizu's mizu_arrow_write_ix /
_write_ix_col (0c6f5c9, re-vendored) — ixs_emit_frame and the
ixs_emit_*_col family deleted (~200 lines), one C write straight off
the Arrow buffers both sides. The wire does not change: byte identity
holds across the unmodified cross-language suite. pymizu never had the
convert-back residual (its emitter always wrote 'I' directly), so the
expected delta is parity.

Results (A/B probe, this build vs 836b213, an in-process same-language
pair, a 50-row pyarrow table — float64, int32, string — send + recv
round trips drained per iteration, medians of 7 x 2000 over 3 rounds):
40.8 us/rt -> 40.5-42.0 us/rt (parity, inside the run-to-run band).

Status: full suite 590 pass, 6 skip (pandas/duckdb-absence).

## 2026-10-15 — the Arrow producer probes interned and skipped for builtins

The foreign send path's producer probes no longer build a str per
lookup or pay HasAttrString's exception machinery: the two dunder
names intern at module init and look up exception-free
(PyObject_GetOptionalAttr on 3.13+, _PyObject_LookupAttr below), the
probe-then-get pair collapsing to one lookup, and exact builtins
(list, tuple, dict, str, int, float, bool, bytes, complex) skip the
probes outright — no instance dict and an immutable type, so neither
dunder can exist. ixs_frame_cols_build takes the same lookup form.
The wire does not move.

Results (A/B probe, this build vs da5de74, an in-process foreign pair,
send + recv round trips, best of 5 x 20000): int 0.405 -> 0.278 us/rt,
{a: 1, b: 2.5} 0.532 -> 0.384, [1, 2, 3] 0.454 -> 0.327, a 40-byte str
0.146 -> 0.146 (the control — STR1 stages ahead of the foreign block).
~0.13-0.15 us off every probed non-buffer send.

Status: full suite 593 pass, 6 skip (pandas/duckdb-absence).

## 2026-10-20 — PoolExecutor drain: persistent snapshot, bounded bursts

The executor adapter's drain thread resolved each task against a fresh
copy of the outstanding set, so a burst of n completions cost O(n^2)
Python list churn — per-task cost grew 26 -> 75 us from 500 to 4,000
outstanding. The drain's snapshot is now persistent (submissions merge
in bulk from a pending list, resolves swap-remove in O(1)) and
completions drain in bursts of up to 256 per verbs-lock hold. Not the
canonical suite: an A/B probe through the asyncio harness of
examples/shiny/bench.py — trivial ~5 us tasks (mc_pi(100)) fanned out n
at a time, futures awaited via asyncio.wrap_future + gather, 4 workers.

Results (this build vs pre-change, tasks/s by outstanding count):

| outstanding | before | after |
|----|----|----|
| 500 | 38.6k | 53.1k |
| 1,000 | 31.7k | 57.0k |
| 2,000 | 24.8k | 46.2k |
| 4,000 | 13.3k | 37.2k |

The shiny bench's granularity row (4,000 x ~5 us tasks, against
ProcessPoolExecutor's 0.31 s) went 0.254 -> 0.100 s and its 39 ms
event-loop stall is gone. The residual per-task growth is the core's
O(n) collect_any scan per resolution — a batch-collect verb is the only
further headroom.

Status: full suite 890 pass, 7 skip (pandas/duckdb-absence, one
platform skip); ruff + pyrefly clean.

## 2026-10-27 — asyncio rows land in the canonical suite

Section 9 of mizu-bench.py benchmarks the event-driven surface ahead of
the async audit's efficiency work: the armed-channel row (one recv
future pending the whole run), the await-per-message and await-per-task
rows, one-future collections, 64 concurrent waiters (the per-wake scan
regime on the one coalescing submitter word), and — as section 9b — the
PoolExecutor outstanding-count guard, the 2026-10-20 A/B harness (until
now examples/shiny/bench.py, not in the repo) moved into the suite.
First recordings on a non-quiet machine: the rows exist for A/B, so the
deltas from here matter more than the absolutes.

Results: channel armed 2.62M rt/s; channel recv_async 403k rt/s; pool
collect_async 268k tasks/s; pool collect_all_async 1.38M tasks/s; pool
64 waiters 1.80M tasks/s; executor 60.7k / 59.3k / 61.9k / 59.9k
tasks/s at 500 / 1,000 / 2,000 / 4,000 outstanding (flat — no
per-submit rescan growth at these counts).
Status: ruff check + format clean; suite not run (bench-only change).

## 2026-10-27 — async efficiency: burst drain, waiter futures, per-submit re-check [(with libmizu notify efficiency)]

The async audit's binding-side findings land: the channel drain moves to
one GIL-held `_recv_burst` verb per pass (a per-message recv(0) paid two
GIL transitions and a kwargs parse); waiter futures are a Future
subclass whose cancel() routes to the detach (the add_done_callback
form scheduled a no-op loop callback per resolution); the pool pump's
terminal scan is the new `_tasks_terminal_set` subset form (no bool
list + dict of the whole enrolled set per pass); the pool arm guards on
already-armed (each collect_async enrollment paid an endpoint drain);
the executor's submit re-check scans just the new task (a full-drain
per submit was O(outstanding) per submission) and batches its
shutdown-waiter wake per pass; the loop thread's call_soon_threadsafe
is cached on the notify record and vectorcalled; the drain quantum's
clock reads stride 64. Core-side (vendored): the pool publish's
coalescing-word RMW is load-gated, re-arms of a continuously armed word
skip the endpoint drain, and the tx opens before the edge is claimed
(a lost-wake hole on open failure).

Results (A/B, this build vs the morning's first recordings; same-day
sync anchors pipelined channel 4.02M rt/s, pool 1.82M tasks/s — the
host reads slow): channel recv_async 403k -> 607k rt/s; pool
collect_async 268k -> 489k tasks/s; pool collect_all_async 1.38M ->
1.41M tasks/s; pool 64 waiters 1.80M -> 1.99M tasks/s; executor
60.7/59.3/61.9/59.9k -> 64.1/65.0/61.9/61.9k. Channel armed 2.62M ->
2.08-2.22M across three runs = 0.52-0.55x the day's pipelined anchor,
inside the before run's ratio band (the publish path is
instruction-identical for the case; drift, not a regression).
Status: full suite 1019 pass, 7 skip (pandas/duckdb-absence); ruff +
pyrefly clean.

## 2026-10-27 — asyncio enrollment fusion (_tasks_enroll / _tasks_release)

The pool's asyncio enrollment moves behind two fused C verbs, the pymizu
counterpart of mizu's same-day waiter-registry work:
`_tasks_enroll(tasks)` flags every task's async-consumer marker and
announces NOTIFY in one call, all-or-nothing (a conflicting task raises
with nothing flagged or announced); `_tasks_release(tasks)` clears the
flags unconditionally and withdraws the announces best-effort (a
destroyed pool has nothing to withdraw from). `_aio.py` loses its
per-task `_async_set` loops at enroll, settle, detach, and fail-all —
2T Python->C crossings per collection down to 2 calls. The fusion also
fixes a rollback bug: a collection enrollment hitting an
already-enrolled task raised mid-loop and the rollback cleared the flag
on EVERY task, including the conflicting task's legitimate consumer —
silently dropping its one-consumer protection.

A/B (standalone harness replicating the section-9 shapes, best-of-3,
back-to-back against the pre-change build): pool collect_async 510k ->
507-522k tasks/s (parity — two calls saved of the whole enroll/settle
path); pool collect_all_async 1.78M -> 1.86-1.88M (+5%); pool 64 waiters
1.95M -> 2.06-2.14M (+6-10%). The executor is untouched (it announces
without flags by design).

Canonical suite after the change: recv_async 622k, collect_async 520k,
collect_all_async 1.49M, 64 waiters 2.11M — over the morning's bests,
with the sync anchors reading ~10% faster this run (pipelined pool
2.03M vs 1.82M), so the A/B ratios above are the change's effect and
the absolutes ride the host. Channel armed 1.67M, its documented drift
band (the change touches no channel path).

Status: full suite 1021 pass, 7 skip (pandas/duckdb-absence, one
platform skip); ruff + pyrefly clean.

## 2026-10-27 — submit_batch frames its callables in C

`Pool.submit_batch`'s payload loop moves from the facade (one
`_task_frame` call per item, in Python) into the C verb: items that are
not ready _TaskFrame payloads must be zero-arg callables, framed against
one shared empty args/kwargs pair per call — the whole batch still
validates before any submission, and the accepted-attribute and
ring-full contracts are unchanged. The facade body is one line.

A/B (standalone harness replicating the canonical shapes, best-of-3,
back-to-back against the pre-change build): pipelined pool batch 1.52M
-> 1.78-1.81M tasks/s (+17-19%); pool collect_all_async 1.85M ->
1.96-2.04M (+6-10%); pool 64 waiters 2.05M -> 2.12-2.28M (+3-11%); the
collect_async control (no submit_batch in its path) flat at 505-511k.

Canonical suite after the change: pool batch 1.82M (table swap from
1.25M), collect_all_async 1.72M and 64 waiters 2.28M (swaps over the
morning's 1.49M / 2.11M); collect_async 518k, recv_async 609k, channel
armed 2.46M (no swaps).

Status: full suite 1022 pass, 7 skip (pandas/duckdb-absence, one
platform skip); ruff + pyrefly clean.

## 2026-10-28 — threading-contract guards (ConcurrentUseError)

Non-blocking atomic role guards on the channel send/recv verbs and the
pool submit family (the single-producer / one-thread-per-side contract):
two atomics per guarded verb (micro: guard pair 1.2 ns vs uncontended
pthread mutex pair 3.8 ns). No table swap — an A/B isolation run, logged
per convention.

A/B (canonical suite, back-to-back against the pre-guard build on the
same host): every guarded hot path flat — sequential round-trip channel
1.000 / pool 1.000, pipelined 0.995-1.024 across the four rows, payload
0.993-1.000, streaming 1.028, stage rows 0.969-1.042, event-driven
0.980-1.016, map rows 0.984-1.059. Rows past +/-5% are all same-build
jitter, proven by two baseline runs swinging as much: frame 10col relay
R (one computed col) 171 vs 217 pre-vs-pre (27%), executor outstanding
1000 67.3k vs 62.9k pre-vs-pre, the skew wall-time row. Facade submit
A/B on the wave harness: 20.0 -> 19.7 us (within noise).

Status: full suite 1026 pass, 7 skip (pandas/duckdb-absence, one
platform skip); ruff + pyrefly clean.

## 2026-10-28 — waiter-side + ready_wait guards, armed-consumer check

Extends the threading-contract guards to the roles 64cb933 left
documented-only: the pool's waiting half (Task.collect / collect_any /
collect_all / ready_wait / stop — parking waits only; zero-timeout polls
never announce or park, so the executor drain's collect(0) settles
alongside an outside blocking collect stay unguarded), the channel
ready_wait, and the blocking-vs-async receive exclusion, thread-scoped:
a blocking receive from off the armed consumer's loop thread raises at
the call site, while on the loop thread it can never race the drain
(the "channel armed" row pins that pattern). No table swap — an A/B
isolation run, logged per convention.

A/B (canonical suite, back-to-back against the 64cb933 build on the
same host): swing band is host noise, proven by the R-side control
rows the change cannot touch — R pool plan rows 1.010-1.046, echo rows
1.058-1.065, frame 10col relay R 1.064-1.158. pymizu rows inside the
same band: sequential rt 1.000 / 1.143 (0.7 -> 0.8 us absolute),
pipelined 0.946-1.150 both directions, payload 0.992-1.160, streaming
0.972, event-driven 0.925-1.037, executor 0.997-1.032.

Status: full suite 1031 pass, 7 skip; ruff + pyrefly clean.

## 2026-11-04 — frame REF rows restored by mizu's zc boundary fix

The regression in `frame 10col relay, R (unmodified REF)` (6.2 -> ~190
us/rt) and `frame strcol relay` (2.4 -> 10.2 ms) was R-side receive
cost: mizu 53382e7's recv-verb gc boundary re-charged the 80 MB frame
region per view wrap, firing a full minor collection per receive. mizu
now charges each region once per pending lifetime (its bench notes carry
the bisect evidence and the resolve-only probe). Re-read on the fixed
build: unmodified REF 5.9 us/rt (table swap from 6.2), one computed col
246.8 us/rt (band, no swap), strcol 2,442.6 us/rt (the 2,410.8 band, no
swap).

Status: bench-only measurement against the fixed mizu install; no
pymizu change.

## 2026-11-06 — Phase 1 C dedup A/B (refactor/phase1-c-dedup, 4eea29e + 2b7f688)

A/B (canonical suite, back-to-back against the da561fc build on the
same host) for the mechanical consolidation: no wire, API, or staging
change, so parity is the expectation. Latency rows geomean 0.975,
throughput rows 1.006; every row inside 0.83-1.08 with the in-process
control rows swinging 1.009-1.052 (host noise band). Rows the change
touches most (sequential rt 0.889-1.000, pipelined 0.984-1.048,
executor outstanding 0.953-0.988, event-driven 0.980-1.034) sit inside
that band. No table swap — an A/B isolation run, logged per convention.

Status: full suite 1044 pass, 1 skip.

## 2026-11-06 — Phase 2 hot-path efficiency (59d947d, 669720d, a70e42d, 9176a33)

The backlog's phase 2 (branch refactor/hot-path-efficiency): the 'I'
walker records one (index | borrowed key) path segment per descent and
renders only at decline time; ixe_fcol's masked arms gate on the
recorded vnulls; layout.c's MIZL write pass records the remote-leaf
validity claim as it goes (the third element walk is gone); and the
facades cache hot bound C verbs plus lazy _aio/_map holders. Four A/B
runs against 53cd2ee on the same host, which was jittery (in-process
controls swung 0.945-1.097; the widen and R-echo rows moved +/-20% on
untouched code). Item 1's target rows dominate: plan k = 0/1/4/16 went
263.6 -> 87.1, 290.6 -> 100.7, 292.0 -> 97.6, 349.8 -> 159.2 us/task
(-55% to -67%) — a structural win, so the table's four plan rows swap
to the day's per-row bests. All-rows geomeans vs the prior build:
1.070 (item 1), 0.996 (item 2), 0.985 (item 3) — items 2-3 parity
inside the noise band (their exact paths have no bench row; the
masked-Arrow and MIZH/MIZS rows stand as adjacent signal). One
pre-existing anomaly, visible on the baseline build too: stage masked
int64 ran 202-210 us/send against the table's 97.6 — a definition or
host drift to re-check on a quiet host, not phase 2's doing.

Status: full suite 1044 pass, 1 skip; ruff + pyrefly clean.

## 2026-11-06 — masked int64 conversion fuse (libmizu 6dfe375)

The masked int64 anomaly was the conversion kernel's genuine-sentinel
count: a second pass over dst that only the int32/int64 rows pay.
libmizu 6dfe375 fuses it into the copy; re-vendored here. Same-host
A/B: masked int64 185.6 -> ~140 us/send, the int64:int32 gap ~1.5x ->
~1.1x. No best-known swap — 140 does not beat the 97.6 quiet-machine
record.

## 2026-11-06 — typed map results: the runner-result codec (perf/map-result-codec)

Pool.map() results no longer pass through pickle: the runner and chunk
publishes ride a new private 'P' tag ('m') — each batch one typed leaf
(the wire type's raw bytes, exact Python or numpy scalar classes on
read) or, on any classification decline (a mid-batch type change, an
int past int64, bool/str/None, an unmappable numpy dtype), a per-batch
protocol-4 pickle. A foreign submitter (an R host driving Python
workers through a spec map) gets today's plain (hist, vals) through the
'I' writer — the stage hook unwraps the marker ahead of the foreign
branch. A/B (same host, back to back, the section-6 rows of
mizu-bench.py) against the pre-change build:

| row | before | after |
|---|---|---|
| map trivial f, numpy x | 0.375 us/elt | 0.069 us/elt |
| map trivial f, list-x (Python floats) | 0.073 us/elt | 0.070 us/elt |
| map trivial f, template | 0.049 us/elt | 0.049 us/elt |
| map trivial f, seed | 1.93 us/elt | 1.66 us/elt |
| map trivial f, prepared | 0.375 us/elt | 0.065 us/elt |
| map winsum x 2000 | 497.3k tasks/s | 689.3k tasks/s |
| map winsum x 20000 | 497.7k tasks/s | 738.4k tasks/s |
| map template 20k, copy / view | 741.7k / 740.3k tasks/s | 738.4k / 739.6k tasks/s |
| map skewed f x 4000 | 6.8 ms wall | 5.6 ms wall |

The numpy-scalar rows land at the template regime — the result encoding
is no longer the cost, matching the plan's targets (trivial f at or
under 0.1 us/elt, winsum at 700-770k tasks/s). The list-x Python-float
row is flat: the classifier costs less than pickle's ~7 ns/elt.
Template and skew rows hold (the skew gain is its np.float64 results
leaving pickle). Best-known swaps: map trivial f, its seed and prepared
rows, winsum x 2000 and x 20000, skewed.

Status: full suite 1061 pass, 1 skip (the known platform skip); ruff +
pyrefly clean.

## 2026-11-06 — C batch loop re-evaluation; _run_batch fast path instead (perf/run-batch-fastpath)

Post-codec profile of the trivial-f row (0.069 us/elt): the worker-side
element loop is the residual (~31 of 69 ns/elt wall at 4 workers, 122.5
ns/elt per worker). Its decomposition: ndarray.__getitem__ ~39 ns and
fn call + np arithmetic ~34 ns — both irreducible without numpy's C
headers (an np.float64 from C costs 43 ns via the class constructor) —
plus ~53 ns of interpreter overhead: the call_one closure hop and the
per-element *args/**kwargs unpack. A C batch loop could remove only the
last slice, and _run_batch can lose it in Python instead: one try
around the loop (the index annotation keeps call_one's semantics) and a
direct fn(get(i)) call on the common no-args path. In-process: 122.5 ->
71.8 ns/elt (empty args), 152.4 -> 134.0 (with args). Post-restructure
the C loop's ceiling is ~10 ns/elt per worker (~3 ns wall) — the
2026-08-24 revert decision stands, now for the overhead regime too:
the residual loop is numpy scalar creation plus the user's fn call.

A/B (same host, back to back, section-6 rows) against the codec build
d3d370a:

| row | before | after |
|---|---|---|
| map trivial f, numpy x | 0.070 us/elt | 0.056 us/elt |
| map trivial f, list-x (Python floats) | 0.070-0.184 us/elt | 0.056 us/elt |
| map trivial f, template | 0.054 us/elt | 0.034 us/elt |
| map trivial f, prepared | 0.064 us/elt | 0.049 us/elt |
| map winsum x 2000 | 593.6k tasks/s | 706.7k tasks/s |
| map winsum x 20000 | 734.2k tasks/s | 759.3k tasks/s |
| map template 20k, copy / view | 735.8k / 734.1k tasks/s | 738.8k / 739.2k tasks/s |
| map trivial f, seed | 1.68 us/elt | 1.65 us/elt |
| map skewed f x 4000 | 5.4 ms wall | 5.3 ms wall |

(The before run drifted low mid-session — its list-x and winsum-2k
rows repeat below the day's band; the after rows still clear the band
tops on every row the change touches. Seed and skew are flat, as
designed: the seeded path keeps call_one and skew returns np scalars.)
Best-known swaps: map trivial f (0.06) and its prepared row (0.05),
winsum x 2000 (706.7k) and x 20000 (759.3k).

Status: map + stream suites 175 pass, 1 skip; ruff + pyrefly clean.

## 2026-11-06 — asyncio pump and PoolExecutor C fast paths [(a5ac913, 82b84c6, 7b6b50b, 3a65881, 21c32dd)]

`Task.collect_async()` runs end to end in C when the result is already
published (no arm cycle, no Python frame), the pool pump's arm is sticky
(one arm+disarm pair per idle period, not per await), enrollment probes
terminal before arming and re-checks the new entry only (never the
enrolled set), and the singles waiters live in a C word registry scanned
per wake. The executor's submit fuses submit + announce + terminal
re-check (`_exec_submit`), resolving a terminal-at-submit task inline
with no drain pass, and the drain's scan is one `_tasks_collect_terminal`
call. A/B below is aa1dc4c -> tip, same host back to back; the "executor
sync" row is new (submit 1,000, then result() 1,000, 4 workers, const) —
its before value is the same shape run against aa1dc4c by probe.

| row | before | after |
|---|---|---|
| pool collect_async | 522.6k tasks/s | 1,592.0k tasks/s |
| pool collect_all_async | 1.69M tasks/s | 1.70M tasks/s |
| pool 64 waiters | 2.07M tasks/s | 1.93M tasks/s |
| channel recv_async | 680k rt/s | 648k rt/s |
| channel armed | 2.56M rt/s | 2.44M rt/s |
| executor outstanding 500 / 1,000 / 2,000 / 4,000 | 59.3k / 60.3k / 59.0k / 59.0k tasks/s | 77.4k / 80.8k / 80.8k / 80.4k tasks/s |
| executor sync | 5.38 us/task | 4.8 us/task |

The 64-waiters row pays the trade: the old per-enrollment full-set pump
(122 passes per rep) settled collections inside the enrollment burst;
the new O(1) enrollments defer every settle to 2-3 wake passes after it
(~1 ms of loop round-trips per rep). Enrollment itself is far cheaper
and per-wake work unchanged — the dip is latency placement, not
throughput. Sections 1-6 flat (8M payload's before run was a high
outlier; the after value matches the best-known). Best-known swaps:
pool collect_async (1.59M), channel recv_async (648k), the executor
outstanding row, and the new executor sync row (4.8 us/task).

Status: full suite 1071 pass, 1 skip; ruff + pyrefly clean.

## 2026-11-07 — stdlib comparison re-read; README/docs tables refreshed

Full mizu-stdlib-bench.py on main (dd96753) after the map work. The
README / docs tables moved to mizu's row shape — round trip, pipelined,
8 KB vector round trip (new: 2.4 us vs 100.7 process / 9.3 thread — the
one payload row that also beats the thread pool), winsum x 2000 (4.3 ->
2.9 ms) — with this run's numbers on the changed rows; round-trip and
pipelined measured inside their bands and keep their values.

| row | pymizu | cf process | cf thread |
|---|---|---|---|
| sequential rt | 0.8 us/task | 87.6 us/task | 9.3 us/task |
| pipelined | 2,037,714 tasks/s | 19,896 tasks/s | 410,930 tasks/s |
| payload 8,000 B | 2.4 us/task | 100.7 us/task | 9.3 us/task |
| payload 800,000 B | 57.8 us/task | 374.1 us/task | 9.3 us/task |
| payload 8,000,000 B | 101.7 us/task | 3,589.9 us/task | 9.2 us/task |
| fan-out x 2000, 4 workers | 275.7k tasks/s | 17.1k tasks/s | 43.3k tasks/s |
| map trivial f, 4 workers | 0.1 us/elt | 68.3 us/elt | 2.8 us/elt |
| map winsum x 2000, 4 workers | 691.2k tasks/s | 16.2k tasks/s | 42.0k tasks/s |
| map skewed f x 4000, 4 workers | 5.3 ms wall | 256.4 ms wall | 46.9 ms wall |

(cf interpreter pool: sequential 19.6 us, pipelined 65.1k; payload and
map rows unsupported.)

Status: report-only run plus the table refresh; no code change.

## 2026-11-08 — Linux baseline and the linux-perf pass [(pymizu 3f1619a; libmizu 48fe302)]

First full-suite read on a Linux host (Linux 7.0 x86_64, i7-1165G7
4C/8T, 15 GB, THP=madvise, intel_pstate powersave / EPP
balance_performance) against the M4 Pro best-known table: nearly every
row lands in a 2.4-3.3x hardware band. The outliers got a tuning pass
on the linux-perf branches of libmizu and pymizu (vendored here at
433169c; the plan and per-item detail live in
.posit/assistant/plans/linux-perf.md). The suite now defaults
OPENBLAS_NUM_THREADS=1 — OpenBLAS's spinning thread pool contended
with the measured processes on this 4C/8T host (the pipelined pool row
swung 31% run-to-run; pinned it rides +/-3.5%).

A/B on the divergent rows (before = this morning's main on this host;
after = the branch, same host):

| row | before | after |
|----|----|----|
| stage masked (f64, all-valid bitmap) | 704.1 us | 344.0 us |
| stage masked int64 | 816.6 us | 635.5 us |
| stage masked+scan (int32) | ~285-306 us | 188.0 us |
| frame 10col relay, Python (flat) | 25,359 us | 14,759 us |
| arg 8 MB, Python pool (flat) | 999.4 us | ~0.9-1.3 ms (noise band) |
| plan k = 16 (per-candidate slope) | 601.9 us (+26.5/cand) | 534.3 us (+21.8/cand) |
| pipelined, pool (1 worker) | 0.68-0.90M rt/s unstable | 982,564 tasks/s |

The masked rows' fix is upstream (run-coalesced masked conversion plus
a fused sentinel count for int32 and for past-LLC int64); masked f64
now rides at 1.2x the identity row. The frame relay's 25 ms was
stage-side: region-create churn 9.5 ms (the 80 MB region exceeds the
64 MB spill free-list cap), the MIZL write 4.3 ms, and the Frame's
Arrow export re-scanning every column for nulls per send 11 ms — the
export's clean verdict now memoizes, which is the row's 1.7x. The
remaining frame lever is the free-list byte cap (retention policy, not
touched). arg-8 MB flat is bandwidth-bound in steady state (the
worker's view cache recycles the mapping), and the plan-k slope lives
in the R workers' own vendored core — the view-open prefault
(MADV_POPULATE_READ past 1 MiB) lands its win on fresh mappings
instead (frame regions, first touches). The pipelined pool row is
submit-side stage-bound (~0.85 us/submit, no hotspot on callgrind);
its instability was the OpenBLAS contention above, not wake latency
(worker parks 0.18% of tasks).

Full suite on the branch: 1092 passed, 12 skipped. libmizu unit, ext
and integration tiers pass; export surface unchanged.

Status: report-only run plus the code changes above; the best-known
table is untouched (M4 Pro rows — the masked/frame A/B on the M4 is
still owed).

## 2026-11-08 — M4 Pro re-baseline; owed masked/frame re-check [(1872b95)]

Phase 0 of the performance plan
(.posit/assistant/plans/2026-11-08-performance-improvements.md): the
current source rebuilt (`pip install . --no-cache-dir`), the full
suite, then two back-to-back mizu-bench.py runs on this host — the
baseline the plan's later phases A/B against. This also lands the owed
M4 Pro re-check of yesterday's Linux masked-conversion and frame-relay
work. The frame win transfers: the 10col one-computed-col relay reads
201.0 / 217.1 us against the 422.3 us record (the Arrow export's
memoized null verdict) and the unmodified REF row 5.3 / 5.5. The
masked int64 win does not: 183.3 / 184.2 us/send, back at this host's
pre-fuse level (the 2026-11-06 post-fuse read here was ~140) — the
Linux kernels' run-coalescing and fused sentinel count are x86
cache-regime wins; whether they cost the M4 its 140 is an A/B against
the pre-vendoring core, not pursued today. masked f64 (113.6 / 122.0)
and masked+scan (151.3 / 124.2) sit in their bands.

Results (run 1 / run 2, the plan's watch rows plus the re-check rows):

| row | run 1 | run 2 |
|---|---|---|
| sequential rt, channel / pool | 0.4 / 0.8 us | 0.4 / 0.8 us |
| pipelined, channel / channel batch | 4.20M / 10.00M rt/s | 4.11M / 9.77M rt/s |
| pipelined, pool / pool batch | 2.09M / 1.80M tasks/s | 1.93M / 1.80M tasks/s |
| payload 8,000 B | 2.3 us | 2.6 us |
| map trivial f / template / seed / prepared | 0.1 / 0.0 / 1.7 / 0.0 us/elt | 0.1 / 0.0 / 1.6 / 0.0 us/elt |
| map winsum x 2000 / x 20000 | 715.0k / 746.1k tasks/s | 623.8k / 739.8k tasks/s |
| map template 20k, copy / view | 744.5k / 754.2k tasks/s | 748.6k / 756.9k tasks/s |
| map skewed f | 5.3 ms | 5.1 ms |
| stage masked / masked int64 / masked+scan | 113.6 / 184.2 / 151.3 us | 122.0 / 183.3 / 124.2 us |
| frame 10col relay: REF / computed col / Python flat | 5.5 / 201.0 us / 4,274.9 us | 5.3 / 217.1 us / 4,326.4 us |
| channel armed / recv_async | 2.58M / 654k rt/s | 2.67M / 662k rt/s |
| pool collect_async / collect_all_async / 64 waiters | 1.61M / 1.73M / 1.99M tasks/s | 1.65M / 1.73M / 1.99M tasks/s |
| executor outstanding 500 / 1k / 2k / 4k | 80.6k / 79.7k / 82.1k / 83.2k tasks/s | 77.6k / 78.5k / 80.9k / 80.6k tasks/s |
| executor sync | 4.9 us | 5.4 us |

Best-known swaps (the table's dates carry them): payload 8 KB 2.3,
800 KB 55.4, 8 MB 101.7; map skewed 5.1 ms and winsum x 2000 715.0k;
frame 10col REF 5.3 and one computed col 201.0; channel armed 2.67M,
recv_async 662k, collect_async 1.65M, collect_all_async 1.73M; the
executor outstanding tuple; plan k = 0 82.7 and k = 1 99.1.

Status: full suite 1103 pass, 1 skip; no code change.

## 2026-11-08 — Phases 1-2 of the performance plan [(73d4e06, 2a62d2a, 7b5a1f0, 5f7e4ae, a772305, 3b7ee26)]

Phases 1-2 of .posit/assistant/plans/2026-11-08-performance-improvements.md:
non-raising interned-key module probes, exact-`bytes` staging straight
into the raw tier, scalar-first template element writes, local-index
streaming chunks, one seed hash per batch (byte-identical draws, pinned
by the new golden tests), and O(1) channel-waiter detach with
module-level detach functions. A/B against the phase-0 baseline (suite
rows; finer values from a best-of-5 probe):

| row | before | after |
|---|---|---|
| channel recv_async cancel | 227.8k cancel/s | 879.0k cancel/s (~4x) |
| channel recv_async | 655.8k rt/s | 834.2k rt/s (+27%) |
| map trivial f, stream | 0.1801 us/elt | 0.1685 us/elt |
| map trivial f, seed | 1.6639 us/elt | 1.6546 us/elt |

The cancel row's 4x is the detach itself; recv_async's +27% is the
enrollment lambda removal. The seed hash caching nets under 1% —
`random.seed()`'s per-element SHA-512 dominates that row, not the
hashing half. Phase 1's watch rows (pipelined channel/pool, 8 KB
payload, map template) are flat at suite resolution, as expected for
per-op nanosecond wins. Best-known swaps: channel recv_async (834k);
the two new rows enter the table (map stream 0.17, recv_async cancel
1.00M).

Status: full suite 1105 pass, 1 skip; ruff + pyrefly clean.

## 2026-11-08 — Phase 3 of the performance plan [(e2468f0, a8e2ae6, ccdc91e, e421536; follow-ups 7bd317a, 88386df)]

Phase 3 of .posit/assistant/plans/2026-11-08-performance-improvements.md:
per-worker task-fn resolution caches (the by-ref decode and the interop
name decode), the interrupt-check hook throttled to 1 ms per thread,
stack-resident map-result leaf plans, and the task frame's buffer views
carried open from the size pass into the write pass (one GetBuffer per
leaf; the write-pass re-export decline is gone by construction — P-17's
rollback assertion retires with its trigger). A/B against b8fd68f;
finer values from best-of-5 probes (the spec and spin rows are
probe-only — brief suite rows added for this read were dropped as
confusing to consumers):

| row | before | after |
|---|---|---|
| pool rt (probe) | 0.796 us/task | 0.648 us/task (-19%) |
| pool spec rt (probe) | 1.308 us/task | 0.859 us/task (-34%) |
| channel spin rt (probe) | 0.398 us/rt | 0.379 us/rt (-5%) |
| payload 8 KB rt (probe) | 2.361 us/task | 2.147 us/task (-9%) |
| pipelined pool batch (suite) | 1.81M tasks/s | 3.89M tasks/s |
| collect_all_async (suite) | 1.74M tasks/s | 3.35M tasks/s |
| 64 waiters (suite) | 1.90M tasks/s | 3.18M tasks/s |

The resolution cache's win scales with how decode-bound the row is: the
batch and async fan-out rows (worker decode on the hot path) roughly
double, the submitter-bound pipelined row is flat (isolated A/B 2.04M ->
1.95M, inside its wide band), and the spin row keeps ~5% with the
control channel row flat. The before build's run died once in the
executor 9b row ("task handle already collected" — a pre-existing
submit/drain race from #31's fused re-check, not this phase); the 9a
rows were captured ahead of it. Best-known swaps: pipelined pool batch
3.89M, payload 8 KB 2.1, fan-out 296.4k, plan k = 16 147.8, executor
sync 4.5, collect_all_async 3.35M, 64 waiters 3.18M.

Status: full suite 1113 pass, 1 skip; ruff + pyrefly clean.

Post-review follow-ups (7bd317a, 88386df): the map-result cleanup walks
only the initialized plan prefix (a mid-list publish failure left later
entries unread), and the task frame's view array is stack-seeded (a task
with <= 4 buffer leaves pays no allocation). Re-confirmation A/B against
b8fd68f (suite pair, then best-of-5 probes; the baseline built in place
from a worktree):

| row | before | after |
|---|---|---|
| pool rt (probe) | 0.856 us/task | 0.692 us/task (-19%) |
| pool spec rt (probe) | 1.453 us/task | 1.040 us/task (-28%) |
| buffer task rt (probe) | 2.206 us/task | 2.003 us/task (-9%) |
| channel spin rt (probe) | 0.411 us/rt | 0.409 us/rt (flat) |
| pipelined pool batch (suite) | 1.82M tasks/s | 4.09M tasks/s |
| collect_all_async (suite) | 1.78M tasks/s | 3.67M tasks/s |
| 64 waiters (suite) | 1.99M tasks/s | 3.44M tasks/s |
| payload 8 KB (suite) | 2.5 us/task | 2.1 us/task |
| executor sync (suite) | 7.1 us/task | 4.3 us/task |
| fan-out x 2000 (suite) | 284k tasks/s | 304k tasks/s |

The submitter-bound pipelined pool row stayed in its band across seven
alternating pairs (baseline ~2.0M, HEAD ~1.9M — the suite's single 1.73M
reading was a cold first run); the phase does not touch the submit path.
Control rows flat (sequential channel rt, map rows, executor
outstanding). Best-known swaps: pipelined pool batch 4.09M,
collect_all_async 3.67M, 64 waiters 3.44M.

Status: full suite 1115 pass, 1 skip; ruff + pyrefly clean.

## 2026-11-08 — Phase 4 of the performance plan [(bfc9575, 5d52a42, b2d86cc, ed1c591, 5760515)]

Phase 4 of .posit/assistant/plans/2026-11-08-performance-improvements.md:
the compact codec's size/write passes fuse into one validating walk (the
writer's exact bytes pinned by the new `_codec_stream` hook), a
`pymizu.Frame` caches its stage construction at the first same-language
send, and the clean validity verdict memoizes on `_MapOutView` and
`_ShmView`. A/B against 38cde4d (alternating suite runs + best-of-5
probes):

| row | before | after |
|---|---|---|
| view arrow export, map / converted (suite) | 411.0 / 410.6 us/export | 10.6-11.0 / 10.1 us/export (~40x) |
| frame resend, heap-backed (suite) | 10.5 us/send | 9.1 / 9.6 us/send |
| codec list rt (probe) | 0.584 / 0.622 us/rt | 0.521 / 0.549 us/rt |
| codec dict rt (probe) | 0.642 / 0.649 us/rt | 0.582 / 0.645 us/rt |

The region-backed resend row is flat (0.5 us/send): a fully-borrowed
frame stages remote leaves, O(1) either way. Regression guards within
their bands.

Status: full suite 1125 pass, 1 skip; ruff + pyrefly clean.
