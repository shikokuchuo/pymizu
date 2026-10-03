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
| pipelined, pool batch | 1.25M tasks/s | 2026-10-01 |
| payload 8,000 B | 2.4 us/task | 2026-09-30 |
| payload 800,000 B | 58.7 us/task | 2026-09-13 |
| payload 8,000,000 B | 107.2 us/task | 2026-10-03 |
| fan-out x 2000, 4 workers | 295.3k tasks/s | 2026-10-03 |
| streaming, channel batch | 21.6M msg/s | 2026-10-03 |
| map trivial f | 0.4 us/elt | 2026-09-13 |
| map trivial f, template copy / view | 752k / 742k tasks/s | 2026-10-03 |
| map trivial f, seed | 1.9 us/elt | 2026-10-01 |
| map trivial f, prepared | 0.4 us/elt | 2026-10-01 |
| map winsum x 2000 | 480.7k tasks/s | 2026-09-11 |
| map winsum x 20000 | 506.6k tasks/s | 2026-10-03 |
| map skewed f x 4000 | 6.8 ms wall | 2026-10-01 |
| stage memcpy | 100.5 us/send | 2026-08-30 |
| stage identity | 101.6 us/send | 2026-08-30 |
| stage int64 | 99.7 us/send | 2026-09-11 |
| frame 10col relay, unmodified REF | 6.2 us/rt | 2026-10-03 |
| frame 10col relay, one computed col | 422.3 us/rt | 2026-10-03 |
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
