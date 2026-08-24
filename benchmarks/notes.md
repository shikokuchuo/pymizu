# Benchmark records

Report-only numbers from `benchmarks/rei-bench.py` (Apple M4 Pro unless
noted); asserts nothing, so records live here. Append in date order.

## 2026-08-24 — fast-path codec + STR1 producer (phases 1–2)

After the C codec (bool/int/float/str/bytes + flat containers) and the
STR1 string tier landed; payload tiers unchanged.

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

Channel ping-pong micro-benchmark (send + recv, best of 3,000), isolating
the staging path before/after the codec:

| payload | before (pickle) | after (codec) |
|----|----|----|
| `42` | 2.51 µs/rt | 0.48 µs/rt |
| `"hello"` | 2.59 µs/rt | 0.49 µs/rt |
| `[1, 2.5, "x"]` | 2.61 µs/rt | 0.59 µs/rt |

## 2026-08-24 — SHM_VEC zero-copy view tier (phase 3)

The producer stages eligible buffers past max(inline budget, REI_ZC_FLOOR)
as an REIH layout region (channel: past REI_ZC_FLOOR_RAW, the arena copy
serving below it); the consumer wraps the region as a read-only buffer
view (numpy `frombuffer` / memoryview) instead of copying out. Before/after
(best of 5 x 30 round-trips, identity task / echo peer):

| scenario | before (copy tiers) | after (views) |
|----|----|----|
| pool, 8 MB payload | ~848 µs/task | ~731 µs/task |
| channel, 8 MB payload | ~1,210 µs/rt | ~539 µs/rt |
| channel, 800 KB payload | ~38 µs/rt | ~49 µs/rt |

The 8 MB channel round trip halves (two memcpy elided). The 800 KB channel
case regresses slightly: each view pays a fresh open/mmap/munmap per
receive — pyrei has no consumer-side view cache (rei's `zc.c` name-keyed
cache amortizes exactly this). A cache is the follow-up if real workloads
show it; until then the floor policy mirrors rei's.

## 2026-08-24 — consumer-side view cache (phase 4)

A per-handle name-keyed LRU cache (`REI_OPEN_CACHE_MAX` entries) of shared
mapping owners: repeat receives of a live region pay a counted add instead
of a fresh open/fstat/mmap, and an evicted owner's mapping closes only when
its last view is gone. Same ad-hoc measurement as the phase-3 table
(best of 5 x 30 round-trips, echo peer):

| scenario | with the cache |
|----|----|
| channel, 800 KB payload | ~19 µs/rt |
| channel, 8 MB payload | ~215 µs/rt |

## 2026-08-24 — structured task-frame codec (phase 5)

Pool task payloads stage as a PYREI_TAG_TASK stream in the compact codec:
fn by (module, qualname) reference — or its own protocol-4 pickle when not
referenceable — and args/kwargs as codec scalars, None, one flat container
level, or buffer leaves (inline bytes; past max(inline budget,
REI_ZC_FLOOR) a SHM_VEC region referenced by name, the BUFREF leaf, one
per frame — the core's staging seam holds a single spill checkout). A
BUFREF argument arrives as a read-only view. Full-suite run of
benchmarks/rei-bench.py against the phase 1-2 baselines:

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

The 8 MB row lands at the channel's zero-copy figure (~215 µs), as
designed.

## 2026-08-24 — template output area (map improvements, phase 1)

`Pool.map(template=...)` stages an n x m output area in the map region:
runners write results in place and collect is one gather memcpy — or
none with `collect="view"`. Map-suite run (best of 3, 4
workers, winsum x n -> n x 2 float64) against the same-day baseline:

| scenario | baseline (plain) | phase 1 |
|----|----|----|
| map n=2,000 | 442,188 elts/s | 645,604 copy / 651,935 view |
| map n=20,000 | 477,594 elts/s | 696,252 copy / 689,039 view |

No regression on the plain path (442k/478k vs the 458k full-suite
baseline; run-to-run jitter). The remaining gap to in-process scaling is
the Python element loop — phase 3's target.

## 2026-08-24 — prepared maps (map improvements, phase 2)

`Pool.map_prepare(fn, x, ...)` stages once into a persistent region;
`Pool.map_run(handle)` re-arms in O(1) (`_map_reset`: generation bump,
CLAIM re-stamp, cursor/cancel clear — the runner payload now carries the
run's generation, fencing a prior run's straggler out of the re-armed
run) and reuses the workers' name-keyed context cache. A view-collected
run transfers its region to the view and restages fresh on the next run.
Repeated-map timings (best of 5 x reps, 4 workers):

| scenario | plain | prepared |
|----|----|----|
| identity n=50 (x50) | 101.0 µs/map | 19.6 µs/map |
| identity n=200 (x50) | 74.1 µs/map | 31.2 µs/map |
| winsum n=200 (x20) | 620 µs/map | 574 µs/map |

The fixed stage/unlink cost was the whole call at small n with a trivial
fn; with the ~5 µs winsum task compute dominates and the gain is the
expected ~46 µs/map of staging overhead.
