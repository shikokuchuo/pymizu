# Benchmark records

Report-only numbers from `benchmarks/rei-bench.py` (Apple M4 Pro unless
noted); asserts nothing, so records live here. Newest first.

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
