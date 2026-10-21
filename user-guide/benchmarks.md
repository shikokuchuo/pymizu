# Benchmarks

Communication overhead against the stdlib `concurrent.futures` pools (Apple M4 Pro, from `benchmarks/mizu-stdlib-bench.py`).

Against `ProcessPoolExecutor` (tasks run in separate processes, with pickled payloads):

| Benchmark | pymizu | ProcessPoolExecutor | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.7 µs | 94.2 µs | 135x |
| Pipelined throughput, 1 worker | 2,390,000 tasks/s | 18,500 tasks/s | 129x |
| Parallel map overhead, trivial function, 4 workers | 0.4 µs/elt\* | 70.3 µs/elt | 176x |
| Parallel map of 2,000 ~5 µs tasks, 4 workers | 4.3 ms | 129 ms | 30x |

Against `ThreadPoolExecutor` (tasks share one process, so the GIL caps CPU-bound work at a single core):

| Benchmark | pymizu | ThreadPoolExecutor | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.7 µs | 9.5 µs | 14x |
| Pipelined throughput, 1 worker | 2,390,000 tasks/s | 405,000 tasks/s | 5.9x |
| Parallel map overhead, trivial function, 4 workers | 0.4 µs/elt\* | 2.6 µs/elt | 6.5x |
| Parallel map of 2,000 ~5 µs tasks, 4 workers | 4.3 ms | 50 ms | 12x |

Against `InterpreterPoolExecutor` (Python 3.14+, subinterpreters with per-interpreter GILs), the trivial-task rows land between the thread and process pools: 21.3 µs per round trip and 59,600 tasks/s pipelined on this machine (measured 2026-10-07). It cannot run the map rows at all. Task payloads must be shareable: builtin scalars, plain containers, or buffer-protocol objects. numpy arrays and numpy scalars raise `NotShareableError`.

\* elt = element, in microseconds of wall time per map element.

## How pymizu compares

pymizu occupies a specific point in the design space: parallelism across processes on a single machine, with data moving through shared memory instead of serialized copies. The tools in this table overlap at the edges. They differ in what has to run, how data moves, and what survives a crash.

[TABLE]

### Process pools: `multiprocessing`, `concurrent.futures`, joblib, ipyparallel

The stdlib pools are the baseline that the benchmark tables measure.

joblib's `Parallel`/`delayed` is the long-standing answer for an embarrassingly parallel loop over numpy data. Its `loky` backend is the closest in spirit to pymizu: a reusable process pool that memmaps arrays past a size threshold to a temp folder instead of pickling them. That removes the pickle copy. The array still round-trips through the filesystem, and a task round trip stays in the millisecond range.

ipyparallel answers the same question for Jupyter. A controller process brokers pickled messages over ZeroMQ sockets to engine processes. A controller in the loop and socket copies are the price of its generality.

`multiprocessing.shared_memory` exposes raw shared buffers and leaves synchronization, lifetime tracking, and crash cleanup to you. pymizu is that idea finished: the same pages, with the waiting, framing, and peer-death detection built in.

### Distributed frameworks: Ray and Dask

Ray and Dask are designed to spread work across machines. On one machine you pay for that design. A local Ray instance still starts a scheduler, an object store, and a dashboard. A task round trip costs on the order of a millisecond.

Dask's `distributed` scheduler is similar. Its process-free threaded scheduler shares memory, but the GIL caps it for Python-level work. Ray's object store does read numpy arrays without copying on the same node. The difference is overhead and footprint. If the work fits one machine and the tasks are measured in microseconds, the framework overhead dominates the work.

### Modern CPython: free-threading and subinterpreters

Python 3.14 officially supports a build with the GIL removed, in which threads share one heap and exchange data with no copies at all. This is the right answer for single-language CPU parallelism when isolation is not needed. A crashing thread takes the process down, C extensions must be built and validated for it, and it does nothing for crossing into R. pymizu targets the standard GIL build and does not yet support the free-threaded one. The two approaches solve different problems: threads trade isolation for sharing, and pymizu keeps the isolation and makes the sharing cheap.

Python 3.14 also added `concurrent.interpreters` to the stdlib ([PEP 734](https://peps.python.org/pep-0734/)). It runs multiple interpreters in one process, each with its own GIL. CPU work parallelizes without new processes on the standard build. Objects that pass between interpreters must be shareable. Builtin scalars and plain containers of them cross as copies. Buffer-protocol objects can share their underlying memory, with synchronization left to you. Anything else, numpy arrays included, is rejected outright. The isolation is real but not OS-level: a fatal error in one interpreter ends the process. The surrounding machinery (queues, lifetimes, crash handling) is yours to build. `concurrent.interpreters` is also Python 3.14 and later only. pymizu runs on Python 3.10 and later.

### The one thing only pymizu does

A pool whose workers are R processes, exchanging numpy arrays and data frames as shared-memory views ([R interop](interop.md)). None of the other tools in this guide cross the language boundary.

The short version: choose pymizu when the work fits one machine, when tasks are fine-grained or arrays large, or when R needs to be in the loop. Choose Ray or Dask when you outgrow one machine. Choose `concurrent.interpreters` when you want stdlib-only isolation and can require Python 3.14. Choose free-threading when one language and shared-everything is acceptable.

`benchmarks/mizu-bench.py` runs the pymizu rows standalone, `benchmarks/mizu-stdlib-bench.py` the comparison, and `benchmarks/crosslang-map-bench.py` the cross-language map comparison. The scripts are report-only and assert nothing about the numbers: CI runner timing is too variable. Dated records live in `benchmarks/notes.md`.
