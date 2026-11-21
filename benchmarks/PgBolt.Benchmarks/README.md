# PgBolt.Benchmarks — locking and lock-free synchronization in the runtime

These BenchmarkDotNet 0.15.8 microbenchmarks compare five synchronization points in `src/PgBolt.Server/Runtime`. Each pairs a copy of the relevant runtime code with a lock-free alternative. Stubs replace the database, network and PgBouncer. The benchmark uses copies in `Copies/` without changing production code or exposing internal types through InternalsVisibleTo. Each copy identifies its source file and commit. The benchmark compiles the original `MonoClock`, which contains no synchronization.

The project sets `IsTestProject=false` and has no test SDK. Run it separately from `dotnet test PgBolt.slnx`.

## Run

```bash
# Stress-test invariants. Failure returns 1. Select a group with --verify event|sink|slots|table|deadlines
dotnet run -c Release --project benchmarks/PgBolt.Benchmarks -- --verify

# Run all benchmarks. Main runs --verify first and stops on failure. Takes ~20 min on M4 Pro.
dotnet run -c Release --project benchmarks/PgBolt.Benchmarks -- --filter '*'

# Benchmark one component
dotnet run -c Release --project benchmarks/PgBolt.Benchmarks -- --filter '*SlotAllocator*'

# Measure ping-pong latency percentiles separately from BenchmarkDotNet
dotnet run -c Release --project benchmarks/PgBolt.Benchmarks -- --latency

# Smoke-test all cases with Job.Dry. Takes ~15 s.
PGBOLT_BENCH_DRY=1 dotnet run -c Release --project benchmarks/PgBolt.Benchmarks -- --skip-verify --filter '*'
```

BenchmarkDotNet writes reports to `BenchmarkDotNet.Artifacts/`. The `--latency` command prints the path to its report.

## Method

- The `Concurrent` job uses two processes, each with four warmup iterations and twelve measured iterations of about 250 ms, with `UnrollFactor=1`. Two processes allow for variation in P-core and E-core scheduling. `MemoryDiagnoser` reports process-wide bytes allocated per operation. `ThreadingDiagnoser` reports thread-pool work items and lock contentions. The report includes Median, P95, Min and Max.
- Every concurrent scenario creates dedicated threads in `GlobalSetup` (`WorkerPool`). A `Barrier` releases them together and each thread runs a fixed share of the operations. `OperationsPerInvoke` equals the total operation count. **Mean is elapsed time per operation across all threads**, so throughput is 1/Mean. `[Params]` sets the thread count. The lock implementation is the baseline (Ratio = 1).
- Before measurement, `Verification/Verifier.cs` stress-tests the key invariant for each prototype and the corresponding runtime copy.

### Implementations and workloads

| # | Component | Baseline (copy) | Alternative | Workload |
|---|---|---|---|---|
| 1 | `AsyncAutoResetEvent` | Lock and a `LinkedList` of waiters (`aaf1738`) | Single slot with CAS (`52b4a8f`, already in HEAD at measurement time) | (a) N∈{1,4,8,12} threads call Set while one waiter loops. (b) Set/Wait ping-pong. (c) Idle `WaitAsync` with 0 and 1 ms timeouts. |
| 2 | `CompletionSink`, ack path | One lock covers all work, including `Task.Run` and `ContinueWith` | (b) Cheap lock: only the unstarted task and `TryAdd` are inside the lock. `Start` and `ContinueWith` follow. (c) Lock-free: `ConcurrentQueue`, a counter, one CAS-elected drainer and a Dekker-style stop protocol. (c′) Same as (c), but only the increment reaching exactly `AckBatchSize` triggers draining. | N∈{1,4,8,12} threads call `Complete(ack)`. Default `AckBatchSize=100` and `AckFlushInterval=20 ms`, real timer, stubbed send. |
| 3 | `SlotAllocator` | Lock | Interlocked: `Release` decrements the queue counter before the overall counter. Only the dispatcher calls `Take` and `Available`. | One `Available`+`Take` thread and R∈{1,3,7,11} `Release` threads (2/4/8/12 total). SPSC rings pass slots between them. |
| 4 | `ExecutionTable.Snapshot` | `new List(_byLease.Count)` acquires all dictionary stripes for Count | Enumerate without Count, or find the minimum deadline during enumeration without a List | Snapshot or minimum time with 100 or 10,000 entries and 0 or 4 background writers. Add+Remove throughput with N∈{1,4,8,12} threads while a separate thread **continuously** snapshots the table (worst case). |
| 5 | `LeaseKeeper.ArmAtMost`/`_timerLock` | Lock | Seqlock epoch: Rearm makes the epoch odd and then even. A lock still serializes Rearm calls. After `Add`, `ArmAtMost` issues a full barrier, reads the epoch and rechecks it after `Change`. | Register (new Execution + Add + ArmAtMost) and Unregister on the dispatcher thread with 100 background entries. A separate thread runs Rearm never, about every 1 ms or about every 10 µs. Deadlines increase as in production or decrease to force `Change` each time. The timer is stubbed. |

## Correctness (`--verify`)

**All 18 invariant checks passed in 95 s.** The deadline check detected the deliberately incorrect ArmAtMost variant without an epoch.

| Component | Invariant and stress workload | Result |
|---|---|---|
| AutoResetEvent | No lost wakeups. Over 3,000 rounds, four threads call `Set` and the waiter calls `WaitAsync` with a timeout of 0, 1 or 20 ms or cancellation after 0–200 µs. If the waiter has not seen all Set calls by the end of a round, `WaitAsync(2 s)` must return true. A separate ping-pong runs for 100,000 round trips. | Lock and CAS: 0 lost wakeups, 0 timeouts |
| CompletionSink | (1) Every ack from 8×25,007 `Complete` calls is sent exactly once, including the final partial batch flushed by the timer. (2) In 40 races between `StopAsync` and continuous `Complete` calls, each accepted ack must be sent exactly once before Stop returns. Rejected acks must never be sent and no sends may occur after Stop. | (a), (b), (c), (c′): 0 violations. The final batch flushed within 3–23 ms across variants. |
| SlotAllocator | 0 ≤ slots in use ≤ limit in an exhausted pool (Workers 8, Reserved 2 and 1, MaxConcurrency 4) over 300,000 `Take` calls. Check counters after each `Take` and `Release`, track held slots independently and verify all counts end at zero. | Lock and Interlocked: 0 violations |
| ExecutionTable | Every snapshot retains all 500 persistent entries while 6 threads call Add/Remove over 20,000 snapshots. The reported minimum cannot exceed the minimum of the persistent entries. | 0 missing entries, 0 incorrect minima |
| LeaseKeeper | No missed deadlines over 1,000,000 `Register` calls with random deadlines, two Rearm threads and three removal threads. While an entry is in the table, the timer must be armed no later than its deadline. Check immediately after Register and before removal. | Lock 0, epoch 0. **Naive CAS without an epoch: 245 + 2940 violations.** |

Mutation checks validated the verifier. Removing the post-publication `_stopped` recheck in the lock-free sink caused the Stop check to fail (5–8 accepted acks unsent). Removing the timer's `_timerArmed` reset lost the final batch (24–33 unsent).

## Results

Apple M4 Pro (8P+4E, 12 cores), macOS 27.0, .NET 10.0.9 Arm64. Times are per operation across all threads. Δ is the change in time relative to the lock baseline. Negative values indicate faster operations. ×N is the throughput multiplier. Allocations are process-wide bytes per operation.

### 1. AsyncAutoResetEvent — CAS (already in HEAD) versus lock (aaf1738)

| Scenario | Threads | lock | CAS | Δ | Allocations lock → CAS |
|---|---:|---:|---:|---:|---|
| Set with one waiter | 1 | 8.97 ns | 0.78 ns | −91 % (×11) | 2 B → 0 |
| | 4 | 8.84 ns | 0.37 ns | −96 % (×24) | 0 → 0 |
| | 8 | 11.11 ns | 0.35 ns | −97 % (×32) | 0 → 0 |
| | 12 | 11.07 ns | 0.48 ns | −96 % (×23) | 0 → 0 |
| Ping-pong round trip (BDN, async) | 2 | 2.09 µs (median 1.92) | 1.51 µs (median 1.44) | −27 % (RatioSD 0.12) | 915 B → 606 B (−34 %) |
| Idle `WaitAsync(0)` | 1 | 6.8 ns | 14.1 ns | **+108 %** | 0 → 0 |
| Idle `WaitAsync(1 ms)` | 1 | 1.322 ms | 1.321 ms | 0 % (timer resolution dominates) | 1383 B → 1255 B (−9 %) |

Almost every Set finds an already-signaled latch. CAS needs a single read, while the lock variant enters and exits the lock. A separate ping-pong run (`--latency`, blocking wait, 3×200,000 round trips) measured mean times of 2.55–3.10 µs for lock and 2.52–2.76 µs for CAS. The respective p50 ranges were 2.67–3.08 and 2.62–2.88 µs. The p99 ranges were 3.71–9.21 and 3.92–6.21 µs. Run-to-run variation exceeds the difference between variants, so the measurements show no clear tail-latency gain. CAS costs about 7 ns more for `WaitAsync(0)`. One unverified explanation is the Arm64 GC write barrier helper used by `Interlocked.CompareExchange` on a reference field, compared with a lock around a `bool` field.

**Keep CAS.** The Set and ping-pong gains are measurable, allocations fall and the correctness checks pass. The absolute regression on a zero timeout is small.

### 2. CompletionSink.Complete(ack)

The main run compared four variants.

| Threads | (a) lock | (b) cheap lock | (c) lock-free | (c′) lock-free, trigger `==` | Allocations a / b / c / c′ |
|---:|---:|---:|---:|---:|---|
| 1 | 35.1 ns | 34.7 ns (−1 %) | 39.9 ns (+14 %, bimodal, median 47.6) | 24.4 ns (−30 %) | 45 / 45 / 13 / 13 B |
| 4 | 48.9 ns | 44.1 ns (−8 %) | 139.1 ns (+190 %, ×0.34) | 124.8 ns (+160 %) | 45 / 45 / 20 / 19 B |
| 8 | 49.8 ns | 46.1 ns (−6 %) | 271.4 ns (+451 %, ×0.18) | 263.4 ns (+435 %) | 45 / 45 / 38 / 52 B |
| 12 | 52.1 ns | 49.7 ns (−4 %) | 448.0 ns (+765 %, ×0.12) | 438.6 ns (+747 %) | 45 / 45 / 38 / 35 B |

In the first run, with three variants, (b) changed elapsed time by −2 / +17 / +11 / +5% relative to (a), and (c) by +24 / +224 / +502 / +847% on 1 / 4 / 8 / 12 threads.

- **With four or more threads the lock-free variants are 2.6–8.7× slower.** On 12 threads the lock handles about 19 million Complete calls/s and the lock-free variant about 2.2 million/s. Variant (c′) tested whether repeated failed CAS attempts on the drainer flag caused the slowdown. It reduced (c)'s time by only 2–10% on 4–12 threads. Two contended atomic read-modify-write operations on separate cache lines (`ConcurrentQueue.Enqueue` and the counter's `Interlocked.Increment`) appear expensive. One hypothesis is that under contention `System.Threading.Lock` needs one atomic operation and its holder runs the short critical section repeatedly. No profile verified that mechanism. With a single thread (c′) is 30% faster and allocates 13 B versus 45 B. The concurrent workload is the reason to consider changing the sink.
- **Cheap lock falls within the noise.** On four threads the difference switched from +17% to −8% across runs, with RatioSD up to 0.2.
- With continuous `Complete` calls, lock-free `StopAsync` effectively waits for that stream to subside. The Stop test accepted about 389,000 of 400,000 entries with the lock-free variant, compared with about 69,000 before the lock variant finished. The lock variant can observe an empty buffer while holding the lock. The lock-free variant almost always finds another send.

**Keep the lock.** The lock-free version is slower under contention and requires a Dekker-style protocol on `_stopped`, a permanently set drainer flag after stopping, a `_timerArmed` protocol and a window measured from the last timer firing. Stop behavior is also a concern. Cheap lock adds a gap between registering a task under the lock and starting it after releasing the lock, without a measurable gain.

### 3. SlotAllocator — Take + Release

| Threads (1 dispatcher + R) | lock | Interlocked | Δ | Allocations |
|---:|---:|---:|---:|---|
| 2 (R=1) | 76.1 ns | 71.7 ns | −5 % (within RatioSD 0.09) | 0 / 0 |
| 4 (R=3) | 198.5 ns | 78.7 ns | −60 % (×2.5) | 0 / 0 |
| 8 (R=7) | 469.9 ns | 174.2 ns | −63 % (×2.7) | 0 / 0 |
| 12 (R=11) | 561.5 ns | 206.5 ns | −63 % (×2.7) | 0 / 0 |

The relative gain is consistent, but the absolute cost is 0.2–0.56 µs per acquired job. At 10,000 jobs/s that amounts to 2–6 ms of CPU time per second alongside job execution and database access.

**Keep the lock unless profiling identifies contention.** The replacement changes three guarantees.

1. `Plan`/`ISharedPoolDistribution` loses its atomic counter snapshot. In `NextEntry`, the strategy contract check throws `InvalidOperationException` if the plan has ended while a ready queue has a free slot. A `Release` between the strategy result and the check can increase `Available`, causing a false violation.
2. `Take` now relies on a single caller, an implicit invariant that must be protected.
3. A double `Release` is detected only after `Decrement`.

The microbenchmark does not exercise Plan.

### 4. ExecutionTable.Snapshot

Time for one snapshot or minimum calculation:

| Entries | Background writers | Snapshot (baseline) | without Count | Δ | Allocations | Minimum via Snapshot + loop | by enumeration | Δ | Allocations |
|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| 100 | 0 | 732 ns | 409 ns | −44 % | 856 → 2192 B | 996 ns | 303 ns | −70 % | 896 B → 0 |
| 100 | 4 | 4.00 µs | 0.85 µs | −79 % | (*) | 4.96 µs | 1.28 µs | −74 % | (*) |
| 10 000 | 0 | 56.4 µs | 81.2 µs | **+44 %** | 80 KB → 262 KB (LOH, Gen2) | 79.8 µs | 48.8 µs | −39 % | 80 KB → 0 |
| 10 000 | 4 | 107.3 µs | 118.3 µs | +10 % | (*) | 139.6 µs | 60.2 µs | −57 % | (*) |

(*) Background writers allocate memory themselves. MemoryDiagnoser measures the entire process, so the Allocated column on those rows cannot isolate snapshot allocations.

Time per Add+Remove pair with N writer threads while a separate thread takes snapshots continuously. Δ is relative to the current Snapshot variant.

| Entries | Threads | no snapshots | Snapshot (baseline) | without Count | minimum by enumeration |
|---:|---:|---:|---:|---:|---:|
| 100 | 1 | 32.8 ns | 162.2 ns | 48.2 ns (−70 %) | 40.7 ns (−75 %) |
| 100 | 4 | 45.1 ns | 142.4 ns | 65.7 ns (−54 %) | 66.0 ns (−54 %) |
| 100 | 8 | 67.8 ns | 141.1 ns | 69.6 ns (−51 %) | 63.4 ns (−55 %) |
| 100 | 12 | 49.3 ns | 138.1 ns | 58.4 ns (−58 %) | 69.0 ns (−50 %, RatioSD 0.10) |
| 10 000 | 1 | 31.1 ns | 45.8 ns | 54.1 ns (+18 %) | 34.5 ns (−25 %) |
| 10 000 | 4 | 20.9 ns | 42.5 ns | 35.3 ns (−17 %) | 23.6 ns (−44 %) |
| 10 000 | 8 | 19.1 ns | 58.4 ns | 33.8 ns (−42 %) | 23.4 ns (−60 %) |
| 10 000 | 12 | 19.6 ns | 47.0 ns | 36.9 ns (−21 %) | 22.5 ns (−52 %) |

Allocations per pair were 56 B without snapshots (the dictionary node), 71–235 B with the current Snapshot, 112–303 B without Count, and 56–59 B when finding the minimum during enumeration.

`ConcurrentDictionary.Count` locks all stripes, slowing both snapshots and writers. On the small table, removing Count reduces snapshot time by 44–79% and makes writers 2–3× faster with continuous snapshots. At 10,000 entries, a List without initial capacity grows by doubling, roughly triples allocations and reaches the LOH. The snapshot without Count takes 10–44% longer. Finding the minimum deadline during enumeration reduces time by 39–74% in all cases, allocates nothing and barely slows writers.

**In `RearmDeadlineTimer`, find the minimum during enumeration.** Replacing Snapshot and the following loop is a local change that preserves semantics. Enumeration already ran without locks, and Count never made the snapshot atomic. For the heartbeat `Snapshot()`, removing Count is faster at typical execution counts (roughly Workers, tens to hundreds) but slower at 10,000. Sizing the List from a known estimate such as Workers is a possible compromise that **was not benchmarked**. The cost is a few microseconds per HeartbeatInterval, so the priority is low.

### 5. LeaseKeeper.ArmAtMost — epoch (seqlock) versus lock

Time per Register + Unregister pair:

| Rearm | Deadlines | lock | epoch | Δ | Allocations |
|---|---|---:|---:|---:|---|
| none | increasing | 54.0 ns | 51.2 ns | −5 % | 208 / 208 B |
| none | decreasing | 55.3 ns | 54.0 ns | −2 % | 208 / 208 B |
| ~1 ms | increasing | 54.3 ns | 51.9 ns | −4 % | 208 / 208 B |
| ~1 ms | decreasing | 57.1 ns | 57.0 ns | 0 % | 208 / 208 B |
| ~10 µs | increasing | 83.2 ns | 79.0 ns | −5 % | 219 / 219 B |
| ~10 µs | decreasing | 87.3 ns | 86.2 ns | −1 % | 220 / 220 B |

`new Execution` (including a CTS) and dictionary `Add`/`Remove` account for almost all registration time. The epoch saves an uncontended lock acquisition and release, worth 0–4 ns per registration.

**Keep the lock.** The 0–5% gain (a few nanoseconds) does not justify a seqlock, waiting on odd epochs and repeating `Change` when it overlaps Rearm. The naive variant without an epoch is **incorrect**, as the verifier demonstrates. The correct variant requires a careful proof. During verification two threads calling Rearm continuously nearly starved registration on the unfair lock. The first version of the check failed to complete 300,000 Register calls in 7 minutes. Rearm is rare in production, but the lock provides no progress guarantee under this stress workload.

## Summary

| Component | Recommendation | Evidence |
|---|---|---|
| AsyncAutoResetEvent | **Keep CAS** (already in HEAD) | Set throughput ×11–×32, ping-pong −27%, allocations −34%. Correctness checks passed. |
| CompletionSink | **Keep the lock** | Lock-free is 2.6–8.7× slower with ≥4 threads. Cheap lock falls within the noise. Stop is more complex. |
| SlotAllocator | **Keep the lock** | −60% in the microbenchmark but only 0.2–0.5 µs per job. The alternative breaks the atomic snapshot for Plan and the strategy contract. |
| ExecutionTable | **Find the minimum during enumeration in RearmDeadlineTimer.** Changing Snapshot() is optional. | −39% to −74% with zero allocations. Removing Count improves snapshots on small tables and slows them at 10,000 entries. |
| LeaseKeeper.ArmAtMost | **Keep the lock** | The 0–5% gain amounts to a few nanoseconds and requires a complex seqlock. |

## Limitations

- **macOS and P/E cores.** Threads could not be pinned to cores. The scheduler distributes them over 8 P and 4 E cores, and the benchmark does not control QoS. BenchmarkDotNet could not raise process priority (“Permission denied”). The distribution can be bimodal; for example, variant (c) on one thread had a mean of 39.9 ns and median of 47.6 ns. Linux x64 may yield different ratios, particularly for contended atomic operations.
- **Oversubscription.** With 12 worker threads, the waiter, dispatcher or snapshot thread brings the total to 13 active threads on 12 cores.
- **Noise.** RatioSD reached 0.2 for CompletionSink on four threads. The sign of the difference between (b) and (a) changed across two runs, so differences below about 15% cannot be distinguished here. BenchmarkDotNet ping-pong uses async continuations and `--latency` uses a blocking wait. They measure different mechanisms and produce different absolute times.
- **No database I/O.** Ack sending, the store and the LeaseKeeper timer use stubs. All variants cost nanoseconds or microseconds per job, while database access takes milliseconds. A 60% microbenchmark gain alone cannot establish the impact on an application.
- **Worst cases.** The write benchmark takes snapshots continuously, whereas production takes them once per HeartbeatInterval and on timer callbacks. Rearm every 10 µs also exaggerates contention.
- **Allocated** covers the entire process, including background writers.
- **Lock Contentions** was close to zero almost everywhere. `System.Threading.Lock` usually resolves contention by spinning. The counter records only blocking waits.
- The copies include only the hot path. Each file in `Copies/` identifies the omissions. The lock-free CompletionSink prototype supports only ack.
