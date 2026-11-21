# Load testing, SLOs, and performance results

The load harness is the `benchmarks/PgBolt.LoadTest` console application. It saves full reports locally. This report records the test conditions and findings.
For the methodology, commands, flags, and report format, see the [load harness README](../benchmarks/PgBolt.LoadTest/README.md).

**7b‑2 results, 21 November 2026.** On stock PostgreSQL, the final build sustained 10,000 enqueues/s for ten minutes in both connection modes.
SLOs 8 and 9 remain open for sustained load and recovery after a transaction holds xmin. [Measurements and fixes](#stock-postgresql-under-profile-b)

## Running the tests

Install Docker and .NET 10. Without `--connection`, the harness starts PostgreSQL 17 and PgBouncer 1.24.
Run one scenario per command.

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- <sustained|ramp|partitions|vacuum> [flags]
```

For modes, flags, exit codes, and the duration of the full suite, see the [load harness README](../benchmarks/PgBolt.LoadTest/README.md).

## Profile B and SLOs

Profile B targets 10,000 enqueues/s with a 1-minute warmup and a 10-minute measurement window. The `default` and `critical` queues receive
80% and 20% of jobs, respectively. Both hosts serve both queues. Of all jobs, 90% are immediate and 10% have a delay of 1–30 s.
1% fail on their first attempt and retry under the default policy. Each job performs 1 ms of work and has ~200 bytes of arguments.
There are 2 hosts with 32 workers each (`Maximum Pool Size=20`) and a producer in a separate `IHost` (`Maximum Pool Size=8`).
As with a separate producer service, in-process wakeup signals cannot reach the hosts. Their dispatchers discover new jobs by polling.

| # | SLO | Threshold |
|---|---|---|
| 1 | Throughput | Every 10 s window completes ≥ 95% of the target rate. A window with insufficient enqueue load fails. |
| 2 | Immediate job start latency (enqueue → start) | p50 ≤ 50 ms, p99 ≤ 500 ms, p99.9 ≤ 2 s |
| 3 | Delayed job lateness (run_at → start) | p99 ≤ 1.5 s |
| 4 | Backlog of ready, unleased jobs | Each sample ≤ rate × 1 s. Last-minute mean ≤ first-minute mean + rate × 1 s. |
| 5 | Correctness | All `jobs` Succeeded, `queue` empty, executions and `attempt` reconciled by `Seq`, `outcome.dropped` = 0, `storage.errors` = 0 |
| 6 | Connections | `bouncer` requires `maxwait` ≤ 100 ms in each sample. `direct` requires ≤ 48 connections. |

The harness evaluates the SLOs separately in `direct` (direct connection) and `bouncer` (PgBouncer, `pool_mode=transaction`) modes.

## Environment

| Parameter | Value |
|---|---|
| Host | Apple M4 Pro, 12 cores |
| Docker Desktop | 6 CPUs, 4.8 GB |
| PostgreSQL | 17.11 (`postgres:17-alpine`) with `shared_buffers=1GB`, `max_wal_size=4GB`, `max_connections=100`, `max_locks_per_transaction=64`, and default autovacuum settings (`naptime` 1 min, 3 workers, `cost_delay` 2 ms) |
| PgBouncer | 1.24.1 with `default_pool_size=40` and `max_prepared_statements=200` |
| `pg_stat_statements.track_planning` | Off except in `partitions`, where it is on |
| `queue` reloptions | `autovacuum_vacuum_scale_factor=0, autovacuum_vacuum_threshold=5000, autovacuum_vacuum_cost_limit=2000` |
| .NET | 10.0.9 |
| Commit | `57436ef` |

On this laptop running Docker Desktop, virtual disk fsync and the 6-CPU VM limit PostgreSQL sooner than dedicated hardware would.
Comparisons within the same run (modes, P, indexes) remain meaningful. Treat absolute figures as lower estimates.

## Results

### `sustained`

Both modes failed SLO 2. Exit code 1.

| SLO | direct | bouncer |
|---|---|---|
| 1 | PASS, min. 98.0% of target | PASS, min. 96.4% of target |
| 2 | **FAIL**, p50 96.6 ms, p99 392.4 ms, p99.9 532.0 ms | **FAIL**, p50 93.4 ms, p99 409.6 ms, p99.9 1950.7 ms |
| 3 | PASS, p99 1011.2 ms | PASS, p99 1038.3 ms |
| 4 | PASS, max. 4075, minute means 1066 → 1172 | PASS, max. 7227, minute means 1084 → 1060 |
| 5 | PASS | PASS |
| 6 | PASS, 17 connections | PASS, `maxwait` 0 ms |

Enqueue averaged ~308 calls/s with 32.4 jobs per batch, no errors, and no producer underload. The harness used 9–11% of CPU capacity.
Dequeue averaged ~810 calls/s. Mean execution times were 0.21 ms for dequeue, 1.2 ms for ack, and 0.40 ms for enqueue.
`LockManager` waits were rare, with at most 1 backend waiting and a mean of 0.00.

### `ramp`

Both runs exited with code 0.

| Mode | Highest sustainable rate | First SLO breached | Final step |
|---|---|---|---|
| direct | 12,000/s | SLO 2 (p99) | At 14,000/s, the 380 and 390 s windows had p99 of 5775 and 2621 ms. SLO 1 held (≥ 95.4%). |
| bouncer | 10,000/s | SLO 2 (p99) | At 12,000/s, the 330 and 350 s windows had p99 of 824 and 603 ms. SLO 1 held. |

These measurements support the roadmap's “5–20k jobs/sec on one PostgreSQL node” up to 10–12k/s on this environment.
Tail latency limited the steps. Throughput met SLO 1 even at the stopping step. See finding 3 under Decisions.

### `partitions`

Exit code 1. At `P = 15, current`, p50 breached SLO 2 in all modes (finding 1). In `direct-prepared`, p99 also exceeded the
500 ms threshold at 526.3 ms (finding 4). Lock table capacity was 7808.

All 32 combinations failed SLO 2. At P = 15, p50 was 97.5–105 ms with `current` (finding 1).
Both `direct-prepared, current` and `bouncer-prepared, extra` also failed p99 (526.3 and 719.9 ms).
At P ≥ 60, p99 also exceeded its threshold (815 ms to 60 s).

SLO 1 by combination

| Mode | Indexes | P = 15 | P = 60 | P = 200 | P = 400 |
|---|---|---|---|---|---|
| direct | current | PASS | FAIL (91.6%) | FAIL (40.6%) | FAIL (12.0%) |
| bouncer | current | PASS | PASS | FAIL (40.6%) | FAIL (18.1%) |
| direct-prepared | current | PASS | FAIL (88.6%) | FAIL (42.2%) | FAIL (18.4%) |
| bouncer-prepared | current | PASS | FAIL (93.5%) | FAIL (40.8%) | FAIL (8.5%) |
| all four | extra | PASS | FAIL | FAIL | FAIL |

Parentheses show the lowest window as a percentage of the target rate.

The P threshold is the smallest P in the matrix at which an SLO fails.

| Mode | `current`, report (SLO 1 or 2) | `current`, SLO 1 | `extra`, SLO 1 |
|---|---|---|---|
| direct | 15 | 60 | 60 |
| bouncer | 15 | 200 | 60 |
| direct-prepared | 15 | 60 | 60 |
| bouncer-prepared | 15 | 60 | 60 |

The report threshold comes from p50 failing SLO 2 at P = 15 (finding 1). The conclusions use `current` (finding 2).
The `extra` threshold only informs the decision about opt-in indexes (see the `extra` indexes in finding 2).

Dequeue by P (`direct`, `current`, custom plan)

| P | Locks, total / outside fast path | mean plan, ms | mean exec, ms | Calls/s | Completed/s (measurement windows) |
|---|---|---|---|---|---|
| 15 | 100 / 74 | 0.505 | 0.208 | 813 | 9930–10256 |
| 60 | 325 / 299 | 1.117 | 0.265 | 661 | 9162–11297 |
| 200 | 1025 / 999 | 4.595 | 0.449 | 259 | 4063–4266 |
| 400 | 2025 / 1999 | 11.359 | 1.014 | 114 | 1205–1902 |

Ack and enqueue were independent of P (`direct`, `current`). They took 31 and 12 locks (5 and 0 outside the fast path), with mean planning
times of 0.15–0.55 ms. Across combinations, the maximum number of backends waiting on `LockManager` ranged from 0–2, the mean was ≤ 0.05,
and `storage.errors` = 0.

### `vacuum`

Commit `1cf7245`. After a 2-minute warmup, phase 1 ran profile B for 20 minutes (120–1320 s). Phase 2 ran B for 5 minutes
with xmin held (until 1620 s). Phase 3 allowed 10 minutes for recovery (until 2220 s).
Each of three configurations ran in both modes. All six runs returned exit code 1.

| Configuration | Settings |
|---|---|
| current | `queue` reloptions from `V001`, default cluster autovacuum settings |
| `cost_delay=0` | Current settings plus `autovacuum_vacuum_cost_delay=0` on `queue` |
| `naptime=10s` | Current reloptions, cluster-wide `autovacuum_naptime=10s` |

| Configuration, mode | SLO 6 | SLO 7: 60–120 s → last minute | SLO 8: backlog growth, samples > rate | SLO 9 |
|---|---|---|---|---|
| current, direct | PASS, 18 connections | PASS, 141.9 → 147.5 MB | **FAIL**, 877 → 1425, 0 | **FAIL** |
| current, bouncer | PASS, `maxwait` 0 ms | PASS, 147.1 → 247.6 MB | **FAIL**, 1336 → 7012, 147 | **FAIL** |
| `cost_delay=0`, direct | PASS, 17 connections | PASS, 144.2 → 181.5 MB | **FAIL**, 1311 → 1559, 0 | **FAIL** |
| `cost_delay=0`, bouncer | **FAIL**, `maxwait` 455 ms | PASS, 149.7 → 251.4 MB | **FAIL**, 1026 → 7377, 182 | **FAIL** |
| `naptime=10s`, direct | PASS, 18 connections | PASS, 38.7 → 44.3 MB | **FAIL**, 1124 → 1377, 0 | **FAIL** |
| `naptime=10s`, bouncer | PASS, `maxwait` 0 ms | PASS, 45.1 → 46.1 MB | **FAIL**, 1221 → 1471, 0 | **FAIL** |

All 120 windows in phase 1 failed SLO 8 in all six runs for two reasons. First, in 48–110 windows, SLO 1, SLO 2 p99, and SLO 3 passed,
so the failure came from the window's SLO 2 p50 or p99.9. The report omits per-window p50. Profile B p50 was 96.6 and 93.4 ms in
`sustained` (finding 1). Second, the p99 tail exceeded its threshold. In five of six runs, SLO 2 p99 exceeded 500 ms in 41–72 phase 1
windows (9 for current settings in `direct`). Delayed-job p99 exceeded 1.5 s in 14–43 windows (4 for current settings in `direct`).
Only `bouncer` showed an association between tail latency and autovacuum. With `naptime=10s`, 41 windows exceeded 500 ms at p99 and 0 samples
had backlog above the rate, versus 66–72 windows and 147–182 samples for current settings and `cost_delay=0`.
In `direct`, more frequent passes did not reduce the tail (63 windows with `naptime=10s`, compared with 9 for current settings and 68 for `cost_delay=0`).

SLO 9 excludes p50. Every run reported “did not recover by the end of phase 3.” With xmin held, throughput fell to ~1k/s by the end of phase 2,
averaging 1.8–1.9k/s across the phase for every configuration. No autovacuum setting could remove the dead tuples. An estimated ~2.4 million
ready jobs accumulated in 5 minutes. Phase 3 processed 13.8–14.2k/s, but backlog stayed above the rate until 2106–2220 s, past the T deadline
(1920 s, phase 3 start + 5 min). No phase 3 window achieved p99 ≤ 500 ms. Backlog did fall below the rate in three runs (current and
`naptime=10s` in `direct`, and `naptime=10s` in `bouncer`). Their final-window p99 was 1.15–1.29 s.

One sample failed SLO 6 with `cost_delay=0, bouncer` (849 s in phase 1, `maxwait` 455 ms, `cl_waiting` 4, `sv_active` 13).
Autovacuum was idle in that sample. Every other sample in that run and both other `bouncer` runs had `maxwait` 0 (`direct` bypasses PgBouncer).
The report found no link to `cost_delay=0` and recorded no separate finding since that configuration had already been rejected.
The “enqueue” annotation for SLOs 8–9 indicates 3–9 underloaded windows out of 210.

Vacuum metrics from the `.csv` files. Size is `pg_total_relation_size(queue)`.

| Metric | current, direct | current, bouncer | `cost_delay=0`, direct | `cost_delay=0`, bouncer | `naptime=10s`, direct | `naptime=10s`, bouncer |
|---|---|---|---|---|---|---|
| Phase 1 `queue` size, mean / peak, MB | 145.3 / 147.5 | 181.6 / 258.8 | 161.4 / 181.5 | 185.3 / 261.6 | 41.6 / 44.7 | 47.4 / 56.3 |
| Peak size in phases 2 / 3, MB | 570.5 / 835.8 | 583.5 / 893.1 | 573.0 / 868.2 | 585.4 / 872.8 | 555.7 / 774.2 | 555.0 / 754.9 |
| Size at the end of phase 3, MB | 498.1 | 893.0 | 867.6 | 872.7 | 405.0 | 384.9 |
| Peak `n_dead_tup` in phase 1 / full run, millions | 1.28 / 2.49 | 2.41 / 3.15 | 1.28 / 2.42 | 2.36 / 3.12 | 0.23 / 1.59 | 0.42 / 1.58 |
| Phase 1 SLO 2 p99, ms | 412.2 | 18,743.3 | 811.5 | 24,592.4 | 753.2 | 611.8 |
| Autovacuum passes on `queue` | 37 | 35 | 37 | 37 | 222 | 220 |
| Completed/s, mean in phases 2 / 3 | 1931 / 14,184 | 1900 / 13,752 | 1928 / 13,819 | 1809 / 13,767 | 1935 / 14,182 | 1935 / 14,189 |
| SLO 2 p99 in the last window of phase 3, ms | 1150.0 | 60,031 (overflow) | 60,031 | 60,031 | 1161.2 | 1294.3 |
| Active autovacuum workers, max. | 2 | 2 | 2 | 2 | 2 | 2 |

Across all runs in phase 1, 69–72% of samples had no active autovacuum worker, 27–30% had one, and fewer than 1% had two.

The `20261018-*` reports list “Repeated (excess) executions” as “no data” for `vacuum`, `ramp`, and `partitions`.
At the time, the harness counted them only in `sustained` (0 excess executions). It gained support for counting them in scenarios without a drain after these runs.

## Decisions

Each finding corresponds to an SLO or a report column. Local fixes cover table and index parameters, a single SQL statement without protocol changes,
an option default, or a documentation recommendation. Changes to a hot-path protocol, multiple statements, or a new behavioral option count as major
and go on the roadmap. Borderline cases count as major.

### 1. Immediate jobs took ≈ 95 ms to start at p50 (major)

**Metric.** SLO 2 p50. **Measurements.** In sustained direct and sustained bouncer, p50 was 96.6 ms (`direct`) and 93.4 ms (`bouncer`)
against a 50 ms threshold. It reached 97.5–105 ms for `P = 15, current` in all `partitions` modes. Full-run `ramp` p50 was 96.2 and 96.4 ms.

**Where jobs waited.** Ready jobs spent most of their time waiting in `queue`. Database execution and harness work were small.

- At the 2,000/s step, window p99 was 322–375 ms (`ramp` direct), similar to 10,000/s (278–487 ms in `sustained`). Whole-run `ramp`
  p50 was 96.2–96.4 ms. Latency barely changed with load, with no sign of saturation.
- The ready backlog averaged 1039 (`direct`) and 1038 (`bouncer`) over the measurement window. By Little's law, this is ≈ 0.1 s × rate,
  consistent with an average queue wait of ~0.1 s.
- In 14 of 56 (`direct`) and 15 of 57 (`bouncer`) ten-second samples, there were no leased jobs (`leased` = 0 in the `.csv`).
  The same samples averaged 1267 and 936 ready jobs (up to 2849). Both hosts were idle with work waiting.
- Database costs were low. Mean dequeue execution took 0.21 ms and enqueue p99 was 3–4 ms. Harness CPU was 9%.
  At most 1 backend waited on `LockManager`, with a mean of 0.00.
- At `P = 60`, dequeue planning cost more (1.1 ms versus 0.5 ms) but p50 fell to 62.8–69.5 ms.
  The timing of dispatch determined the delay.

The dispatcher code suggested a mechanism. With a producer in another process, hosts receive no wakeup signal for new jobs and rely on adaptive polling.
An empty fetch doubles the queue interval from `MinPollInterval` (50 ms) up to `MaxPollInterval` (10 s).
Freeing a slot schedules an immediate fetch but leaves the interval intact. A series of fetches after job completion that return 0 rows can raise it
to hundreds of milliseconds. Only a nonempty fetch or a wakeup signal resets it. Both hosts can then sleep through the full interval while jobs accumulate.
The 827 ms maximum delay (`direct`) fits an 800 ms interval plus 10% jitter. In October this mechanism was still a hypothesis. The dispatcher comparison below tested it.

**Major finding.** A reliable fix changes the worker's fetch protocol. An active queue could retain its polling interval when an early fetch triggered
by a freed slot returns empty, receive an interprocess wakeup (`LISTEN/NOTIFY`) from the producer, or use both.
Changing defaults qualifies as local but does not reliably resolve the finding. Lowering `MinPollInterval` still allows doubling. Lowering
`MaxPollInterval` (for example, from 10 s to 100 ms) limits wait time at the cost of a hundredfold increase in idle polls per queue per server.

**Roadmap item 7b-2.** “Delivery latency for immediate jobs enqueued from another process” lists the criteria for this finding. [Roadmap](superpowers/specs/2026-09-26-pgbolt-roadmap.md#7b-2-производительность-крупные-находки-стенда)

<a name="проверка-опроса-активной-очереди-20-ноября-2026"></a>

#### Active queue polling verification, 20 November 2026

The fix preserves the scheduled poll deadline when an early fetch triggered by a freed slot returns empty. The original and fixed dispatchers ran on the same harness.
Both Release builds were ready before measurement. On each build, `sustained` ran sequentially over direct connections and PgBouncer
with 60 s of warmup and 600 s of measurement at 10,000 jobs/s. Load metrics come from one run per combination.
The last two columns come from one probe job per combination after draining the queue.

| Version and connection | p50, ms | p99, ms | p99.9, ms | Min. throughput, % | Total dequeues | Empty dequeues | Empty dequeues per idle minute | First start after idle, s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Original, direct | 90.4 | 414.2 | 610.8 | 98.4 | 441,933 | 14,741 | 24 | 3.09 |
| Original, bouncer | 122.9 | 58,851.3 | ≥ 60,000 | 64.3 | 410,339 | 8,213 | 25 | 2.95 |
| Fixed, direct | 37.4 | 216.1 | 773.6 | 99.9 | 466,982 | 39,795 | 27 | 7.98 |
| Fixed, bouncer | 37.8 | 728.6 | 7,295.0 | 81.0 | 455,884 | 37,975 | 25 | 0.81 |

**Result.** The fixed version met SLOs 1–6 in direct mode. Via PgBouncer, the median met the 50 ms threshold but SLOs 1, 2, and 4 failed.
Execution fell below 95% of the target rate in 3 of 60 ten-second windows, reaching 81.0% at its lowest. The producer supplied the requested load
in every window. Start p99 was 728.6 ms against 500 ms, and p99.9 was 7.295 s against 2 s. Ready jobs reached the measurement cap of 10,001,
above the allowed 10,000. Both fixed configurations passed execution reconciliation and connection checks. Acceptance of the first 7b-2 finding remained open at this point.

**Cost.** Total dequeue calls in the measurement window rose by 5.7% directly and 11.1% via PgBouncer. Empty fetch counts grew by factors of 2.70 and 4.62.
In the original PgBouncer run, the producer undersupplied two windows and the latency histogram reached its 60 s ceiling. More measurements are needed
to quantify the tail latency improvement in this mode.

**When throughput fell.** The three below-threshold windows in the fixed PgBouncer run started 460, 640, and 650 s into the test.
At about 466 and 646 s, ready jobs had reached the measurement cap, `queue` had about 1.6 million dead tuples, and autovacuum was active.
The samples establish timing. The cause of the dips remains unknown.

With the fixed version and `autovacuum_naptime=10s` via PgBouncer, all 120 ten-second throughput windows passed across two additional runs.
The minimum was 9,810 and 9,954 starts/s versus 9,471 in the 60 s control. In the first 10 s run, connection wait once reached 374 ms against
a 100 ms threshold. The repeat and the direct connection passed SLOs 1–6. Peak dead tuples in `queue` fell from 1.28 million to 0.22 million.
Queue size including indexes fell from 147 to 38–41 MiB. This setting applies to the entire PostgreSQL instance.

### 2. The P threshold for `current` is ≤ 60 because dequeue plans all `jobs` partitions (major)

**Metrics.** The P threshold (SLOs 1 and 2 across `partitions` combinations) and dequeue `mean_plan_time`. In the `partitions` results, the overall
threshold was 15 (SLO 2 p50). For SLO 1 alone it was 60 in three of four modes. Both values are ≤ 60.

**Evidence.** Dequeue (`JOIN jobs j ON j.id = l.job_id`) has no predicate on the partition key. Every call plans all P partitions and locks them
and their indexes, for 5P + 25 relations (100 → 2025). Planning time scales linearly with P (0.5 → 11.4 ms), while execution remains within
1 ms (0.21 → 1.0 ms). Each host's dispatcher calls dequeue serially, so calls/s fall (813 → 114) and throughput tops out at ~4.2k/s at
P = 200 and ~1.9k/s at P = 400. Few backends waited on `LockManager` (≤ 2, mean ≤ 0.05). The cost lies in planning and acquiring locks on
every call. Prepared statements did not help. Under load, PostgreSQL replanned 57–100% of dequeue calls (ratio of `total_plan_time / calls`
to `mean_plan_time`: 97–100% at P = 60 and 400, 57–76% at P = 200, 71–86% at P = 15). The report's `generic` plan came from a harness
probe before load. The hosts mostly used custom plans and achieved similar throughput (~4.3k/s at P = 200). Ack and enqueue prune partitions
at plan time and are independent of P.

At P = 60, average throughput was sufficient (661 calls/s). Brief dips breached SLO 1 when `queue`'s `n_dead_tup` reached 1.0–1.25 million
before autovacuum arrived in direct, direct-prepared, and bouncer-prepared modes. At P = 15, similar growth did not breach SLO 1.
More partitions leave less headroom. The `vacuum` scenario evaluates autovacuum on `queue`.

**Major finding** (P threshold ≤ 60 in at least one mode). Until the fix, keep the default `jobs` partition count
(P = 15, from 7 days of `JobsRetention` + today + 7 days of `PartitionDaysAhead`). Avoid increasing retention or the future horizon enough for P to approach 60.

**Roadmap item 7b-2.** “Pruning `jobs` partitions during dequeue” lists the criteria for this finding. [Roadmap](superpowers/specs/2026-09-26-pgbolt-roadmap.md#7b-2-производительность-крупные-находки-стенда)

**Extra indexes.** The `extra` btree `(type, id)` and GIN `(args jsonb_path_ops)` indexes inform the opt-in index decision only.
Their SLO 1 threshold was P = 60 in all four modes (versus 200 with `current` in `bouncer`). At P = 15, dequeue took 132 locks versus 100,
ack execution took 1.47 ms versus 1.21 ms (+22%), and enqueue took 0.515 ms versus 0.404 ms (+27%). As P grows, dequeue locks 7P + 27
relations. Mean planning time reached 14.9 ms at P = 400. For `bouncer-prepared, extra` at P = 15, SLO 2 p99 reached 719.9 ms against
500 ms. This also informs the opt-in decision and prompted no separate finding or fix.

#### Partition pruning verification on PostgreSQL 17.11, 20 November 2026

Control build `ae68c53`. In local Docker, dequeue fetched 32 real UUIDv7 values spanning one or two days with 15 and 400 partitions and
`current` indexes. Each path ran with forced custom and generic plans. After the first execution, the test rolled back the transaction and
repeated the same prepared query. It counted child tables of `jobs` and their indexes in `pg_locks` before rollback.

| Path, two-day batch | Tables opened at P=15, custom / generic | At P=400, custom / generic | Relations including indexes at P=400, repeat | Outer query planning time at P=400, repeat, ms |
|---|---|---|---|---|
| Original dequeue | 15 / 15 | 400 / 400 | 2000 / 400 | 8.307 / 0.059 |
| Claim only, standard queue | 0 / 0 | 0 / 0 | 0 / 0 | 0.117 / 0.009 |
| Separate read query with an ID array | 2 / 15 | 2 / 400 | 10 / 400 | 0.120 / 0.075 |
| Dequeue using server-side function | 2 / 2 | 2 / 2 | 10 / 10 | 0.137 / 0.014 |
| Limited dequeue using the same function | 2 / 2 | 2 / 2 | 10 / 10 | 0.249 / 0.018 |

For a one-day batch, the function opened one partition at both P values. In migration V003, the function receives the leased batch's UUIDs
and plans the inner SELECT for those values. An empty dequeue skips the read. The table reports planning time for the outer prototype query.
End-to-end dequeue p99, the work of the inner SELECT, and throughput require paired load tests.

The final SQL declares the function `VOLATILE`. After a concurrent partition replacement, the inner SELECT takes a fresh snapshot and sees the moved rows.
A regression test starts dequeue while the replacement is still uncommitted. With `STABLE`, all four combinations incorrectly reported a missing job.
With `VOLATILE`, they returned it directly and via PgBouncer. A recheck of the final SQL at 15 and 400 partitions opened exactly 1 and 2 child tables
for batches spanning the corresponding number of days, on initial preparation and repeat in both plan modes.

A local trace counted PostgreSQL `ReadyForQuery` and `CommandComplete` responses on an established connection. Without preparation, standard and
limited dequeue each took one round trip. The server executed one and two commands respectively. With explicit preparation, the first call took
two round trips and the next two calls took one each. Counts matched for control and candidate, directly and via PgBouncer.

Two repeats of 200 uncontended dequeues on the candidate averaged 1.07–1.21 µs of server execution for the normal advisory lock acquisition command,
and 2.26–2.80 µs with two timestamps. This calibrates the diagnostic command. A separate histogram measures wait time with two active hosts.

A report check on stock PostgreSQL used 15 partitions, two hosts, PgBouncer, and a limit of 32 for 20 seconds at 500 enqueues/s.
The harness recorded 2,117 dequeues, of which 767 returned jobs and 1,350 were empty. It recorded 2,117 batch sends and advisory lock measurements
and 767 inner SELECT calls. When nested query accounting was disabled, the harness explicitly reported the missing `read_jobs` and exited with code 2.

Two builds with identical diagnostics were prepared for paired runs. Control `1ba3cfc`, based on `ae68c53`, uses the original join to `jobs`.
Candidate `c3b4611` uses V003. Run the commands from the root of each working copy, with a separate results directory for each.
[Mode matrix and criteria](superpowers/plans/2026-11-20-pgbolt-stock-postgres-and-dequeue.md)

```sh
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- partitions --mode all --partitions 15,60,200,400 --indexes current --pg-profile stock
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- partitions --mode bouncer --partitions 15,200 --indexes current --pg-profile stock --queue-profile limited
```

### 3. Tail latency and the final `ramp` step coincided with dead tuple growth in `queue` (input to `vacuum`)

**Metrics.** SLO 2 p99.9 (`sustained bouncer`) and SLO 2 p99 across `ramp` steps. **Measurements.** In `sustained bouncer`, p99.9 was 1950.7 ms
against a 2 s threshold, with a maximum of 3905 ms. The dip in the 540–560 s windows (p99 of 1913 and 3553 ms, backlog up to 7227) coincided with
`queue`'s `n_dead_tup` reaching 1.96 million while the only autovacuum worker processed the `jobs_20261018` partition.
After vacuum on `queue` (551 s), dead tuples fell to 18k and latency returned to normal. In the stopping step of `ramp` `direct` at 14,000/s
(380–390 s windows), `queue`'s `n_dead_tup` reached 1.6 million before autovacuum arrived. In `bouncer` at 12,000/s, the 330 s window saw
1.4–1.5 million dead tuples and autovacuum on a `jobs` partition. The 350 s window (p99 603 ms) had 0.3–0.5 million, consistent with the tail
from finding 1 at 12,000/s. The same pattern appears in the SLO 1 dips at P = 60 (finding 2).

**Classification.** Profile B met the relevant SLO. The `sustained bouncer` p99.9 stayed within the threshold. The stopping step is a measurement
for `ramp`, and `ramp` has no acceptance threshold. The contribution of dead tuples in `queue` informs the choice of `queue` reloptions and global autovacuum
settings in `vacuum`. Over 11 minutes of `sustained`, autovacuum made 12 passes on `queue`. Dead tuples rose to 1.3–2.0 million between
passes (the `queue` threshold is 5000 dead tuples).

### 4. SLO 2 p99 reached 526.3 ms for `P = 15, current` in `direct-prepared` (major, part of finding 1)

**Metric.** SLO 2 p99 in `partitions`, `direct-prepared`, P = 15, `current`. **Measurements.** p50 was 105.0 ms, p99 was 526.3 ms versus
the 500 ms threshold, and p99.9 was 1016.3 ms. Under the same conditions, p99 was 398.1 ms in `direct` and 412.9 ms in `bouncer-prepared`.
SLO 1 passed (minimum 99.0% of the target rate).

**Evidence.** Prepared statements did not explain the result. Dequeue execution averaged 0.211 ms versus 0.208 ms in `direct`, with no
`LockManager` waits. The distribution matches finding 1. The ready backlog averaged 1303, with no jobs leased in 3 of 11 ten-second samples.
Five of 12 windows exceeded 500 ms at p99 (50, 80, 100, 110, and 140 s, at 517–844 ms). In the 80, 100, and 110 s windows, backlog
peaked at 3957–5015, with `queue`'s `n_dead_tup` at 0.41–1.24 million and autovacuum idle. The intervening 90 s window reached 3638 at
p99 of 435 ms. The 50 s window reached 3231 with 0.99–1.19 million dead tuples. This matches finding 3. That pattern does not explain the
140 s window (669.7 ms), with backlog up to 2058 and 0.40–0.60 million dead tuples. The 5% threshold overshoot came from one run, so its variation is unknown.

**Major, part of finding 1.** The tail informs the `vacuum` scenario (finding 3). There is no separate roadmap item.
Reopen the finding if p99 for `P = 15, current` still exceeds 500 ms in any mode after fixing findings 1 and 3.

### 5. No `queue` autovacuum configuration met all of SLOs 7–9 (major)

**Metrics.** SLOs 7–9 in `vacuum`, peak `queue` size, and SLO 2 p99 in phase 1. See the `vacuum` results above.
The reports' “Autovacuum parameter selection inputs” row supplies the following values.

| Configuration | Mode | SLO 7–9 | Peak phase 1 `queue` size, MB | Phase 1 SLO 2 p99, ms |
|---|---|---|---|---|
| current | direct | no | 147.5 | 412.2 |
| current | bouncer | no | 258.8 | 18,743.3 |
| `cost_delay=0` | direct | no | 181.5 | 811.5 |
| `cost_delay=0` | bouncer | no | 261.6 | 24,592.4 |
| `naptime=10s` | direct | no | 44.7 | 753.2 |
| `naptime=10s` | bouncer | no | 56.3 | 611.8 |

**Per-table settings.** `VacuumWinner.Choose` compares the current settings with `cost_delay=0`. Neither met SLOs 7–9 in `bouncer`, so
the rule retained the current reloptions without comparing size or p99. For reference, `cost_delay=0` did no better on either measure.
Peak size in `bouncer` was 261.6 versus 258.8 MB (a tie under the rule), and p99 was 24,592.4 versus 18,743.3 ms (24% worse).
The per-table choice was the current reloptions in `V001__initial.sql`.

**Global settings.** The global test ran because `cost_delay=0` failed to meet SLOs 7–9 in either mode.
`VacuumWinner.ChooseGlobal` compares the current settings with `naptime=10s`. Neither met SLOs 7–9 in `bouncer`, so the per-table choice
stood. The measurements did not justify recommending `autovacuum_naptime = 10s`. Several observations warranted a recheck. At `naptime=10s`,
phase 1 `queue` size in SLO 7 samples was 38.7–46.1 MB versus 141.9–247.6 MB with current settings. The pronounced phase 1 backlog rise in
`bouncer` disappeared (1336 → 7012 with current settings and 1221 → 1471 at `naptime=10s`, with above-rate samples falling from 147 → 0).
Peak phase 1 `n_dead_tup` in `bouncer` fell from 2.41 → 0.42 million and phase 1 SLO 2 p99 in `bouncer` from 18,743.3 → 611.8 ms.
After the xmin hold, end-of-phase-3 window p99 was 1.2–1.3 s versus histogram overflow (60 s) for current settings in `bouncer`.
Final `queue` size was 385–405 MB versus 498–893 MB. In `direct`, phase 1 p99 with `naptime=10s` was worse than current settings
(753.2 versus 412.2 ms), but `cost_delay=0` reached 811.5 ms, showing substantial variation across single `direct` runs.
Recheck both modes. If current settings meet SLOs 7–9 in `direct`, the selection rule retains them given this discrepancy.

**`autovacuum_max_workers`.** Keep the default (3). Across six runs, there were at most 2 active autovacuum workers, with two active in fewer than
1% of phase 1 samples. The limit was never reached. Only one worker processes a table at a time, so adding workers cannot speed up a pass over `queue`.

**Major finding.** Neither the per-table configuration nor the global candidate met SLOs 7–9, so the finding went on the roadmap.
Both p50 (a shared factor, finding 1) and tail latency contributed to SLO 8 failures. SLO 2 p99 exceeded 500 ms in 41–72 phase 1 windows
in five of six runs. In `bouncer`, current settings and `cost_delay=0` had 147 and 182 samples with backlog above the rate, and
`naptime=10s` had 0. The throughput drop while xmin was held breached SLO 9. Autovacuum settings cannot resolve it.
Recheck after fixing finding 1, evaluating p99 tail latency and backlog separately.

**Roadmap item 7b-2.** “Autovacuum on `queue` under profile B” lists the criteria for this finding. [Roadmap](superpowers/specs/2026-09-26-pgbolt-roadmap.md#7b-2-производительность-крупные-находки-стенда)

### Local fixes

The runs produced no local fixes. Findings 1, 2, and 5 were major roadmap items. Finding 4 belongs to finding 1. Finding 3 informed the
`vacuum` scenario that produced finding 5. None called for a separate patch. The selection rule retained the existing `queue` reloptions
in `V001__initial.sql`.

The harness found one product issue before the full runs. `PgBoltDataSourceFactory` now always adds `No Reset On Close=true` to
`ConnectionString` (commit `e2e53ba`). Previously in `bouncer-prepared`, returning a connection to the pool ran `DISCARD ALL` and removed
prepared statements from the server while PgBouncer still tracked them. The host then failed on startup with `prepared statement "…" does not exist`.

## Recommendations

These figures apply to profile B (10,000/s) on the environment above. Run the harness for your own hardware and workload.

**Retention and partition count.** The number of attached `jobs` partitions P is `JobsRetention` in days + today + `PartitionDaysAhead`,
or 15 by default. Days pinned by live jobs remain in P until those jobs leave. Before the partition-pruning fix, each partition added to
dequeue planning time (finding 2). At P = 60, three of four modes failed SLO 1. At P = 200, throughput was ~4.2k/s. On builds without
dequeue partition pruning, keep P near the default and avoid raising `JobsRetention` and `PartitionDaysAhead` enough to approach 60.

**`max_locks_per_transaction`.** The default of 64 was sufficient. Lock table capacity was 7808. Even at P = 400 (2025 dequeue locks,
1999 outside the fast path), `storage.errors` = 0, with at most 2 backends waiting on `LockManager` and a mean ≤ 0.05.
Dequeue planning time became the bottleneck before the lock table filled.

**`autovacuum_naptime`.** Keep the 1-minute default. No configuration met SLOs 7–9, so the selection rule rejected `10s` (finding 5).
Of the tested settings, this had the largest effect and warranted a roadmap recheck. In `bouncer`, peak phase 1 `queue` size was 56.3 MB
versus 258.8 MB, and 0 versus 147 samples had backlog above the rate. In `direct`, phase 1 p99 was worse (753.2 versus 412.2 ms).
Per-table `autovacuum_vacuum_cost_delay = 0` on `queue` provided no improvement over the current reloptions.

**`autovacuum_max_workers`.** Keep the default of 3. At most 2 workers were active, and fewer than 1% of samples had two.
One worker handles each table, so extra workers cannot accelerate a pass over `queue`.

**Long transactions.** A transaction holding xmin (reporting, analytics, or a lagging replica with `hot_standby_feedback`) prevents cleanup of
`queue`. During a 5-minute hold, throughput fell to ~1k/s (average 1.8–1.9k/s), and `queue` grew to 555–585 MB. It kept growing in recovery
(peak 755–893 MB). In 3 of 6 runs, autovacuum reduced its size to 385–498 MB by the end of phase 3. The remaining runs ended at their peak.
For alerts on `pgbolt.cluster.table.*`, see [Table statistics](observability.md#table-statistics).

**PgBouncer.** Use `pool_mode=transaction`. Statement mode is unsupported. With two 32-worker hosts and a producer, `default_pool_size=40`
left ample capacity. In `sustained bouncer`, active server connections averaged 1.6 and peaked at 10, with `cl_waiting` 0 and `maxwait` 0 ms.
Only `UsePreparedStatements` needs `max_prepared_statements > 0` (at 0 it produces `prepared statement "…" does not exist`). The harness uses 200.

**`UsePreparedStatements`.** The results give no performance reason to enable it. At P = 15, mean dequeue execution was 0.211 ms versus 0.208 ms.
SLO 2 p99 was 526.3 ms in `direct-prepared` and 412.9 ms in `bouncer-prepared` versus 398.1 and 386.6 ms without preparation. Under load,
PostgreSQL still replanned 71–86% of dequeue calls. Throughput remained similar at large P.

**Npgsql pools.** `Maximum Pool Size=20` was enough for a 32-worker host, and 8 for the producer. At 10,000/s in `direct`, the two hosts and
producer used at most 17–18 connections (`sustained` and `vacuum`). With direct connections, the sum of `Maximum Pool Size` across all pods
must fit within `max_connections` after subtracting `superuser_reserved_connections` and connections for other clients.

<a name="штатный-postgresql-в-профиле-b"></a>

## Stock PostgreSQL under profile B

Local Docker Desktop 29.8.0 ran with 6 CPUs and 4.8 GiB of memory. PostgreSQL 17.11 used `shared_buffers=128MB`, `max_wal_size=1GB`,
and `autovacuum_naptime=1min`, with write durability enabled. Profile B specifies 10,000 enqueues/s, two hosts with 32 workers each, and
15 partitions. PgBouncer 1.24.1 used transaction pooling with a pool of 40. Runs were sequential. Reports, CSVs, PostgreSQL snapshots,
and build hashes are in the local directory `benchmarks/PgBolt.LoadTest/results/stock-acceptance-20261120201934`.

<a name="десять-минут-нагрузки-после-минуты-прогрева"></a>

### Ten minutes of load after a one-minute warmup

| Build | Mode | SLO 1–6 | Minimum starts/s in a 10 s window | Start p50 / p99 / p99.9, ms | Peak ready jobs | SLO 6 |
|---|---|---|---:|---|---:|---|
| Control `1ba3cfc` | direct | passed | 10,003 | 36.6 / 183.6 / 477.4 | 3,753 | 17 connections |
| Control `1ba3cfc` | bouncer | passed | 9,988 | 36.4 / 169.6 / 415.7 | 2,742 | maxwait 0 ms |
| V003 `7a2e4a0` | direct | passed | 9,987 | 35.0 / 146.9 / 344.3 | 2,678 | 18 connections |
| V003 and promoter fix `2847b2f` | direct | passed | 9,992 | 34.8 / 137.7 / 298.2 | 2,012 | 18 connections |
| V003 `7a2e4a0` | bouncer | passed | 9,989 | 35.2 / 152.8 / 340.5 | 2,829 | maxwait 0 ms |
| V003 and promoter fix `2847b2f` | bouncer | passed | 9,992 | 34.9 / 141.8 / 302.1 | 2,331 | maxwait 0 ms |
| Final `ead32c0` | direct | passed | 10,016 | 35.0 / 142.1 / 295.7 | 1,693 | 18 connections |
| Final `ead32c0` | bouncer | passed | 9,951 | 35.2 / 155.8 / 380.7 | 7,297 | maxwait 0 ms |

All eight runs averaged 10,000 enqueues/s. Reconciliation after draining found an empty queue, zero missing jobs, zero excess executions,
and zero incorrect attempt numbers. The initial PgBouncer control warmup was interrupted because it overlapped harness tests.
The table uses a full repeat on fresh containers.

**Promoter.** Build `7a2e4a0` returned one queue name to the client for every job promoted. In the last statistics samples within the
measurement window, that meant 462.7 rows per call directly and 465.1 via PgBouncer for two queues. In `2847b2f`, the SQL returns one
row with the exact job count and distinct queue names. Storage reads another batch whenever the previous one was full.
The server sends wakeup signals based on the result of one storage call.

Over ten minutes, promoter queries went from 1,426 to 1,314 directly and from 1,420 to 1,278 via PgBouncer. Total server time for the
query went from 4.63 to 4.26 s and from 4.53 to 4.45 s, respectively. In both modes, the response contains one row per batch.
Each pair had one full run per build, so the p99 differences alone cannot establish a start latency improvement.

<a name="партиции-с-обычными-очередями"></a>

### Partitions with standard queues

In each cell, control `1ba3cfc` appears on the left and V003 `7a2e4a0` on the right. Each combination had a 30 s warmup and 120 s
measurement. Throughput includes initial starts and retries. Start latency covers immediate jobs. The client measured dequeue p99.
Planning time per call is the combined planning time for the outer query and nested `read_jobs`, divided by the number of dequeue calls.

| Mode | P | Starts/s | Start p99, ms | Dequeue p99, ms | Planning time per call, ms | SLOs met |
|---|---:|---:|---:|---:|---:|---|
| direct | 15 | 10095 / 10094 | 151.7 / 120.8 | 2.689 / 2.493 | 0.449 / 0.316 | 1, 2 / 1, 2 |
| direct | 60 | 10091 / 10097 | 3723.3 / 123.8 | 4.299 / 2.529 | 1.038 / 0.263 | 1 / 1, 2 |
| direct | 200 | 4389 / 10102 | 60031.0 / 123.3 | 9.719 / 2.483 | 4.179 / 0.327 | — / 1, 2 |
| direct | 400 | 2017 / 10101 | 60031.0 / 121.0 | 56.959 / 2.327 | 9.809 / 0.249 | — / 1, 2 |
| bouncer | 15 | 10090 / 10099 | 127.4 / 124.7 | 2.681 / 2.545 | 0.408 / 0.283 | 1, 2 / 1, 2 |
| bouncer | 60 | 10100 / 10097 | 249.2 / 139.0 | 4.179 / 2.453 | 0.951 / 0.237 | 1, 2 / 1, 2 |
| bouncer | 200 | 4484 / 10057 | 60031.0 / 142.3 | 9.607 / 2.617 | 4.095 / 0.277 | — / 1, 2 |
| bouncer | 400 | 1956 / 10104 | 60031.0 / 463.6 | 59.711 / 2.955 | 10.220 / 0.253 | — / 1, 2 |
| direct-prepared | 15 | 10099 / 10093 | 117.5 / 583.2 | 1.827 / 2.083 | 0.000 / 0.036 | 1, 2 / 1 |
| direct-prepared | 60 | 10098 / 10095 | 121.0 / 428.8 | 2.261 / 2.034 | 0.002 / 0.036 | 1, 2 / 1, 2 |
| direct-prepared | 200 | 4396 / 10095 | 60031.0 / 607.7 | 9.839 / 2.495 | 4.200 / 0.211 | — / 1 |
| direct-prepared | 400 | 2316 / 10103 | 60031.0 / 146.2 | 18.175 / 1.937 | 8.433 / 0.038 | — / 2 |
| bouncer-prepared | 15 | 10100 / 10008 | 481.0 / 3782.7 | 2.841 / 7.743 | 0.362 / 0.265 | 1, 2 / — |
| bouncer-prepared | 60 | 10095 / 10106 | 922.1 / 104.3 | 4.219 / 2.087 | 0.418 / 0.059 | 1 / 1, 2 |
| bouncer-prepared | 200 | 8764 / 10103 | 11632.6 / 110.6 | 7.675 / 2.283 | 0.472 / 0.247 | — / 1, 2 |
| bouncer-prepared | 400 | 1986 / 10092 | 60031.0 / 112.7 | 18.815 / 2.125 | 10.464 / 0.037 | — / 1, 2 |

For a fixed job set, the control's dequeue probe acquired 100, 325, 1,025, and 2,025 relation locks without preparation and 35, 125, 405, and 805
with preparation. At all four partition counts, V003 took 30 locks without preparation and 14 with it. Every cell in both matrices recorded
one send per dequeue call, zero dequeue errors, and zero storage errors. Harness process CPU stayed below its threshold in every combination.

At P=200 and 400, all four V003 modes averaged over 10,000 starts/s. In the original V003 matrix at P=15, `direct-prepared` exceeded the
p99 threshold. `bouncer-prepared` also failed SLO 1 with producer underload. At P=400 in `direct-prepared`, one underloaded window breached
SLO 1, and the lowest start rate across all windows was 96.5% of target. A latency of 60,031 ms denotes histogram overflow.

Both P=15 pairs with preparation ran again on fresh containers. Start p99 was 128.1 / 136.4 ms in `direct-prepared` and 130.4 / 112.4 ms
in `bouncer-prepared`. All four repeats met SLOs 1 and 2. The original matrix failures remain in the table. In the first
`bouncer-prepared` run with V003, the minimum throughput was 80.2% of target, with one window of producer underload.

The `direct-prepared` repeat delivered 4,518 / 10,096 starts/s at P=200 and 3,290 / 10,103 at P=400.
Both V003 combinations met SLOs 1 and 2 with start p99 of 102.6 and 104.4 ms. The control again failed both SLOs in both combinations.
The dequeue improvement at large P justifies retaining V003. The first pass and repeats show per-window variation with prepared statements.

<a name="очередь-с-кластерным-лимитом-32"></a>

### Queue with a cluster-wide limit of 32

Two hosts share one queue via PgBouncer. The table shows mean starts/s, client-side dequeue p99, and server-side advisory lock acquisition p99.
The change from `1ba3cfc` to `7a2e4a0` affects how job definitions are read. The change from `2847b2f` to `3fb9350` affects ack delivery.

| Build | P=15, starts/s | P=200, starts/s | Dequeue p99 at P=15 / 200, ms | Lock p99 at P=15 / 200, µs |
|---|---:|---:|---:|---:|
| Control `1ba3cfc` | 1,286 | 991 | 3.639 / 82.047 | 4 / 3 |
| V003 `7a2e4a0` | 1,300 | 1,210 | 3.167 / 71.103 | 4 / 5 |
| With promoter fix `2847b2f` | 1,302 | 1,214 | 3.163 / 68.607 | 5 / 5 |
| With early ACK `3fb9350` | 8,124 | 6,814 | 2.467 / 17.423 | 3 / 3 |
| Repeat `2847b2f` | 1,302 | 1,212 | 3.187 / 69.503 | 4 / 4 |
| Final ACK `ead32c0` | 8,006 | 6,812 | 2.741 / 16.991 | 3 / 3 |

A job counts against the limit until its outcome commits. With a limit of 32, the default ack batch (100) never filled and waited for its 20 ms timer.
At P=15, build `2847b2f` issued 4,835 ACK queries in 120 s. In `3fb9350`, the server sends the batch early when the cluster limit is exhausted
and the local host has no more work in that queue. In the same 120 s it issued 45,685 ACK queries. The measured cost of higher throughput was
more short transactions. At both P values, input was about 10,000 jobs/s and the queue kept growing.

In `ead32c0`, the server also checks the buffer when it learns the limit is exhausted after buffering an ACK, or the last local job finishes
with cancellation. Tests with a stopped clock confirm capacity becomes available before the timer fires and another queue keeps its normal batch.
In the repeat pair, throughput rose by factors of 6.1 at P=15 and 5.6 at P=200. Every build in both pairs recorded zero storage errors and
zero excess executions. SLOs 1 and 2 remain open for the limited queue.

<a name="очистка-очереди-и-восстановление-после-длинной-транзакции"></a>

### Queue cleanup and recovery after a long transaction

Each run used a fresh database. After two minutes of warmup, the harness applied normal load for 20 minutes, held a `REPEATABLE READ`
snapshot for five minutes, and continued enqueuing for ten minutes after releasing the snapshot. At the end of the second phase, it verified
the connection inside the same transaction. All pairs used the same workload profile, indexes, and write durability guarantees. Tuning changed
only `autovacuum_naptime`, from 1 minute to 10 seconds.

For SLO 6, direct runs show peak connections against a limit of 48. PgBouncer runs show peak connection wait in ms against a limit of 100 ms.
Peak size, start p99, and the count of windows that failed SLOs 1–3 refer to the first phase. “With xmin held” is mean starts/s in the second phase.
T measures time to sustained recovery after snapshot release, with a threshold of 300 s. A dash means no qualifying window appeared before measurement ended.

| Build | Mode | Setting | SLO 6 | Peak queue size, MiB | p99, ms | Failed windows out of 120 | With xmin held, starts/s | T | Excess executions |
|---|---|---|---:|---:|---:|---:|---:|---|---:|
| `1ba3cfc` | direct | stock | 18 | 141.8 | 417.8 | 22 | 2,022 | — | 0 |
| `1ba3cfc` | direct | repeat | 18 | 147.0 | 739.8 | 60 | 2,023 | — | 3 |
| `1ba3cfc` | direct | 10 s | 18 | 44.0 | 859.1 | 76 | 2,042 | — | 0 |
| `7a2e4a0` | direct | stock | 17 | 146.6 | 786.4 | 61 | 2,044 | 590 s | 8 |
| `ead32c0` | direct | stock | 20 | 146.0 | 827.9 | 61 | 2,037 | — | 2 |
| `ead32c0` | direct | repeat | 19 | 150.1 | 763.4 | 74 | 2,046 | — | 0 |
| `ead32c0` | direct | 10 s | 18 | 42.7 | 254.7 | 12 | 2,014 | — | 0 |
| `1ba3cfc` | bouncer | stock | 0.0 | 152.4 | 916.0 | 86 | 1,979 | — | 16 |
| `1ba3cfc` | bouncer | repeat | 0.0 | 148.2 | 818.7 | 78 | 1,968 | — | 0 |
| `7a2e4a0` | bouncer | stock | 215.3 | 152.7 | 775.7 | 51 | 2,006 | — | 0 |
| `ead32c0` | bouncer | stock | 0.0 | 156.6 | 880.6 | 88 | 2,010 | — | 0 |
| `ead32c0` | bouncer | repeat | 0.0 | 149.9 | 823.3 | 75 | 2,022 | — | 0 |
| `ead32c0` | bouncer | 10 s | 0.0 | 44.6 | 716.3 | 63 | 2,036 | — | 0 |

All 13 completed runs passed SLO 7 and failed SLOs 8 and 9. SLO 6 passed in 12 runs. The only excessive connection wait was 215.3 ms on the
intermediate V003 build. With T = 590 s, the remaining ten seconds were insufficient to evaluate post-recovery backlog growth.
Storage errors were zero across all 13 runs. The first stock direct run of the final build recorded two excess executions, and the other five completed
runs on that build recorded zero. The collected data did not establish the cause of the two executions.

**Stock settings.** On the final build, each of the two runs per mode averaged 10,075–10,100 starts/s before the xmin hold.
Of 120 windows, 60 and 74 exceeded the 500 ms start p99 threshold directly, and 85 and 72 via PgBouncer. Dead tuples in `queue` peaked at
1.29–1.32 million in this phase. All four runs also had producer underload in three to six windows. In the first direct run, the slowest
enqueue interval delivered 7,567 jobs/s. In the first PgBouncer run, five one-second samples exceeded the 10,000 ready-job limit with zero
connection wait. The harness caps the ready-job count at 10,001.

**Five minutes with an open snapshot.** In the final build's first stock direct run, mean server-side dequeue time rose from 0.25 to 25.84 ms
per call. A `queue_fetch` scan read an average of 103 index entries before the hold and about 525k near the end of the hold.
Via PgBouncer, mean dequeue time rose from 0.25 to 26.52 ms. These figures use differences between cumulative PostgreSQL counters in snapshots
taken 90 s apart within the last two minutes of each phase. After release, mean throughput in the third phase reached 13,940–14,128 starts/s in the final
build's four stock runs. Some windows still exceeded the p99 threshold through the end of the phase. All four reports showed “no data”
for backlog growth after T because none reached T.

**More frequent cleanup.** On `ead32c0`, `10s` raised autovacuum passes over the queue from 37 to 222 per run in both modes.
Before the long transaction, peak dead tuples fell to 249k directly and 241k via PgBouncer. During the xmin hold, both modes still delivered
about 2,000 starts/s. The global setting reduced queue size and the number of failing windows before the hold, but both modes failed SLOs 8 and 9.
The final stock runs recorded 64.8–67.5 GB of WAL and 118–123 requested checkpoints. Tuned runs recorded 62.5–65.0 GB and 114–118.
These counters cover the full run and cannot establish the cause of a particular slow window.

**Profile choice.** Stock PostgreSQL suffices for a ten-minute `sustained` run with standard queues. For full profile B acceptance, including
twenty-minute stability and recovery, SLOs 8 and 9 remain open under both configurations. The results give no basis for changing global PostgreSQL
defaults. After both tuned `vacuum` runs failed, the recommendation did not require two `sustained` runs on the tuned instance.
The `vacuum` scenario ends without a drain. The table counts observed excess executions. A separate `sustained` run fully reconciled losses and attempt counts.

The series stopped on 21 November at the user's request. The `1ba3cfc` control with `10s` via PgBouncer yielded partial recovery-phase data
and no final SLO results. Repeat `vacuum` runs on intermediate build `7a2e4a0` were skipped, leaving its initial SLO 6 breach unverified by a repeat.
All six `vacuum` runs on the final build completed.
