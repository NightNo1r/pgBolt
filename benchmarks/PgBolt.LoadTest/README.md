# PgBolt.LoadTest

The PgBolt load harness measures whether PgBolt can sustain 10,000 enqueues/s on PostgreSQL 17, directly and through PgBouncer. It also measures when additional `jobs` partitions slow down hot queries and how autovacuum handles churn in `queue`. Each run produces a Markdown report with SLO verdicts and a CSV with one-second samples.

The harness is a single-process .NET 10 console application with a producer, two PgBolt hosts (`AddPgBolt` + `AddPgBoltServer`, 32 workers each) and a sampler. For BenchmarkDotNet microbenchmarks of individual runtime operations, see `benchmarks/PgBolt.Benchmarks` and `benchmarks/PgBolt.Server.Benchmarks`.

## Requirements

- The .NET SDK specified in `global.json`.
- Docker. Without `--connection`, the harness starts `postgres:17-alpine` and `edoburu/pgbouncer:v1.24.1-p1` on a shared network and removes them at the end. It enables `fsync`, `full_page_writes` and `synchronous_commit`, which the Testcontainers builder disables by default.
- At least 30 GB free in the Docker disk image. A full `vacuum` run generates about 22 million `jobs` rows (10–15 GB of data) plus WAL up to `max_wal_size` (4 GB). Each mode drops its schema when finished.
- Enough CPU to run the full suite. If the harness process uses over 80% of available cores in more than 10% of measurement windows, it can exit with code 2 (see **Exit codes**). Disk virtualization in Docker for Mac can become the bottleneck before PgBolt. The report records the environment. Use `--connection` to repeat a run on a dedicated PostgreSQL instance.

## Run

From the repository root, run

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- <sustained|ramp|partitions|vacuum> [flags]
```

Pass the scenario first, then flag/value pairs. Omitted flags use the defaults listed below. Ctrl+C stops the run, writes a partial report and returns exit code 2.

## Load profile B

All scenarios use the same job profile.

- The target is 10,000 enqueues/s (`--rate`). The `default` queue receives 80% of jobs and `critical` receives 20%. Both hosts serve both queues.
- 90% of jobs run immediately. The other 10% have a delay of 1–30 whole seconds.
- 1% of jobs fail on the first attempt and retry under the default policy (15 s ± 20%).
- Each job does 1 ms of work and carries about 200 bytes of arguments.

The producer calls `EnqueueManyAsync` when a batch reaches 100 jobs or its oldest item reaches 10 ms. At 10,000 jobs/s this gives about 400 calls/s.

## Scenarios

The first command in each example runs the full scenario with defaults. The second checks the harness in a few minutes and writes to `/tmp/pgbolt-load`. Substitute your own output directory if needed.

### sustained

Profile B runs at a steady rate. After a 1-minute warmup, the harness measures 10 minutes, allows up to 2 minutes to drain the queue, then checks correctness and evaluates SLOs 1–6.

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- sustained
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- sustained --mode both --rate 500 --warmup 10s --duration 20s --out /tmp/pgbolt-load
```

By default (`both`), `direct` and `bouncer` run sequentially on the same containers. Each produces a separate report.

To use PostgreSQL's default memory and WAL settings, run `sustained --mode direct --pg-profile stock`. For a paired comparison through PgBouncer, run `sustained --mode bouncer --pg-profile stock`, then repeat with `--pg-setting autovacuum_naptime=10s`. Each invocation starts a fresh container. Use `--pg-profile bench` or omit the flag to reproduce the original profile.

### ramp

Find the maximum sustained rate. Start at `--ramp-start` and increase by `--ramp-step` every `--ramp-step-duration` until `--ramp-max` or the first violation. There is no warmup or drain. Evaluation uses 10-second windows starting at or after the first third of each step (the last 40 s of a 60 s step). Each window must pass SLO 1, including the producer underload check. SLO 2 uses p99 across those windows. The report gives the maximum sustained rate and the first limit reached: SLO 1, SLO 2, `enqueue` (underload), or `harness` (CPU saturation, reported as `harness-limited`).

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- ramp
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- ramp --mode direct --ramp-start 500 --ramp-step 500 --ramp-step-duration 60s --ramp-max 1000 --out /tmp/pgbolt-load
```

`--ramp-start` must be at most `--ramp-max`. Steps must last at least 30 s.

### partitions

This matrix varies connection mode, index set and number of `jobs` partitions (P). It measures the P at which dequeue, ack and enqueue slow down and how prepared statements affect them. Each combination uses a fresh schema. The harness measures statement locks before applying load, warms up for 30 s and measures profile B for 2 minutes without a drain. The default matrix has 32 combinations: four modes (`direct`, `bouncer`, `direct-prepared`, `bouncer-prepared`), two index sets and P = 15, 60, 200, 400.

`current` uses the product indexes (primary key, `jobs_batch`, `jobs_parent`, `jobs_recurring`). `extra` adds a B-tree on `(type, id)` and GIN on `(args jsonb_path_ops)` to simulate additional indexes.

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- partitions
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- partitions --mode all --partitions 15,20 --indexes current --rate 500 --warmup 5s --duration 20s --out /tmp/pgbolt-load
```

If a combination fails, the summary records the failure and the matrix continues. The run then returns code 2. The `*-prepared` modes identify the plan used by each statement (custom or generic). Lock counts for generic ack and enqueue plans are lower bounds. `bouncer-prepared` requires PgBouncer's `max_prepared_statements > 0`. The harness sets it to 200 in its own container. With external PgBouncer the harness has no admin console access and reports that it could not check the setting. If it is 0, `bouncer-prepared` combinations fail with `prepared statement "…" does not exist` and the run returns code 2.

Add `--queue-profile limited` to exercise limited dequeue. All jobs go to `default`, and the two 32-worker hosts share a cluster-wide limit of 32. The default `standard` profile uses `default` and `critical` without a limit.

The harness measures dequeue locks after fetching 32 jobs from a single day and before rolling back the transaction. Prepared modes measure repeated execution of the selected plan. Over the measurement window, the report combines FetchAsync durations from both hosts and gives p99, empty, nonempty and failed calls, database sends, and server-side p99 advisory lock wait. Tracking nested SELECTs requires `pg_stat_statements.track=all`, which the harness enables in its own container. The outer dequeue duration includes `read_jobs`.

### vacuum

Measure autovacuum on `queue` under profile B. After a 2-minute warmup, phase 1 runs at a steady rate for 20 minutes. Phase 2 holds xmin for 5 minutes with an open `REPEATABLE READ` transaction. Phase 3 measures recovery for 10 minutes. The harness evaluates SLOs 6–9 without a drain. If the server terminates the session holding xmin, phase 2 is invalid and the run aborts with an error.

Use `--queue-reloptions` to apply `ALTER TABLE queue SET (...)` before the run and compare settings without changing code. `--pg-setting` configures the harness's PostgreSQL container through `-c k=v` arguments at startup.

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- vacuum
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- vacuum --mode both --rate 500 --warmup 5s --phase1 20s --hold-xmin 10s --phase3 20s --queue-reloptions "autovacuum_vacuum_cost_delay=0" --pg-setting "autovacuum_naptime=10s" --out /tmp/pgbolt-load
```

SLO 7 returns “no data” when phase 1 lasts less than 3 minutes.

### External database

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- sustained --connection "Host=db;Database=bench;Username=u;Password=p" --rate 500 --warmup 10s --duration 20s --out /tmp/pgbolt-load
```

With `--connection`, the harness creates a unique schema and drops it after the run. It uses `pg_stat_statements` when available. Otherwise the affected report columns show “no data”. PgBouncer modes require `--bouncer-connection`. The harness has no access to an external PgBouncer admin console, so `SHOW POOLS` metrics and SLO 6 for `bouncer` show “no data”.

## Flags

| Flag | Scenarios | Default | Meaning |
|---|---|---|---|
| `--mode direct\|bouncer\|both\|direct-prepared\|bouncer-prepared\|all` | all | Without `--connection`, `both` (`direct` + `bouncer`) or `all` for `partitions`. With only `--connection`, `direct` or the two direct modes for `partitions`. With both connection flags, `both` or `direct`, `bouncer`, `direct-prepared` for `partitions`. | Connection modes. Only `partitions` accepts `*-prepared` and `all`. With `--connection`, the `bouncer*` modes require `--bouncer-connection`. With external PgBouncer, explicitly select `--mode bouncer-prepared` or `all` to exercise `bouncer-prepared`. |
| `--connection "<cs>"` | all | harness containers | External database connection string |
| `--bouncer-connection "<cs>"` | all | harness PgBouncer | External PgBouncer connection string, requires `--connection` |
| `--out <dir>` | all | `benchmarks/PgBolt.LoadTest/results` | Report directory, relative to the current directory |
| `--rate <n>` | `sustained`, `partitions`, `vacuum` | 10,000 | Target enqueues/s |
| `--warmup <t>` | `sustained`, `partitions`, `vacuum` | 1 min, 30 s for `partitions`, 2 min for `vacuum` | Warmup before measurement (before phase 1 for `vacuum`) |
| `--duration <t>` | `sustained`, `partitions` | 10 min, 2 min per combination for `partitions` | Measurement window |
| `--ramp-start <n>` | `ramp` | 2,000 | First step rate |
| `--ramp-step <n>` | `ramp` | 2,000 | Rate increase per step |
| `--ramp-step-duration <t>` | `ramp` | 60 s | Step duration, at least 30 s |
| `--ramp-max <n>` | `ramp` | 40,000 | Final step rate, at least `--ramp-start` |
| `--partitions <P,...>` | `partitions` | 15,60,200,400 | Number of `jobs` partitions |
| `--indexes current,extra` | `partitions` | `current,extra` | Index sets |
| `--phase1 <t>` | `vacuum` | 20 min | Steady-state phase |
| `--hold-xmin <t>` | `vacuum` | 5 min | Phase holding xmin |
| `--phase3 <t>` | `vacuum` | 10 min | Recovery phase |
| `--queue-reloptions "k=v,..."` | `vacuum` | — | Apply `ALTER TABLE queue SET (...)` before the run |
| `--queue-profile <profile>` | `partitions` | `standard` | Use `limited` for one queue with a cluster-wide limit of 32 |
| `--pg-profile <profile>` | `sustained`, `partitions`, `vacuum` | `bench` | Use `stock` for PostgreSQL defaults or `bench` for `shared_buffers=1GB` and `max_wal_size=4GB`. Applies to the harness container. |
| `--pg-setting "k=v,..."` | `sustained`, `vacuum` | — | Override profile settings on the PostgreSQL container. Incompatible with `--connection`. |

Durations are numbers followed by `s`, `m` or `h` (for example, `30s`, `2m`, `1h`). Measurement windows, phases and ramp steps must be multiples of 10 s because SLOs use 10-second windows. Warmup has no such restriction. Unknown flags, flags used outside their scenarios, missing values and invalid combinations return code 2 and write an error to stderr.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | All evaluated SLOs passed. A `ramp` run that reaches an SLO violation or the harness CPU limit completes successfully. For `partitions`, the SLOs at `P = 15, current` passed in all modes. |
| 1 | At least one SLO failed, including SLO 1 with the `enqueue` mark for underload. `ramp` never returns 1. For `partitions`, only a failure at `P = 15, current` in at least one mode can produce 1. Other partition counts and `extra` contribute to the P threshold. A matrix without `P = 15, current` cannot return 1. |
| 2 | Harness exception, failed `partitions` combination, unavailable database or containers (30 consecutive unsuccessful sampling attempts), invalid arguments, Ctrl+C, or `max_prepared_statements = 0` in the harness PgBouncer during `bouncer-prepared`. External PgBouncer skips this check, but combinations fail if the value is 0. Harness CPU saturation can also return 2, except for `ramp`; in `partitions` this applies only at `P = 15, current`. |

A “no data” verdict means a source was unavailable or a measurement interval was too short. It does not affect the exit code. Multiple modes return the highest code.

## SLOs

The harness evaluates each mode independently over its measurement window.

| # | SLO | Threshold |
|---|---|---|
| 1 | Throughput | Every 10 s window must complete at least 95% of the target rate. A window where the producer enqueues less than 98% of rate × 10 s fails with the mark `enqueue`. |
| 2 | Immediate job start latency | p50 ≤ 50 ms, p99 ≤ 500 ms, p99.9 ≤ 2 s |
| 3 | Delayed job lateness from `run_at` | p99 ≤ 1.5 s |
| 4 | Backlog of ready, unclaimed jobs | Every sample ≤ rate × 1 s. Mean of the last minute ≤ mean of the first minute + rate × 1 s. |
| 5 | Correctness after a drain of ≤ 2 min | Every `jobs` row is Succeeded, `queue` is empty, `outcome.dropped` = 0 and `storage.errors` = 0. Execution count is 2 for jobs that fail on the first attempt and 1 for all others. `jobs.attempt` matches. |
| 6 | Connections | `bouncer`: maximum `maxwait` in `SHOW POOLS` ≤ 100 ms in every sample. `direct`: host and producer connections ≤ 48. |
| 7 | Phase 1 `queue` size (`vacuum`) | Last-minute mean ≤ 2 × mean over seconds 60–120 of phase 1 |
| 8 | Phase 1 stability (`vacuum`) | SLOs 1–4 pass in all 10 s windows of phase 1 |
| 9 | Recovery (`vacuum`) | SLOs 1 and 2 and the first part of SLO 4 recover within 5 minutes of the start of phase 3 |

`sustained` evaluates SLOs 1–6. `ramp` evaluates SLOs 1–2 per step. `partitions` evaluates SLOs 1–2 per combination. `vacuum` evaluates SLOs 6–9. Without a drain, it skips SLO 5 and records `outcome.dropped`, `storage.errors` and duplicate executions without thresholds.

## Reports

Git ignores the default report directory. Published measurements and conclusions are in [the performance report](../../docs/performance.md).

Each mode writes `<yyyyMMdd-HHmm>-<scenario>-<mode>.md` to `--out` alongside a `.csv` with one-second samples. Each `partitions` combination appends `-p<P>-<indexes>` to the name. The full matrix also writes `<yyyyMMdd-HHmm>-partitions-summary.md`. If a filename already exists, the harness appends `-2`, `-3`, and so on.

Each Markdown report includes

- Run parameters and environment: host and Docker resources, PostgreSQL and PgBouncer versions, `--pg-profile` (`external` for an external database), `--pg-setting`, effective memory, WAL, autovacuum, diagnostics and durability settings, `queue` and `completions` reloptions, .NET version and commit.
- SLO thresholds, measured values, PASS/FAIL/“no data” verdicts, and `enqueue` or `harness-limited` marks.
- The `ramp` result (maximum sustained rate and first violation) or the inputs for selecting autovacuum settings (`vacuum`).
- Immediate and delayed job start latency (p50, p90, p99, p99.9 and max) and histogram overflows.
- Throughput per 10 s window: completions, enqueues, `EnqueueManyAsync` calls, mean batch size, enqueue errors and p99, CPU usage, generator wait, dropped tokens and underload.
- Harness CPU usage and `pg_stat_statements` totals by class (dequeue, ack, outcome, heartbeat, enqueue, promoter and, when found, `dequeue_limited`). Each class includes calls and mean and total planning and execution times.
- Statement locks (`partitions`): plan type, non-fast-path and total locks, and whether each count is a measurement or lower bound.
- Bloat and autovacuum statistics, final database size, checkpoints and `LockManager` waits.
- Correctness and PgBolt counters: missing executions (only in `sustained` after drain), duplicate executions, `outcome.dropped` and `storage.errors`.
- Sampler overhead (query time, skipped ticks and failed queries) and environment warnings.

The `partitions` summary reports shared lock table capacity and, for each combination, SLOs 1–2, p99, `LockManager` waits, `storage.errors` and any failure reason. It includes hot-path statements, plan type, locks, `mean_plan_time` and `mean_exec_time`. The P threshold is the smallest tested P where a mode and index set violate an SLO.

CSV columns are `at_s`, `ready_backlog`, `queue_total_bytes`, `max_wait_us`, `cl_waiting`, `sv_active`, `sv_idle`, `connections`, `completed`, `delayed`, `leased`, `lock_manager_waits`, `autovacuum_workers`, `queue_live_tup`, `queue_dead_tup`, `completions_live_tup`, `completions_dead_tup`, `wal_bytes`, `vacuums`, `query_ms` and `counts_ms`. An empty cell indicates a missed sample or unavailable source.

Individual laptop runs are noisy. Compare modes, P and reloptions within a run. Treat differences below 10% as a tie. The report shows checkpoint spikes so they can be distinguished from PgBolt overhead.

## Duration

| Suite | Approximate time |
|---|---|
| `sustained`, 2 modes | 0.5 h (per mode: 1 min warmup + 10 min measurement + ≤ 2 min drain + setup) |
| `ramp`, 2 modes | 0.7 h (up to 20 one-minute steps per mode) |
| `partitions`, 32 combinations | 1.8 h (about 3.5 min per combination) |
| `vacuum`, 2 modes × 2 variants (current reloptions and `autovacuum_vacuum_cost_delay=0`) | 2.5 h (about 38 min per run) |
| Full suite | 5.5 h, or up to about 6.8 h with the global `autovacuum_naptime=10s` candidate (another 2 × 38 min) |

Run the full suite in the background, one scenario at a time. Runs use separate containers and should execute sequentially on a single machine.

## Smoke tests

The integration tests require Docker. They include a short in-process run of each scenario and verify that it creates a report and returns 0 or 1 (0 for `ramp`).

```
dotnet test tests/PgBolt.LoadTest.Tests --filter "Category=Load"
```

The regular `dotnet test PgBolt.slnx --filter "Category!=Load"` run excludes these tests.
