# Observability with traces, metrics, and logs

PgBolt emits standard telemetry for an OpenTelemetry stack with Prometheus, Tempo, and Loki.

- **Traces** connect the producer span that enqueues a job and the consumer span that executes it in a single trace.
- **Metrics** cover individual pods and the cluster (`pgbolt.cluster.*`). Pod metrics are additive, and the histograms support `histogram_quantile`.
- **Logs** include job lifecycle events, a per-job logging scope, and stable `EventId` values for alerts.

The core uses only the BCL (`System.Diagnostics.ActivitySource` and `System.Diagnostics.Metrics.Meter`) and has no dependency on the OpenTelemetry SDK.
Telemetry adds little overhead without subscribers. See [Overhead](#overhead).

- [Setup](#setup)
- [Tracing](#tracing)
- [Metrics](#metrics)
- [Cluster metrics and aggregation](#cluster-metrics-and-aggregation)
- [Cardinality](#cardinality)
- [Logs](#logs)
- [PromQL examples](#promql-examples)
- [Overhead](#overhead)

## Setup

### The `PgBolt.OpenTelemetry` package

The package depends on `PgBolt.Abstractions` and OpenTelemetry SDK 1.15.3 (the `OpenTelemetry` package).
The upgrade from 1.12.0 addresses GHSA-g94r-2vxg-569j in `OpenTelemetry.Api` 1.12.0.
The package has no references to `PgBolt.Postgres` or `PgBolt.Server`.

```csharp
builder.Services.AddOpenTelemetry()                               // OpenTelemetry.Extensions.Hosting package
    .ConfigureResource(r => r.AddService("orders-worker"))        // sets service.instance.id, see below
    .WithTracing(t => t.AddPgBoltInstrumentation().AddOtlpExporter())
    .WithMetrics(m => m.AddPgBoltInstrumentation().AddOtlpExporter());
```

- `TracerProviderBuilder.AddPgBoltInstrumentation()` subscribes the provider to the `PgBolt` activity source.
- `MeterProviderBuilder.AddPgBoltInstrumentation()` subscribes to the `PgBolt` `Meter` and sets histogram bucket boundaries
  through views for SDKs that ignore the instrument's advice.

### Setup without the package

`PgBoltDiagnostics` in `PgBolt.Abstractions` exposes the source and `Meter` names as constants.

```csharp
tracing.AddSource(PgBoltDiagnostics.ActivitySourceName);   // "PgBolt"
metrics.AddMeter(PgBoltDiagnostics.MeterName);             // "PgBolt"
```

Instrument names (`PgBoltDiagnostics.Instruments`) are also public constants. The bucket boundaries
(`PgBoltDiagnostics.HistogramBuckets.Job` and `.Maintenance`) are public, read-only static properties of type `IReadOnlyList<double>`.

### Meter and `IMeterFactory`

Each DI container gets one `Meter`. PgBolt obtains the `Meter` from `IMeterFactory` when available.
`Host.CreateApplicationBuilder` and `Host.CreateDefaultBuilder` register the factory automatically. Otherwise, call `services.AddMetrics()` to register it.
Without a factory, PgBolt creates its own `Meter` and disposes it with the container. Hosts in the same process keep their measurements separate because there is no static `Meter`.

### Identifying pods with `service.instance.id`

Pod metrics are additive, and cluster metrics have the same values across pods. The `service.instance.id` resource attribute distinguishes pods.
The OTel .NET SDK sets it through `ResourceBuilder.AddService(...)` (the default is `autoGenerateServiceInstanceId: true`) or the
`OTEL_RESOURCE_ATTRIBUTES` environment variable. `ResourceBuilder.CreateDefault()` omits it. Without it, Prometheus-compatible backends
merge measurements from different pods into a single series.

## Tracing

### Spans

| Span | Kind | When PgBolt creates it |
|---|---|---|
| `enqueue {job.type}` | Producer | A call to `IJobClient.EnqueueAsync` / `EnqueueManyAsync` (all overloads, including dynamic enqueue by name and transactional enqueue) |
| `workflow submit` | Producer | `Workflow()….SubmitAsync()` |
| `recurring trigger` | Producer | `IRecurringJobManager.TriggerAsync` |
| `{job.type}` | Consumer | One job execution attempt in `JobRunner` |

PgBolt creates one producer span per client API call, which may enqueue multiple jobs. A failed call sets the status to `Error`, sets
`error.type` to the fully qualified exception type, and records an exception event. A transactional enqueue still leaves a span if the caller rolls back the job.

Each execution attempt gets a separate consumer span under the same parent. Jobs released before execution during server shutdown
(`Release` before start) have no span.

### Connecting execution to enqueue

The `jobs.trace_parent` column stores the W3C `traceparent`. PgBolt does not propagate `tracestate` or baggage.

- When a call has a producer span, PgBolt stores its ID. The consumer span uses the producer as its **remote parent**. This parent-child relationship, instead of an
  `ActivityLink`, keeps the entire path in one trace, from HTTP through enqueue and execution to calls made by the job (HttpClient, Npgsql).
  Retries an hour later and delayed jobs can produce long-running traces that Tempo displays poorly and tail sampling can split.
- Without a producer span (no listener, sampler declined, or a non-W3C parent), PgBolt stores the ID of `Activity.Current` if it uses W3C format, otherwise `NULL`.
- `IRecurringJobManager.TriggerAsync` writes `trace_parent` using the same rule. It previously wrote `NULL`.
- A scheduler tick runs in the background without a context. Jobs it enqueues get `trace_parent = NULL`, so each consumer span starts a new trace.
- Workflow continuations receive the `trace_parent` of the `workflow submit` span, shared by all nodes.
- `IJobClient.RequeueAsync` **preserves** `trace_parent`. Like a retry, the next execution is a child of the original producer span in the original trace.

An empty or unparseable `trace_parent` produces a root consumer span without an error or log entry. Before starting the consumer span,
the runtime clears `Activity.Current` so a job without a `trace_parent` cannot inherit an unrelated long-lived `Activity` that started the host.
Inside the job, `Activity.Current` is its consumer span.

### Span attributes

| Attribute | Producer | Consumer | Value |
|---|---|---|---|
| `messaging.system` | yes | yes | `pgbolt` |
| `messaging.operation.type` | yes | yes | `send` / `process` |
| `messaging.destination.name` | yes | yes | Queue name (the producer span omits it for workflows spanning multiple queues) |
| `messaging.message.id` | — | yes | Job ID |
| `messaging.batch.message_count` | when job count > 1 | — | Number of jobs in the call |
| `pgbolt.job.type` | enqueue | yes | Job type |
| `pgbolt.job.attempt` | — | yes | Attempt number |
| `pgbolt.job.max_attempts` | — | yes | Maximum attempts |
| `pgbolt.server.name` | — | yes | Virtual server name |
| `pgbolt.batch.id` | — | when present | ID of the batch containing the job |
| `pgbolt.job.parent_job_id` | — | job continuation | Parent job ID |
| `pgbolt.job.parent_batch_id` | — | batch continuation | Parent batch ID |
| `pgbolt.recurring.id` | trigger | when present | Recurring job ID |
| `pgbolt.job.outcome` | — | after execution | `succeeded` / `retry` / `failed` / `cancelled` / `released` |
| `error.type` | on error | on `retry` / `failed` | See below |

For `retry` and `failed`, the consumer span receives `Error` status and an exception event. If the corresponding condition interrupted
the attempt, `error.type` is `timeout`, `heartbeat_lost`, or `lease_lost`. Otherwise, PgBolt records the fully qualified exception type.
For a job without a registered executor, the type is `PgBolt.Server.JobExecutorNotFoundException`.

## Metrics

Pod metrics are additive. Use **`sum` across pods** for the cluster total. Every pod reports the same cluster values (`pgbolt.cluster.*`).
Use **`max` across pods** for these metrics.

The names below use the OTel-to-Prometheus translation. Dots become underscores, the unit `s` adds the suffix `_seconds`, and counters take `_total`.
Exact names depend on the exporter and its translation settings.

### Pod metrics (aggregate with `sum`)

| Metric | Type, unit | Tags | What it measures |
|---|---|---|---|
| `pgbolt.job.enqueued` | Counter, `{job}` | `pgbolt.queue`, `pgbolt.job.type`, `pgbolt.enqueue.result` = `created` / `duplicate` | Client-enqueued jobs from enqueue, workflow submission, or recurring `TriggerAsync` |
| `pgbolt.recurring.fired` | Counter, `{job}` | — | Jobs enqueued by a scheduler tick |
| `pgbolt.job.duration` | Histogram, `s` | `pgbolt.queue`, `pgbolt.job.type`, `pgbolt.job.outcome` | Duration of one execution attempt |
| `pgbolt.job.wait_time` | Histogram, `s` | `pgbolt.queue`, `pgbolt.job.type` | Time from `run_at` to lease acquisition |
| `pgbolt.job.outcome.dropped` | Counter, `{job}` | `pgbolt.queue`, `pgbolt.drop.reason` = `lease_lost` / `abandoned` / `stopped` / `send_timeout` | Outcomes the runtime could not apply |
| `pgbolt.worker.active` | ObservableUpDownCounter, `{worker}` | `pgbolt.server.name`, `pgbolt.queue` | Occupied queue slots |
| `pgbolt.worker.capacity` | ObservableUpDownCounter, `{worker}` | `pgbolt.server.name` | Virtual server capacity, including `Workers` and the sum of `Reserved` slots |
| `pgbolt.storage.errors` | Counter, `{error}` | `pgbolt.storage.operation` | Caught database access errors |
| `pgbolt.maintenance.runs` | Counter, `{run}` | `pgbolt.maintenance.task`, `pgbolt.maintenance.result` = `ok` / `error` / `deferred` | Maintenance task runs |
| `pgbolt.maintenance.duration` | Histogram, `s` | `pgbolt.maintenance.task` | Duration of a maintenance task run |
| `pgbolt.retention.deferred` | Counter, `{partition}` | `pgbolt.retention.reason` | Partitions retention could not process in this pass |

Prometheus names are `pgbolt_job_enqueued_total`, `pgbolt_recurring_fired_total`, `pgbolt_job_duration_seconds_{bucket,sum,count}`,
`pgbolt_job_wait_time_seconds_*`, `pgbolt_job_outcome_dropped_total`, `pgbolt_worker_active`, `pgbolt_worker_capacity`,
`pgbolt_storage_errors_total`, `pgbolt_maintenance_runs_total`, `pgbolt_maintenance_duration_seconds_*`, and `pgbolt_retention_deferred_total`.
Tags become labels with underscores, such as `pgbolt_queue` and `pgbolt_job_outcome`.

**`pgbolt.job.enqueued`.** Counts rows. Each call records one measurement per combination of queue, job type, and result, since workflow nodes
can use different queues. Workflow submission counts all nodes as `created`. A key conflict throws an exception and excludes the call from the count.
Transactional enqueue counts when the call returns, **even if the caller subsequently rolls back the transaction**. Jobs from scheduler ticks
have their own counter, `pgbolt.recurring.fired`.

**`pgbolt.job.wait_time`.** Measures time from `run_at` until PgBolt acquires the lease. For immediate jobs, `run_at` is the enqueue time.
For delayed jobs and retries, it is the scheduled time, so the wait includes promoter lag. For workflow continuations and triggers, it is the time
the job becomes ready. `Requeue` sets `run_at = now()`. Neither release nor the sweeper changes `run_at`, so **`wait_time` for a new lease after
release or sweeping includes the previous attempt**. Jobs leased and returned without starting (server shutdown, heartbeat loss during dequeue,
or dequeue returning after its timeout) contribute their wait to the next lease. PostgreSQL calculates the time, so pod clock skew has no effect.

**`pgbolt.job.duration`.** PgBolt records duration even without a tracing listener. The `pgbolt.job.outcome` values are `succeeded`, `retry`,
`failed`, `cancelled`, and `released`.

**`pgbolt.job.outcome.dropped`.** Counts outcomes the runtime could not apply.

| `pgbolt.drop.reason` | Cause |
|---|---|
| `lease_lost` | The lease expired or was lost during execution or while the outcome awaited delivery, storage reported a lost lease, or an ack confirmed only some leases in the batch. |
| `abandoned` | Execution exceeded `ShutdownTimeout` during shutdown, `JobRunner.RunAsync` or `CompletionSink.Complete` threw, or execution failed to start after registration. |
| `stopped` | The runtime submitted the outcome after delivery stopped, or cancellation of shutdown interrupted delivery. |
| `send_timeout` | The `LeaseDuration` budget for sending the outcome expired. |

The runtime counts each execution record at most once. A “lease lost” reply after a failed delivery attempt is excluded because storage probably already applied the outcome.

**`pgbolt.storage.errors`.** Increments once per caught exception.

| `pgbolt.storage.operation` | Operation |
|---|---|
| `dequeue` | Dispatcher fetching jobs |
| `ack` | Acknowledging a batch of outcomes |
| `outcome` | Sending retry / fail / confirm-cancel / release |
| `heartbeat` | Sending a heartbeat (error or timeout) or re-registering a server |
| `settings` | Refreshing queue settings |
| `stats_flush` | Flushing outcome statistics |
| `stats_read` | Reading the `queue_stats` snapshot |
| `table_stats_read` | Reading statistics for the `queue` and `completions` tables |
| `promoter`, `sweeper`, `resolver`, `scheduler`, `recurring-sync`, `housekeeping`, `stats` | An iteration of the named maintenance loop |

Errors inside maintenance tasks count toward `pgbolt.maintenance.runs` with `result = error`. A task that reports failure without throwing
(for example, `lock_timeout` during partition creation) records `result = deferred`. Both `pgbolt.maintenance.runs` and
`pgbolt.maintenance.duration` include only runs that the pod actually claimed.

**Histogram bucket boundaries** are fixed and identical across pods, so `histogram_quantile` over `sum by (le)` is valid.

- `pgbolt.job.duration`, `pgbolt.job.wait_time`, in seconds, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600.
- `pgbolt.maintenance.duration`, in seconds, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300.

## Cluster metrics and aggregation

| Metric | Type, unit | Tags | Value |
|---|---|---|---|
| `pgbolt.cluster.queue.jobs` | ObservableGauge, `{job}` | `pgbolt.queue`, `pgbolt.job.state` = `scheduled` / `enqueued` / `processing` / `awaiting` | Job count by queue and state |
| `pgbolt.cluster.queue.oldest_ready_age` | ObservableGauge, `s` | `pgbolt.queue` | Age of the oldest ready job still awaiting a lease |
| `pgbolt.cluster.queue.limit` | ObservableGauge, `{worker}` | `pgbolt.queue` | Effective cluster-wide queue concurrency limit (0 when paused). Queues without a limit omit this metric. |
| `pgbolt.cluster.table.dead_tuples` | ObservableGauge, `{tuple}` | `pgbolt.table` = `queue` / `completions` | Estimated number of dead tuples in the table (`pg_stat_user_tables.n_dead_tup`) |
| `pgbolt.cluster.table.size` | ObservableGauge, `By` | `pgbolt.table` | Total table size including indexes and TOAST (`pg_total_relation_size`) |

Prometheus names are `pgbolt_cluster_queue_jobs`, `pgbolt_cluster_queue_oldest_ready_age_seconds`, `pgbolt_cluster_queue_limit`,
`pgbolt_cluster_table_dead_tuples` (the unit `{tuple}` adds no suffix), and `pgbolt_cluster_table_size_bytes`. The table tag becomes the `pgbolt_table` label.

Every pod that publishes a series under `pgbolt.cluster.` reports the same cluster value. Aggregate with **`max`**. Summing would multiply
the value by the number of reporting pods. Only pods with registered servers (`AddPgBoltServer`) publish these metrics.
Producer-only pods and pods whose servers have stopped omit them.

### Data sources and expected variation

`jobs` and `oldest_ready_age` come from a `queue_stats` snapshot. The `stats` task takes the snapshot on a pod with `RunMaintenance = true`.
Once per `StatsSnapshotInterval`, the `stats` loop on **every** pod with servers reads the `queue_stats` table and the time of the last successful snapshot.

- Pods can differ by at most one `stats` iteration (up to 1.2 × `StatsSnapshotInterval` after the snapshot commits).
  During that window, `max` takes the higher value from the two adjacent snapshots.
- If another pod takes over snapshot creation, there are no duplicates or gaps because each pod publishes independently.
- `limit` comes from the queue settings cache and can lag by one `QueueSettingsRefreshInterval` refresh (up to 1.1 × the interval with ±10% jitter).

### Snapshot freshness

To determine snapshot age, PgBolt uses the time of the last successful snapshot on PostgreSQL (the `stats` row in `maintenance_tasks`,
refined by the snapshot rows' `measured_at` values) and adds the time elapsed since the read.

- **Fresh snapshot** (at most 4 × `StatsSnapshotInterval` old). PgBolt publishes `pgbolt.cluster.queue.jobs` for every known queue and all four states.
  Queues with no rows report **zeros**, so the series remains at zero when idle and `queue_stats` is empty.
  Only queues with ready jobs report `oldest_ready_age`.
- **Stale snapshot** (older than 4 × `StatsSnapshotInterval`, unknown snapshot time, or no pod with `RunMaintenance = true` refreshing it).
  Both `jobs` and `oldest_ready_age` return an **empty set**, so the series disappears. `absent()` therefore indicates a stale or missing snapshot.
  An empty queue with a fresh snapshot still reports zero.

The 4 × threshold keeps series visible after one failed snapshot (age normally reaches ~2.4 × the interval, or ~3.6 × after one failure)
and hides them after two consecutive failures. Known queues include those configured on the pod's servers and those with rows in the snapshot.

**Use the same `StatsSnapshotInterval` on all pods in the cluster.** The owner of the `stats` task determines snapshot frequency, but each pod
uses its own interval to decide when data is stale. A reader with a shorter interval will intermittently lose series.

### Table statistics

`dead_tuples` and `size` show bloat in `queue` and `completions`. If dead tuples accumulate under steady load, autovacuum is falling behind or
an old transaction is holding back cleanup (xmin). The same `stats` loop reads these values on every pod with servers, including pods with
`RunMaintenance = false`. If no read succeeds for more than 4 × `StatsSnapshotInterval` (database unavailable), both series disappear.
Read errors increment `pgbolt.storage.errors` with `pgbolt.storage.operation = table_stats_read`. PostgreSQL updates its `n_dead_tup` estimate
with a delay. Aggregate it with `max`, as with the other cluster metrics.

```promql
max by (pgbolt_table) (pgbolt_cluster_table_dead_tuples)
max by (pgbolt_table) (pgbolt_cluster_table_size_bytes)
```

The following `queue` alert thresholds come from the `vacuum` load test at 10,000 jobs/s. Scale them down proportionally for lower rates.
Use `for: 10m` to detect dead tuples that persist because autovacuum cannot keep up or an old transaction blocks cleanup. [Load test results](performance.md#vacuum)

```promql
max(pgbolt_cluster_table_dead_tuples{pgbolt_table="queue"}) > 1000000
```

With autovacuum running, `queue`'s `n_dead_tup` rises between passes and drops after each pass. With a 1-minute `autovacuum_naptime`, it peaked
at 1.28–2.41 million and stayed above 1 million for at most 72 s, so `for: 10m` filters out these peaks. While a transaction holds xmin,
the count rises without dips after the first autovacuum pass. At a 1-minute `autovacuum_naptime`, that pass occurs ~30 s into the hold and
drops the count to 0.29–0.37 million. The count exceeds 1 million after 2.6–3 minutes. The 5-minute holds in the tests ended before the alert
could fire. At 10,000/s, it should fire after about 13 minutes. The size alert below detects the hold sooner.

Alert when the table grows by more than 500 MB in an hour.

```promql
delta(max(pgbolt_cluster_table_size_bytes{pgbolt_table="queue"})[1h:]) > 524288000
```

Under steady load (20 minutes), the size of `queue` rose and fell between autovacuum passes. With a 1-minute `autovacuum_naptime`, the two
`bouncer` runs ranged from 135–259 and 142–262 MB, a spread of up to 124 MB. Other runs varied by at most 42 MB. These ranges bound
`delta` for any window within the measurement. In `bouncer`, successive minima rose 36–43 MB in 10 minutes (135 → 158 → 178 and 142 → 162 → 178 MB).
Extrapolated over an hour, the ~220–260 MB trend plus the oscillation gives ~340–380 MB, below the 500 MB threshold. The longest measured
window was 20 minutes. If the alert fires without long transactions, check autovacuum on `queue` or raise the threshold.
During a 5-minute xmin hold, size grew by 324–511 MB. It reached 500 MB above the steady-phase minimum by the end of the hold or about
20–25 s later, and reached the same margin above the steady-phase maximum within 3 minutes after release. Size kept growing during recovery, peaking at
755–893 MB. By the end of recovery, autovacuum had reduced it to 385–498 MB in 3 of 6 runs. The other runs ended at the peak.
The alert remains active while growth over the preceding hour exceeds the threshold.

### `limit` stays available

When the database is unavailable, `pgbolt.cluster.queue.limit` reports the last value read, the same value dequeue uses.
`QueueSettingsCache` keeps its previous snapshot when a refresh fails. The pod stops publishing this metric only when its servers stop.

## Cardinality

Metric tag values come from bounded sets of job types, queues, virtual server names, and maintenance tasks.
Tags exclude individual job, batch, and server IDs.

`pgbolt.job.duration` has the most series. For each pod, multiply the number of (queue, job type) pairs with execution attempts by the 5 outcomes.
By default, the OTel .NET SDK keeps up to 2000 series per metric and records excess values in an overflow series.
If you have hundreds of job types, estimate the series count first and raise `CardinalityLimit` if needed.

`MeterProviderBuilder.AddPgBoltInstrumentation()` already adds bucket views for `pgbolt.job.duration`, `pgbolt.job.wait_time`, and
`pgbolt.maintenance.duration`. A second view for the same instrument creates a **duplicate metric**.
To raise the limit, subscribe without `AddPgBoltInstrumentation()`. The instruments supply their own bucket boundaries.

```csharp
metrics
    .AddMeter(PgBoltDiagnostics.MeterName)
    .AddView(PgBoltDiagnostics.Instruments.JobDuration, new MetricStreamConfiguration { CardinalityLimit = 5000 })
    .AddView(PgBoltDiagnostics.Instruments.JobWaitTime, new MetricStreamConfiguration { CardinalityLimit = 5000 });
```

## Logs

### Job lifecycle events

In the `PgBolt.Job` category, the outcome of each attempt determines the event. The `Reason` field records interruptions
(`None`, `Timeout`, `HeartbeatLost`, `LeaseLost`, `Shutdown`). Timeouts and lease loss appear as `JobFailedWillRetry` / `JobFailed`, without separate events.

The 3000–3099 job lifecycle table below lists each event and its corresponding outcome.

PgBolt logs the exception for each unsuccessful attempt (`Retry`, `Fail`, confirmed cancellation) with its `JobId` and `EventId`.
For `Reason = LeaseLost`, the event records the attempt's result even though the outcome will not be applied (see `pgbolt.job.outcome.dropped`).
The separate `LeaseKeeper` and `CompletionSink` events describe lease handling and outcome delivery.

### Job scope and trace correlation

Each execution runs in a logging scope containing `JobId`, `JobType`, `Queue`, and `Attempt`. Job code can use any `ILogger` to access the scope
with providers supporting `IExternalScopeProvider`. Text providers receive `ToString()`. The log provider (OTel logs exporter or `ActivityTrackingOptions`)
adds `TraceId` and `SpanId` from `Activity.Current` **only when a consumer span exists**. The `PgBolt` source needs a listener and the sampler
must accept the span. Without a listener, job logs have no `TraceId`.

### Stable event IDs

All PgBolt log calls use `[LoggerMessage]` with explicit `EventId` and `EventName`. Alerts and filters can rely on stable `Id` and `Name`
values. PgBolt never reuses them, and `EventIdContractTests` checks the set. Message text and logger categories have stayed the same.

| Range | Component |
|---|---|
| 1000–1999 | Server and process lifecycle, queue settings |
| 2000–2999 | Dispatcher |
| 3000–3999 | Job lifecycle, retry policy |
| 4000–4999 | `CompletionSink`, `LeaseKeeper`, `OutcomeStatsFlusher` |
| 5000–5999 | Maintenance, retention, recurring jobs, resolver |
| 6000–6999 | `PgBoltInitializer`, recurring job synchronization |
| 7000–7999 | Dashboard |
| 8000–8999 | Telemetry |

### Telemetry failures

PgBolt catches exceptions thrown by an `ActivityListener`, `MeterListener`, or logging provider while emitting telemetry. It skips the signal,
preserves the job outcome, releases the slot, and allows shutdown to proceed. Event 8001 (`TelemetryFailed`) reports failures, or
8002 (`MetricCallbackFailed`) for an observable callback. PgBolt reports failures at most once per minute per container in the
`PgBolt.Telemetry` category. If 8001 appears, check your listeners and providers.

### Event reference

The tables list all events with their default `Level`. PgBolt selects the level of event 5107 at runtime.
It logs Warning if partitions are overdue by at least a day, otherwise Debug.

### 1000–1099: Process and host (`PgBoltProcessingService`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 1000 | `JobTypeSchemaMismatch` | Warning | — | `PgBolt: job types {JobTypes} were published by another process with a different argument schema.` |
| 1001 | `ProcessingStartFailed` | Critical | yes | `PgBolt: starting processing failed; stopping what was started.` |
| 1002 | `HostStopTimeoutServers` | Warning | — | `PgBolt: host stop timeout reached; pending outcomes are dropped and servers rows are left, leases will expire.` |
| 1003 | `HostStopTimeoutMaintenance` | Warning | — | `PgBolt: host stop timeout reached while stopping maintenance loops.` |
| 1004 | `QueueDefaultsConflict` | Warning | — | `PgBolt: queue '{Queue}' defaults were recently published by {PublishedBy} with other values; overwritten: {Changes}.` |
| 1005 | `HostShutdownTimeoutTooShort` | Warning | — | `PgBolt: HostOptions.ShutdownTimeout {HostTimeout} is shorter than {Required} (max server ShutdownTimeout + AckFlushInterval + 2 s); executions may be abandoned and their attempts counted.` |
| 1006 | `PartitionDaysAheadTooShort` | Warning | — | `PgBolt: PartitionDaysAhead ({DaysAhead} days) is shorter than 2 × PartitionsInterval ({Interval}); a single failed iteration may leave no partition for the next day.` |
| 1007 | `DeadServerTimeoutTooShort` | Warning | — | `PgBolt: DeadServerTimeout ({Timeout}) is shorter than 3 × HeartbeatInterval ({Heartbeat}); rows of live servers will be removed on short heartbeat hiccups and restored by the next heartbeat.` |

### 1100–1199: Virtual server (`PgBoltServer`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 1100 | `ServerStarted` | Information | — | `PgBolt server {Server} ({ServerId}) started: queues {Queues}, capacity {Capacity}.` |
| 1101 | `ExecutionsWaitCancelled` | Warning | — | `PgBolt server {Server}: waiting for executions was cancelled by the host.` |
| 1102 | `ServerStopCancelled` | Warning | — | `PgBolt server {Server}: stop was cancelled by the host; the servers row is left for cleanup.` |
| 1103 | `ServerRowRemoveFailed` | Error | yes | `PgBolt server {Server}: failed to remove the servers row.` |
| 1104 | `ServerStopped` | Information | — | `PgBolt server {Server} ({ServerId}) stopped.` |
| 1105 | `DispatcherFailed` | Critical | yes | `PgBolt server {Server}: dispatcher failed; stopping continues.` |
| 1106 | `CancellationCallbackFailed` | Warning | yes | `PgBolt server {Server}: a cancellation callback of a job threw.` |
| 1107 | `JobTypesWithoutExecutor` | Warning | — | `PgBolt server {Server}: job types {JobTypes} default to its queues but have no executor in this process; such jobs go to NoExecutor.` |
| 1108 | `MissingExecutorsCheckFailed` | Warning | yes | `PgBolt server {Server}: failed to check job types without executors.` |
| 1109 | `JobFinishFailed` | Critical | yes | `PgBolt server {Server}: unexpected failure while finishing job {JobId}.` |
| 1110 | `JobAbandoned` | Warning | — | `PgBolt server {Server}: job {JobId} did not stop within {ShutdownTimeout} and is abandoned; its lease will expire and the sweeper will count the attempt.` |
| 1111 | `JobIgnoresCancellation` | Warning | — | `PgBolt server {Server}: job {JobId} has ignored cancellation ({Reason}) for {Elapsed}; its slot stays occupied.` |
| 1112 | `SlotReleaseFailed` | Error | yes | `PgBolt server {Server}: slot of queue {Queue} was not returned after job {JobId} finished.` |

### 1200–1299: Queue settings (`QueueSettingsCache`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 1200 | `QueueSettingsRefreshFailed` | Warning | yes | `PgBolt: refreshing queue settings failed; using the previous snapshot.` |

### 2000–2999: Dispatcher (`Dispatcher`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 2000 | `DequeueFailed` | Error | yes | `PgBolt: dequeue from queue {Queue} failed; fetching paused for {Pause}.` |
| 2001 | `DequeueTimedOutJobsReleased` | Warning | — | `PgBolt: dequeue from queue {Queue} returned {Count} jobs after {Elapsed}, past its timeout {Timeout}; they are released without running.` |
| 2002 | `DispatcherIterationFailed` | Critical | yes | `PgBolt: dispatcher iteration failed unexpectedly; fetching paused for {Pause}.` |
| 2003 | `WakeupSubscriptionDisposeFailed` | Warning | yes | `PgBolt: disposing a wakeup subscription failed.` |
| 2004 | `WakeupSubscriptionFailed` | Error | yes | `PgBolt: wakeup subscription for queue {Queue} failed; the queue is polled without wakeup signals.` |
| 2005 | `StartRollbackSlotReleaseFailed` | Error | yes | `PgBolt: slot of queue {Queue} was not returned after the failed start of job {JobId}.` |
| 2006 | `JobStartRolledBack` | Warning | — | `PgBolt: job {JobId} of queue {Queue} was not started; its registration is rolled back, the lease will expire.` |

### 3000–3099: Job lifecycle (`JobRunner`, category `PgBolt.Job`)

| Id | Name | Outcome | Level | Exception | Template |
|---|---|---|---|---|---|
| 3001 | `JobStarted` | attempt started | Debug | — | `PgBolt: job {JobId} ({JobType}) attempt {Attempt} started on queue {Queue}.` |
| 3002 | `JobSucceeded` | `Ack` | Debug | — | `PgBolt: job {JobId} succeeded in {DurationMs} ms.` |
| 3003 | `JobFailedWillRetry` | `Retry` | Warning | yes | `PgBolt: job {JobId} attempt {Attempt} of {MaxAttempts} failed ({Reason}); retrying in {NextAttemptIn}.` |
| 3004 | `JobFailed` | `Fail` | Error | yes | `PgBolt: job {JobId} failed at attempt {Attempt} of {MaxAttempts} ({Reason}); no attempts left.` |
| 3005 | `JobNoExecutorWillRetry` | no executor, `Retry` | Warning | yes | `PgBolt: no executor is registered for job {JobId} of type {JobType}; retrying in {NextAttemptIn}.` |
| 3006 | `JobNoExecutorFailed` | no executor, `Fail` | Error | yes | `PgBolt: no executor is registered for job {JobId} of type {JobType}; the job is failed.` |
| 3007 | `JobCancelled` | `ConfirmCancel` | Information | yes | `PgBolt: job {JobId} was cancelled.` |
| 3008 | `JobReleased` | `Release` | Debug | — | `PgBolt: job {JobId} was released without spending an attempt (ran: {Ran}).` |

### 3100–3999: Retry policy and timeouts (`RetryDecider`, `JobRunner`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 3100 | `RetryPolicyFailed` | Error | yes | `PgBolt: RetryPolicy threw for attempt {Attempt}; the job is failed without retry.` |
| 3101 | `JobTimeoutCapped` | Warning | — | `PgBolt: job '{JobName}' has TimeoutSeconds = {TimeoutSeconds}, above the .NET timer limit; its timeout is capped at {MaxJobTimeout}.` |

### 4000–4099: Outcome delivery (`CompletionSink`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 4000 | `OutcomeDiscardedSinkStopped` | Warning | — | `PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink is stopped.` |
| 4001 | `OutcomeDiscardedNotRunning` | Warning | — | `PgBolt: outcome {Outcome} of job {JobId} discarded: execution is {State} (abandoned or lease lost).` |
| 4002 | `SinkStopCancelled` | Warning | — | `PgBolt: completion sink stop was cancelled; {Count} outcome send(s) aborted.` |
| 4003 | `AckDiscardedStopCancelled` | Warning | — | `PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink stop was cancelled; its lease will expire.` |
| 4004 | `AckPartialAfterRetry` | Information | — | `PgBolt: ack affected {Acked} of {Count} jobs after a failed attempt; the rest was probably already applied.` |
| 4005 | `AckPartialLeaseLost` | Warning | — | `PgBolt: ack affected {Acked} of {Count} jobs; lease of the rest was lost.` |
| 4006 | `OutcomeLeaseNotFoundAfterRetry` | Information | — | `PgBolt: outcome {Outcome} of job {JobId}: lease not found after a failed attempt; the outcome was probably already applied.` |
| 4007 | `OutcomeDiscardedLeaseLost` | Warning | — | `PgBolt: outcome {Outcome} of job {JobId} discarded: lease was lost.` |
| 4008 | `OutcomeNotSentExecutionState` | Debug | — | `PgBolt: outcome of job {JobId} is not sent: execution is {State}.` |
| 4009 | `LeaseFreedNotificationFailed` | Error | yes | `PgBolt: lease-freed notification for queue {Queue} failed.` |
| 4010 | `OutcomeSendFailedRetrying` | Warning | yes | `PgBolt: sending outcome of {Count} job(s) failed; retrying in {Delay}.` |
| 4011 | `OutcomeNotSentStopCancelled` | Warning | yes | `PgBolt: outcome of job {JobId} was not sent before stop was cancelled; its lease will expire.` |
| 4012 | `OutcomeSendBudgetExceeded` | Error | yes | `PgBolt: outcome of job {JobId} was not sent within {Budget}; the execution is dropped, its lease will expire.` |

### 4100–4199: Leases and heartbeats (`LeaseKeeper`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 4100 | `LeaseRenewalNotConfirmed` | Warning | — | `PgBolt: lease renewal of job {JobId} (lease {LeaseId}) is not confirmed; cancelling the execution.` |
| 4101 | `HeartbeatTimedOut` | Warning | yes | `PgBolt: heartbeat of server {ServerId} timed out after {Timeout}; {Count} leases not renewed.` |
| 4102 | `HeartbeatFailed` | Warning | yes | `PgBolt: heartbeat of server {ServerId} failed; {Count} leases not renewed.` |
| 4103 | `JobCancellationRequested` | Information | — | `PgBolt: cancellation of job {JobId} requested; cancelling the execution.` |
| 4104 | `LeaseLostExecutionCancelled` | Warning | — | `PgBolt: lease {LeaseId} of job {JobId} is lost; the execution is cancelled and its outcome will be dropped.` |
| 4105 | `LeaseLostOutcomeDropped` | Warning | — | `PgBolt: lease {LeaseId} of job {JobId} is lost; pending outcome {Outcome} is dropped.` |
| 4106 | `HeartbeatRecovered` | Information | — | `PgBolt: heartbeat of server {ServerId} recovered.` |
| 4107 | `HeartbeatRecoveryCallbackFailed` | Error | yes | `PgBolt: heartbeat recovery callback of server {ServerId} failed.` |
| 4108 | `ServerReRegistered` | Warning | — | `PgBolt: servers row of server {ServerId} was missing (removed by cleanup); registered again.` |
| 4109 | `ServerReRegistrationFailed` | Warning | yes | `PgBolt: re-registering server {ServerId} failed; retrying on the next heartbeat.` |
| 4110 | `LeaseDeadlineCheckFailed` | Error | yes | `PgBolt: lease deadline check of server {ServerId} failed.` |

### 4200–4299: Outcome statistics (`OutcomeStatsFlusher`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 4200 | `OutcomeStatsFlushFailed` | Warning | yes | `PgBolt: flushing outcome statistics failed; values are kept for the next flush.` |

### 5000–5099: Maintenance (`ProcessingMaintenance`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 5000 | `BatchCompletionNotCounted` | Debug | — | `PgBolt: completion of a member of batch {BatchId} is not counted: the batch row was removed by retention.` |
| 5001 | `ResolverLagging` | Warning | — | `PgBolt: the oldest workflow completion signal is {Age} old (ResolverInterval {Interval}); continuations are delayed. Check that a process with RunMaintenance = true is running and that no long user transaction holds workflow rows (After).` |
| 5002 | `RecurringFireFailed` | Error | — | `PgBolt: recurring job {Id} was not fired: {Error}; retrying in {Delay}.` |
| 5003 | `RecurringMisfireSkipped` | Warning | — | `PgBolt: recurring job {Id} skipped fires from {From:O} to {To:O} by its misfire policy.` |
| 5004 | `RecurringOverlapSkipped` | Debug | — | `PgBolt: recurring job {Id} fire skipped: previous run is still active.` |
| 5005 | `RecurringWaitingForPrevious` | Debug | — | `PgBolt: recurring job {Id} waits for its previous run (catch-up).` |
| 5006 | `RecurringExhausted` | Warning | — | `PgBolt: recurring job {Id} has no future occurrences and will not fire again.` |
| 5007 | `MaintenanceTaskFailed` | Error | yes | `PgBolt: maintenance task {Task} failed.` |
| 5008 | `FuturePartitionsLockTimeout` | Warning | — | `PgBolt: future partitions were not created (lock_timeout); retrying in {Delay}.` |
| 5009 | `TodayPartitionsMissing` | Error | — | `PgBolt: today's partitions are missing and could not be created (lock_timeout); enqueue fails until they exist. Retrying in {Delay}.` |
| 5010 | `DeadServersRemoved` | Information | — | `PgBolt: removed {Count} servers rows without heartbeat for {Timeout}.` |
| 5011 | `MaintenanceLoopIterationFailed` | Error | yes | `PgBolt: {Loop} iteration failed; retrying after the next interval.` |
| 5012 | `QueueStatsReadFailed` | Warning | yes | `PgBolt: reading queue statistics failed; the previous snapshot is kept.` |
| 5013 | `TableStatsReadFailed` | Warning | yes | `PgBolt: reading table statistics failed; the previous values are kept.` |

### 5100–5199: Retention (`RetentionReport`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 5100 | `RetentionPartitionsDropped` | Information | — | `PgBolt retention: dropped {JobsCount} jobs partitions [{JobsDays}], {EventsCount} job_events partitions [{EventsDays}] and {Remnants} compaction remnants.` |
| 5101 | `RetentionBatchesDeleted` | Information | — | `PgBolt retention: deleted {Count} finished batches rows older than JobsRetention.` |
| 5102 | `RetentionPinnedPartitions` | Information | — | `PgBolt retention: {Count} jobs partitions are pinned by live jobs: {Compacted} compacted now, {Small} left as is (few terminal rows), {TooMany} not compactable (more than {Max} live jobs), {Replicated} skipped (logical replication); {Live} live jobs in total, oldest day {Oldest}.` |
| 5103 | `RetentionPartitionTooManyLiveJobs` | Warning | — | `PgBolt retention: jobs partition {Day} holds {Live} live jobs, more than RetentionCompactionMaxLiveJobs ({Max}); it is not compacted and keeps its whole disk space until they finish (overdue by {Overdue} days). Run, cancel or delete these jobs, or raise the limit.` |
| 5104 | `RetentionCompactionSkippedReplication` | Warning | — | `PgBolt retention: compaction of {Count} pinned jobs partitions is skipped because jobs is in a logical replication publication; they keep their disk space until their live jobs finish (oldest day {Oldest}).` |
| 5105 | `RetentionTooManyAttachedPartitions` | Warning | — | `PgBolt retention: {Attached} jobs partitions are attached, normally {Normal}; days pinned by live jobs stay attached and slow down dequeue planning.` |
| 5106 | `RetentionHoldTimeout` | Warning | — | `PgBolt retention: holding the lock on {Table} hit statement_timeout or idle_in_transaction_session_timeout ({Count} partitions deferred); the hot path waited up to the hold limit. Likely a slow disk or a process pause (GC).` |
| 5107 | `RetentionDeferredByLockTimeout` | Warning / Debug | — | `PgBolt retention: {Count} {Table} partitions were deferred by lock_timeout (overdue by up to {Overdue} days); long queries or a holder of the partition itself (autovacuum, direct query) block their removal.` |
| 5108 | `RetentionRemnantDropFailed` | Error | — | `PgBolt retention: dropping the detached {Table} table of {Day} failed: {Error}; it will be retried as a remnant.` |

### 6000–6099: Schema initialization (`PgBoltInitializer`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 6000 | `SchemaVersionNewer` | Warning | — | `PgBolt: schema '{Schema}' has version {Version}, newer than this build knows ({Latest}); continuing because migrations are additive.` |
| 6001 | `StartupFuturePartitionsLockTimeout` | Warning | — | `PgBolt: future partitions were not created (lock_timeout); retrying in background.` |
| 6002 | `BackgroundPartitionCreationFailed` | Error | yes | `PgBolt: background partition creation failed.` |

### 6100–6199: Recurring job synchronization

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 6100 | `RecurringJobInserted` | Information | — | `PgBolt: recurring job {Id} of owner {Owner} inserted.` |
| 6101 | `RecurringJobUpdated` | Information | — | `PgBolt: recurring job {Id} of owner {Owner} updated ({Fields}).` |
| 6102 | `RecurringJobDeleted` | Information | — | `PgBolt: recurring job {Id} of owner {Owner} deleted (no longer declared).` |
| 6103 | `RecurringJobOwnerCollision` | Error | — | `PgBolt: recurring job {Id} declared by owner {Owner} belongs to another owner or was created through the API; left untouched.` |
| 6104 | `RecurringSyncDeadlockRetry` | Warning | yes | `PgBolt: recurring sync of owner {Owner} hit a deadlock (40P01); retrying the transaction (attempt {Attempt} of {MaxAttempts}).` |

### 7000–7099: Dashboard startup checks

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 7000 | `ActionsPolicyNotRegistered` | Error | — | `PgBolt dashboard: ActionsPolicy '{Policy}' is not registered: every dashboard request fails until AddAuthorization defines it.` |
| 7001 | `AnonymousRemoteAccessAllowed` | Warning | — | `PgBolt dashboard allows anonymous remote access (AllowAnonymousRemote = true).` |
| 7002 | `LocalRequestsOnly` | Warning | — | `PgBolt dashboard is reachable only from local requests: add RequireAuthorization(...) to the dashboard group or set AllowAnonymousRemote.` |

### 7100–7199: Dashboard actions and errors

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 7100 | `DashboardActionPerformed` | Information | — | `PgBolt dashboard: {User} {Action} {Target}: {Result}.` |
| 7101 | `DashboardRequestFailed` | Error | yes | `PgBolt dashboard: {Method} {Path} failed.` |

### 8000–8999: Telemetry (category `PgBolt.Telemetry`)

| Id | Name | Level | Exception | Template |
|---|---|---|---|---|
| 8001 | `TelemetryFailed` | Warning | yes | `PgBolt: telemetry failed; the signal is skipped.` |
| 8002 | `MetricCallbackFailed` | Warning | yes | `PgBolt: metric callback failed; the measurement is skipped.` |

## PromQL examples

These examples use the translated Prometheus label names (`pgbolt.queue` becomes `pgbolt_queue`).

Throughput by queue and outcome

```promql
sum by (pgbolt_queue, pgbolt_job_outcome) (rate(pgbolt_job_duration_seconds_count[5m]))
```

p99 wait time (use `pgbolt_job_duration_seconds_bucket` for execution time)

```promql
histogram_quantile(0.99, sum by (le, pgbolt_queue) (rate(pgbolt_job_wait_time_seconds_bucket[5m])))
```

Worker pool saturation

```promql
sum by (pgbolt_queue) (pgbolt_worker_active) / on() group_left sum(pgbolt_worker_capacity)
```

Saturation of a queue's cluster-wide limit (use `sum` for pod metrics and `max` for the cluster metric)

```promql
sum by (pgbolt_queue) (pgbolt_worker_active) / max by (pgbolt_queue) (pgbolt_cluster_queue_limit)
```

Stuck queue with a ready job waiting longer than 5 minutes

```promql
max by (pgbolt_queue) (pgbolt_cluster_queue_oldest_ready_age_seconds) > 300
```

Lease loss

```promql
sum(rate(pgbolt_job_outcome_dropped_total{pgbolt_drop_reason="lease_lost"}[5m])) > 0
```

A stale `queue_stats` snapshot means no pod with `RunMaintenance = true` is taking snapshots or snapshot creation keeps failing.
Use this alert when pods with servers are running. Idle queues retain a zero-valued series, so an empty queue does not trigger a stale snapshot alert.

```promql
absent(pgbolt_cluster_queue_jobs)
```

## Overhead

Without listeners (`ActivityListener`, `MeterListener`), the only per-job telemetry allocation is the logging scope, one 32-byte `JobLogScope` object.
The `benchmarks/PgBolt.Server.Benchmarks` benchmark measures one successful no-op `JobRunner.RunAsync` call without database or network access.
For results and a comparison with the implementation before telemetry, see the [benchmark README](../benchmarks/PgBolt.Server.Benchmarks/README.md).
