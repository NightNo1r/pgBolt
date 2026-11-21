# Changelog

## Unreleased

### Breaking changes

- Observability (part 7a) adds `queue_stats.oldest_ready_seconds` to the initial migration `V001`. Schemas from earlier builds cannot be upgraded.
  Startup fails with a request to recreate the schema (`DROP SCHEMA … CASCADE`).
- The public `PgBolt.Postgres.Storage.ActiveQueueSetting` replaces `bool Limited` with `int? Limit`, the effective queue limit (0 when paused).
  Use `Limit is not null` for the previous value of `Limited`.
- Cluster limits and queue weights (part 3b) add `queue_settings` to the initial migration `V001` and replace the `queue_owner (lease_owner)`
  index with `queue_leased (queue)` using the same predicate. Schemas from part 6 builds cannot be upgraded.
  Startup fails with a request to recreate the schema (`DROP SCHEMA … CASCADE`).
- The dashboard (part 6) adds `stats_minutes`, `queue_stats`, and the indexes `queue_awaiting_list`, `job_events_failed`,
  `job_events_cancelled`, `jobs_parent`, `jobs_recurring`, and `batches_parent` to the initial migration `V001`.
  Schemas from part 5 builds cannot be upgraded. Startup fails with a request to recreate the schema (`DROP SCHEMA … CASCADE`).
- `PgBoltStore.AckAsync` now returns the acknowledged job IDs as an `IReadOnlyList<Guid>` instead of a count.
- `PgBoltProcessingOptions.RunMaintenance = true` also starts the `stats` task, which snapshots `queue_stats` every `StatsSnapshotInterval`
  and applies retention to `stats_minutes`.
- Workflows (part 5) add `batches`, `completions`, and columns on `queue` and `jobs` to the initial migration `V001`.
  Schemas from part 4 builds cannot be upgraded. Startup fails with a request to recreate the schema (`DROP SCHEMA … CASCADE`).
- `JobContext.BatchId` changes from `Guid?` to `BatchId?`. The constructor gains an optional `parentBatchId` parameter.
  PgBolt sets `JobContext.ParentId` only for continuations whose parent is a job.
- Custom implementations and mocks of `IJobClient` must implement the new `Workflow`, `CancelBatchAsync`, and `GetBatchAsync` members.
- PgBolt no longer uses `jobs.state = 1`. It derives `JobState.Awaiting` from a `queue` row with `await_condition`.
- `PgBoltProcessingOptions.RunMaintenance = true` also starts the workflow resolver. Only processes with `AddPgBoltServer` and
  `RunMaintenance = true` resolve continuations and batches.
- `PgBolt.Postgres.Client.IWakeupSignal` replaces `Task<bool> WaitAsync(IReadOnlyCollection<string> queues, TimeSpan timeout, CancellationToken ct)`
  with `IDisposable Subscribe(string queue, Action callback)`. Custom implementations (for example, Redis pub/sub) must implement `Subscribe`.
  `Signal` invokes the callback synchronously, so keep it cheap and nonblocking. Call `Dispose` on the result to unsubscribe.
- `PgBolt.Postgres.Storage.PgBoltStore.HeartbeatAsync(Guid serverId, TimeSpan lease, CancellationToken ct)` is now
  `HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)`.
  Only listed leases are renewed. The sweeper handles server-owned rows omitted from the list.
- `PgBolt.Postgres.Hosting.IPgBoltMaintenance` changes `EnsurePartitionsAsync(int daysAhead = 7, …)` to `EnsurePartitionsAsync(int? daysAhead = null, …)`.
  A value of `null` selects `PgBoltOptions.PartitionDaysAhead`. Custom implementations and mocks must add the new `ApplyRetentionAsync` and
  `RemoveDeadServersAsync` members. Calls with an explicit `int` are source-compatible but binary-incompatible with earlier builds.
- `PgBoltStore.HeartbeatAsync` now returns `HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases)` instead of
  `IReadOnlyList<HeartbeatLease>`.
- **Retention is enabled by default** (`JobsRetention` and `EventsRetention` both default to 7 days). On the first maintenance run after upgrade, retention
  drops expired `jobs` and `job_events` partitions and compacts pinned partitions. To preserve history, set the retention periods
  before upgrading, or set them to `null` in code.
- Migrations are consolidated into the initial `V001`, including `maintenance_tasks` and part 4 objects. `PgBoltMigrations.LatestVersion = 1`.
  Schemas created by earlier builds cannot be upgraded. Startup fails with a request to recreate the schema (`DROP SCHEMA … CASCADE`).
- PgBolt rejects `EnqueueOptions.UniqueKey` and `uniqueKeySelector` results starting with `pgbolt:` by throwing `ArgumentException`.
  The prefix is reserved for internal keys.
- `PgBoltProcessingOptions.RunMaintenance = false` no longer disables all processing. `RunScheduler` (default `true`) controls the recurring scheduler.

### Added

- Observability (part 7a) includes tracing, metrics, and logs. See the [observability guide and PromQL examples](docs/observability.md).
  - The `PgBolt` `ActivitySource` emits producer spans for submission (`enqueue {job.type}`, `workflow submit`, `recurring trigger`).
    Each execution has a consumer span whose parent is the producer span recorded in `jobs.trace_parent`.
  - The `PgBolt` `Meter` publishes pod metrics (`pgbolt.job.enqueued`, `pgbolt.recurring.fired`, `pgbolt.job.duration`,
    `pgbolt.job.wait_time`, `pgbolt.job.outcome.dropped`, `pgbolt.worker.active`, `pgbolt.worker.capacity`, `pgbolt.storage.errors`,
    `pgbolt.maintenance.runs`, `pgbolt.maintenance.duration`, `pgbolt.retention.deferred`) and cluster metrics (`pgbolt.cluster.queue.jobs`,
    `pgbolt.cluster.queue.oldest_ready_age`, `pgbolt.cluster.queue.limit`).
  - Logs include job lifecycle events (`EventId` 3001–3008 in the `PgBolt.Job` category), a job scope (`JobId`, `JobType`, `Queue`, `Attempt`),
    telemetry events 8001 / 8002, `SlotReleaseFailed` (1112), and `QueueStatsReadFailed` (5012). All log calls use `[LoggerMessage]` with
    stable `EventId` and `EventName` values. Existing message text and categories are preserved.
  - `PgBoltDiagnostics` in `PgBolt.Abstractions` exposes source, `Meter`, and instrument names, plus histogram bucket boundaries.
  - The `PgBolt.OpenTelemetry` package adds `AddPgBoltInstrumentation()` to `TracerProviderBuilder` and `MeterProviderBuilder` for subscriptions
    and bucket boundaries. It depends on OpenTelemetry 1.15.3, upgraded from 1.12.0 to address GHSA-g94r-2vxg-569j in `OpenTelemetry.Api` 1.12.0.
  - `LeasedJob.WaitSeconds` (last argument, default 0) measures time from `run_at` until lease acquisition. Queue snapshots include `queue_stats.oldest_ready_seconds`.
  - The `JobRunner` benchmark (`benchmarks/PgBolt.Server.Benchmarks`) measures per-job telemetry overhead. Without listeners, the only additional
    allocation is the logging scope (≈ 32 bytes). There is no timing threshold. The benchmark README describes results and measurement limits.
- Cluster metrics `pgbolt.cluster.table.dead_tuples` (`n_dead_tup`) and `pgbolt.cluster.table.size` (`pg_total_relation_size`) track bloat, with
  `pgbolt.table` = `queue` / `completions` (constants `PgBoltDiagnostics.Instruments.ClusterTableDeadTuples` and `ClusterTableSize`).
  The `stats` loop reads them on every pod with servers. Read errors increment `pgbolt.storage.errors` under `table_stats_read`.
  For alert thresholds, see the [observability guide](docs/observability.md).
- The `benchmarks/PgBolt.LoadTest` harness runs `sustained`, `ramp`, `partitions`, and `vacuum` scenarios directly and via PgBouncer, with profile B
  (10,000 jobs/s) and SLO reports in `results/`. The tests retained the `queue` reloptions in `V001`. [Results, decisions, and configuration guidance](docs/performance.md)
- `QueueOptions.ClusterMaxConcurrency` (`null` or ≥ 1) limits the number of jobs that can hold a lease simultaneously in a queue across the cluster.
  A limited dequeue serializes with `pg_advisory_xact_lock` and counts leased rows. Its batch uses one implicit transaction and one round trip,
  including through PgBouncer in transaction mode. Unlimited queues retain their existing dequeue behavior. While a queue is at its limit,
  the server polls it only on schedule or after committing an outcome for one of its own jobs in that queue. Enqueue signals and freed slots do not trigger a poll.
- `QueueOptions.Weight` (`null` or 1…1000) is a cluster-wide queue setting. Each server divides its shared worker pool by queue weight using
  stride scheduling. With no weights, the server still follows strict declaration order. Weight changes take effect at a fetch-plan boundary.
- `AddPgBolt` registers `IQueueSettingsManager`. Its `ListAsync` and `GetAsync` methods return `QueueSettings`, including code defaults, overrides,
  who set them and when, `EffectiveMaxConcurrency`, `Paused`, and the effective weight. Use `SetConcurrencyOverrideAsync` (`null` for unlimited,
  0 for paused, ≥ 1 for a limit), `ResetConcurrencyOverrideAsync`, `SetWeightOverrideAsync` (`null` for no weight, otherwise 1…1000),
  `ResetWeightOverrideAsync`, and `InvalidateCache`. Overrides take precedence over code defaults. Concurrency and weight have independent overrides.
- `PgBoltOptions.QueueSettingsCacheDuration` sets the `IQueueSettingsManager` cache TTL. The default `null` disables caching.
  Other values must be > 0 and ≤ 1 day. `PgBoltProcessingOptions.QueueSettingsRefreshInterval` controls settings refreshes in a process with a server
  (10 s ± 10%, > 0 and ≤ 1 day).
- At startup, each process with a server publishes its queues' `ClusterMaxConcurrency` and `Weight` values in `queue_settings` with
  `published_by` = `{MachineName}/{ProcessId}`. Different values for the same queue on servers in one process cause startup to fail.
  A conflict with another recently started process logs a Warning. The last writer wins.
- `PgBoltStore` adds `DequeueLimitedAsync` returning `LimitedDequeueResult(Jobs, LimitReached)`, `SyncQueueDefaultsAsync`
  (`QueueDefault` → `QueueDefaultConflict`), and `GetActiveQueueSettingsAsync` returning `ActiveQueueSetting`.
- Generator diagnostic BOLT010 flags conflicting constant `ClusterMaxConcurrency` or `Weight` values for one queue across
  `PgBoltServerOptions.Queue` calls in a project. BOLT011 flags an invalid constant queue option or `QueueSettingsCacheDuration`.
- The dashboard's queues page shows Limit (leased / limit or Paused) and Weight, including whether values came from code or an override
  and who set the override and when. Actions set a limit, remove a limit, pause, reset to the code value, set or remove a weight,
  or clear the settings cache. Queues with overrides remain visible even without jobs or servers.
- If a queue first acquires a limit after process startup (through an override or another process), the process applies it within
  `QueueSettingsRefreshInterval`, starting with the first fetch after the snapshot refresh. Earlier dequeues ignore the new limit.
  Changes to an existing limit, including pausing the queue, take effect immediately.
- The `PgBolt.Dashboard` package exposes `services.AddPgBoltDashboard(...)` and `app.MapPgBoltDashboard("/pgbolt")` (`RouteGroupBuilder`).
  It provides an overview with counters and hourly and daily throughput, queue views, job lists by state (keyset pagination and incremental scans bounded by `ScanBudget`),
  job and batch details, batch lists, recurring jobs with run history, servers with maintenance tasks, and job types. Actions include cancel / trigger / requeue
  (including up to 100 selected jobs), batch cancellation, and pause / resume / trigger / delete for schedules. Users can run a job by name with a form derived
  from `job_types.args_schema`. The dashboard requires no `AddRazorComponents`, `UseAntiforgery`, or static files.
- Without authorization on the dashboard route group, the dashboard accepts only local requests without proxy headers. `AllowAnonymousRemote` lifts this restriction.
  Service meshes with a loopback sidecar that sends no proxy headers require group authorization. The dashboard supports `ActionsPolicy` and `ReadOnly`.
  It checks `HX-Request` and `Sec-Fetch-Site` / `Origin` for CSRF protection. Behind a TLS proxy, the host applies `ForwardedHeaders`.
- Statistics include confirmed server outcome counters (`stats_minutes`, flushed every `StatsFlushInterval` = 15 s and once more at shutdown)
  and snapshots of live job states (`queue_stats`, `StatsSnapshotInterval` = 10 s). `StatsRetention` defaults to 7 days.
- `QueryTimeout` limits the dashboard's own queries. Detail pages and post-action reads use `IJobClient` / `IRecurringJobManager` primary-key
  lookups outside this timeout.
- Workflows (part 5) add the fluent `IJobClient.Workflow()` API with `Enqueue`, `StartBatch`, `After(JobId | BatchId)`, `ContinueWith`,
  `ContinueWithBatch` (`ContinuationCondition.OnSuccess` / `OnAnyFinished`), and `EnqueueMany` in a batch body. `SubmitAsync` uses one transaction
  (a savepoint with `WorkflowOptions.Transaction`). Workflows support nested batches and batch continuations, `CancelBatchAsync`, and `GetBatchAsync`
  (`BatchInfo`, `BatchState`, `BatchCancelResult`). They add `ParentNotFoundException`, `UniqueKeyConflictException`, `JobInfo.ParentId`,
  `ParentBatchId`, `BatchId`, `Continuation`, `JobState.Awaiting` for waiting jobs, and `JobContext.ParentBatchId`.
- `PgBolt.Postgres.Client.ICompletionSignal` / `InProcessCompletionSignal` wake the resolver. `PgBoltProcessingOptions.ResolverInterval` defaults to 1 s.
  The `PgBolt.Server` maintenance loop runs the resolver to account for signals, traverse children, and cancel members. It logs a Warning when signals lag.
- Retention preserves parents with waiting continuations and Failed/Cancelled members of existing batches.
  It deletes `batches` rows older than `JobsRetention` from the top down (`RetentionResult.DeletedBatches`).
- The generator registers metadata for typed workflow steps (`Enqueue<TArgs>`, `ContinueWith<TArgs>`, `EnqueueMany<TArgs>`) and applies BOLT001 / BOLT006
  to those steps. New diagnostic BOLT009 (Warning) flags methods that construct a workflow without calling `SubmitAsync`.
- The `PgBolt.Server` worker runtime adds `AddPgBoltServer`, `AddPgBoltJobFilter`, `ConfigurePgBoltProcessing`, `RetryPolicy`, `IJobFilter`, and `JobExecutorNotFoundException`.
- `PgBolt.Abstractions` adds `JobAttribute.TimeoutSeconds`, `JobDescriptor.TimeoutSeconds`, `JobAbortedException`, and `JobArgumentsException`.
  Generator diagnostics include BOLT007 (error for a negative `TimeoutSeconds`) and BOLT008 (warning for `TimeoutSeconds` above the .NET timer
  limit of 4294967 s, in which case the server caps the timeout at that limit).
- `PgBoltStore` adds `ReleaseAsync`, `RegisterServerAsync`, `RemoveServerAsync`, and `GetJobTypeNamesByQueuesAsync`.
- Storage maintenance (part 2) adds the `maintenance_tasks` table in the initial migration `V001`. The `PgBolt.Server` maintenance loop runs
  `EnsurePartitionsAsync` periodically, applies retention with compaction for partitions pinned by live jobs, and cleans up `servers`.
  It coordinates tasks across the cluster through `maintenance_tasks`.
- `PgBoltOptions` adds `JobsRetention`, `EventsRetention`, `RetentionLockTimeout`, and `RetentionCompactionMaxLiveJobs`.
- `PgBoltProcessingOptions` adds `MaintenancePollInterval`, `PartitionsInterval`, `ServersCleanupInterval`, `RetentionInterval`, and `DeadServerTimeout`.
- Added `RetentionResult`, `PinnedPartition`, `DeferredPartition`, and `DeferReason`, plus `PgBoltStore.TryClaimMaintenanceAsync`, `CompleteMaintenanceAsync`, and `RemoveDeadServersAsync`.
- Recurring jobs (part 4) add `IRecurringJobManager` (`AddOrUpdateAsync`, `RemoveAsync`, `PauseAsync`, `ResumeAsync`, `TriggerAsync`, `GetAsync`),
  code-first `PgBoltOptions.AddRecurring` / `ClearRecurring` / `RecurringOwner`, and time-zone-aware cron schedules (Cronos with seconds).
  Policies include `MisfirePolicy` (`FireOnce`, `Skip`, `CatchUp` with `CatchUpWindow`) and `OverlapPolicy` (`Skip`, `Allow`).
  New members include `JobContext.ScheduledAt`, `JobContext.RecurringJobId`, `JobInfo.ScheduledAt`, `JobInfo.RecurringJobId`,
  `PgBoltProcessingOptions.RunScheduler`, `SchedulerInterval`, `MisfireThreshold`, and `RecurringSyncInterval`.
- The generator also registers metadata for a `[Job]` type at `AddRecurring<TArgs>` / `IRecurringJobManager.AddOrUpdateAsync<TArgs>` call sites
  (for producers without an executor). As for `EnqueueAsync`, the analyzer checks JSON context coverage (BOLT001 / BOLT006).

### Changed

- When a `jobs` row is missing, both standard and limited dequeue roll back the entire batch. The PostgreSQL error includes the missing job ID.
  With `AutoMigrate` disabled, apply migration V002 before upgrading using `PgBoltMigrations.GetScript(schema, fromVersion: 1)`.
- Observability (part 7a) changes trace propagation, snapshot reads, and outcome handling.
  - `jobs.trace_parent` now stores the producer span ID, falling back to the ID of `Activity.Current` as before. `IRecurringJobManager.TriggerAsync`
    uses the same rule for `trace_parent` (previously `NULL`).
  - The `stats` loop runs on every pod with servers and reads the `queue_stats` snapshot for cluster metrics. PgBolt creates `ProcessingMaintenance`
    whenever servers are present, regardless of flags. Only a pod with `RunMaintenance = true` creates the snapshot.
  - An exception from `JobRunner.RunAsync` or from outcome delivery releases the slot and lease. `PgBoltServer.RunExecutionAsync` no longer throws.
    It removes the execution from the table before the first log call and counts it in `pgbolt.job.outcome.dropped` with reason `abandoned`.
- Observability fixes cover attempt timing, telemetry failures, and counter attribution.
  - `JobRunner` captures duration and outcome reason before classification. Time spent in `RetryPolicy` is excluded from attempt duration,
    and cancellation during the policy cannot change the reason. Without a listener on the `PgBolt` source, the runner skips parsing `trace_parent`.
  - `ProducerSpan` restores `Activity.Current` before logging a telemetry failure and builds the span name only when a listener exists.
  - `pgbolt.storage.errors` excludes logger failures after a database response and shutdown exceptions.
  - `LeaseKeeper.Unregister` records `pgbolt.job.outcome.dropped` with reason `abandoned` once per execution.
- The executor invoker wraps argument deserialization failures (`JsonException`, `NotSupportedException`, or `null` instead of an object) in `JobArgumentsException`.
- `job_types.published_by` identifies the publishing process as `{MachineName}/{ProcessId}`.
- Server registration uses an upsert. If cleanup deletes the `servers` row, the next heartbeat restores it.
- A schema version newer than the code logs a Warning and allows startup to continue. `EnsurePartitionsAsync` skips migrations for a newer schema.
- `UsePreparedStatements` works with PgBouncer transaction pooling and `max_prepared_statements > 0`. PgBolt now always adds `No Reset On Close=true`
  to `ConnectionString`. Previously, it added the setting only with prepared statements disabled, and the `DISCARD ALL` session reset on pool return
  caused `prepared statement "…" does not exist`.
- Known limitations include compaction that is not MVCC-safe for REPEATABLE READ/SERIALIZABLE snapshots taken before compaction.
  Disable it with `RetentionCompactionMaxLiveJobs = 0` if needed. PgBolt skips compaction when `jobs` belongs to a logical replication publication.
  Pinned days stay attached until their live jobs leave.
