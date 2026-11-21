namespace PgBolt.LoadTest.Measure;

/// <summary>
/// Totals for a 10-second window. <see cref="Start"/> is relative to run start. <see cref="Enqueued"/> counts jobs whose batch's
/// <c>EnqueueManyAsync</c> call completed successfully during the window. <see cref="CpuFraction"/> is harness process CPU usage as a fraction
/// of all available cores.
/// </summary>
internal sealed record WindowStats(
    int Index, TimeSpan Start, long Completed, long Enqueued, long EnqueueCalls, long EnqueueErrors,
    double EnqueueP99Ms, double CpuFraction, TimeSpan MaxGeneratorWait, long DroppedTokens,
    LatencySummary Immediate, LatencySummary Delayed);

/// <summary>One-second database and PgBouncer sample. <c>null</c> marks a missing reading due to a failed query or unavailable source.</summary>
/// <param name="ReadyBacklog">Ready, unclaimed jobs in <c>queue</c>, capped at rate + 1 to indicate the threshold was exceeded.</param>
/// <param name="QueueTotalBytes"><c>pg_total_relation_size</c> of <c>queue</c>.</param>
/// <param name="MaxWaitMicros">Maximum <c>maxwait</c> across PgBouncer pools.</param>
/// <param name="ClWaiting"><c>cl_waiting</c> for PgBouncer pools.</param>
/// <param name="Connections">Host and producer connections in <c>pg_stat_activity</c>.</param>
/// <param name="Completed">Jobs completed since the start of the run.</param>
/// <param name="Delayed">Delayed <c>queue</c> jobs, counted every 10 seconds. Other samples contain <c>null</c>.</param>
/// <param name="Leased">Leased <c>queue</c> jobs, counted alongside <see cref="Delayed"/>.</param>
/// <param name="QueueIndexBytes"><c>pg_relation_size</c> of each <c>queue</c> index by name.</param>
/// <param name="LockManagerWaits">PgBolt backends waiting on the <c>LockManager</c> LWLock.</param>
/// <param name="Vacuums">Active vacuum operations on relations in the run schema.</param>
/// <param name="QueryTime">Duration of the per-second sampling queries.</param>
/// <param name="CountsTime">Duration of the 10-second count on <c>queue</c>.</param>
/// <param name="DatabaseBytes"><c>pg_database_size</c> of the run database.</param>
internal sealed record Sample(
    TimeSpan At, long? ReadyBacklog, long? QueueTotalBytes, long? MaxWaitMicros, int? ClWaiting, int? Connections,
    long? Completed = null, long? Delayed = null, long? Leased = null,
    TableSample? Queue = null, TableSample? Completions = null,
    IReadOnlyDictionary<string, long>? QueueIndexBytes = null, long? WalBytes = null,
    CheckpointerSample? Checkpointer = null, int? LockManagerWaits = null, int? SvActive = null, int? SvIdle = null,
    int? AutovacuumWorkers = null, IReadOnlyList<VacuumProgress>? Vacuums = null,
    TimeSpan? QueryTime = null, TimeSpan? CountsTime = null, long? DatabaseBytes = null);

/// <summary>A <c>pg_stat_user_tables</c> row.</summary>
internal sealed record TableSample(long LiveTuples, long DeadTuples, long AutovacuumCount, DateTime? LastAutovacuum);

/// <summary><c>pg_stat_checkpointer</c> statistics with times in milliseconds.</summary>
internal sealed record CheckpointerSample(
    long NumTimed, long NumRequested, double WriteTimeMs, double SyncTimeMs, long BuffersWritten);

/// <summary>A <c>pg_stat_progress_vacuum</c> row.</summary>
internal sealed record VacuumProgress(string Relation, string Phase);
