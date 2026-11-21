using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Hosting;

/// <summary>
/// Maintains the schema (core §4.4, Part 2). Operations are idempotent and safe to run concurrently. Without PgBolt.Server, schedule
/// EnsurePartitionsAsync at least daily when PartitionDaysAhead ≥ 2, and ApplyRetentionAsync hourly. After LockTimeout or HoldTimeout, wait at
/// least an hour before retrying retention.
/// </summary>
public interface IPgBoltMaintenance
{
    /// <param name="daysAhead">Partition creation horizon in days. Null uses PgBoltOptions.PartitionDaysAhead.</param>
    /// <param name="ct">Token for cancelling the operation.</param>
    Task<PartitionEnsureResult> EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default);

    /// <summary>
    /// Deletes rows from servers when the last heartbeat is older than olderThan (Part 2 §5.6). A live server restores its row with its next heartbeat.
    /// </summary>
    Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default);

    /// <summary>
    /// Applies retention (Part 2 §5.2–§5.5). Drops expired jobs and job_events partitions without live jobs, compacts pinned partitions containing
    /// at most RetentionCompactionMaxLiveJobs live jobs, and removes remnants of previous runs. LockTimeout and HoldTimeout produce a Deferred
    /// result. Retry at most once per hour.
    /// </summary>
    Task<RetentionResult> ApplyRetentionAsync(CancellationToken ct = default);
}

internal sealed class PgBoltMaintenance(PgBoltOptions options, MigrationRunner migrations, PartitionManager partitions,
    PgBoltStore store, RetentionRunner retention) : IPgBoltMaintenance
{
    public async Task<PartitionEnsureResult> EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default)
    {
        if (options.AutoMigrate && PgBoltMigrations.IsOutdated(await migrations.GetCurrentVersionAsync(ct)))
            await migrations.MigrateAsync(ct);
        return await partitions.EnsureAsync(daysAhead ?? options.PartitionDaysAhead, ct);
    }

    public Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default) =>
        store.RemoveDeadServersAsync(olderThan, ct);

    public Task<RetentionResult> ApplyRetentionAsync(CancellationToken ct = default) => retention.ApplyAsync(ct);

    /// <summary>Runs migrations only if the schema is missing or older than the code (§4). Leaves newer schemas intact.</summary>
    internal static bool NeedsMigration(bool autoMigrate, int? version) =>
        autoMigrate && PgBoltMigrations.IsOutdated(version);
}
