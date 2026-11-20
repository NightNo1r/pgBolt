using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Hosting;

/// <summary>
/// Обслуживание схемы (core §4.4, часть 2). Все операции идемпотентны и безопасны при параллельном вызове. Без PgBolt.Server
/// вызывайте их из своего планировщика: EnsurePartitionsAsync — не реже раза в сутки (при PartitionDaysAhead ≥ 2),
/// ApplyRetentionAsync — раз в час и не чаще после LockTimeout/HoldTimeout.
/// </summary>
public interface IPgBoltMaintenance
{
    /// <param name="daysAhead">null — PgBoltOptions.PartitionDaysAhead.</param>
    Task<PartitionEnsureResult> EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default);

    /// <summary>Удаляет строки servers с heartbeat_at старше olderThan (часть 2 §5.6); строку живого сервера восстановит его heartbeat.</summary>
    Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default);

    /// <summary>
    /// Retention (часть 2 §5.2–§5.5): удаляет партиции jobs/job_events старше JobsRetention/EventsRetention без живых задач,
    /// компактирует закреплённые (не больше RetentionCompactionMaxLiveJobs живых), удаляет остатки прошлых запусков.
    /// LockTimeout/HoldTimeout — не ошибка, а Deferred; повторять не чаще раза в час.
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

    /// <summary>§4: миграция — только если схемы нет или она старше кода; более новая схема не трогается.</summary>
    internal static bool NeedsMigration(bool autoMigrate, int? version) =>
        autoMigrate && PgBoltMigrations.IsOutdated(version);
}
