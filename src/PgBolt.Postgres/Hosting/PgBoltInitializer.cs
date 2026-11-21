using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Hosting;

/// <summary>Checks isolation, applies migrations, and ensures partitions at startup (§1, §4.4, §4.7).</summary>
public sealed partial class PgBoltInitializer : IHostedService
{
    private readonly PgBoltOptions _options;
    private readonly NpgsqlDataSource _dataSource;
    private readonly MigrationRunner _migrations;
    private readonly PartitionManager _partitions;
    private readonly PgBoltStore _store;
    private readonly RecurringDeclarations _recurring;
    private readonly ILogger<PgBoltInitializer> _logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private CancellationTokenSource? _stop;

    internal PgBoltInitializer(PgBoltOptions options, NpgsqlDataSource dataSource, MigrationRunner migrations, PartitionManager partitions,
        PgBoltStore store, RecurringDeclarations recurring, ILogger<PgBoltInitializer> logger)
    {
        _options = options;
        _dataSource = dataSource;
        _migrations = migrations;
        _partitions = partitions;
        _store = store;
        _recurring = recurring;
        _logger = logger;
    }

    internal Task? BackgroundRetry { get; private set; }

    /// <summary>
    /// Signals schema readiness once isolation, migrations, and today's partitions have been checked (Part 3 §8.3). Future partitions are created in
    /// the background. If StartAsync fails or its token is cancelled, the task faults or is cancelled accordingly. PgBolt.Server explicitly awaits
    /// it regardless of hosted service order.
    /// </summary>
    internal Task Ready => _ready.Task;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            await InitializeAsync(cancellationToken);
            _ready.TrySetResult();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _ready.TrySetCanceled(cancellationToken);
            throw;
        }
        catch (Exception e)
        {
            _ready.TrySetException(e);
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stop is null || BackgroundRetry is null)
            return;
        await _stop.CancelAsync();
        try
        {
            await BackgroundRetry.WaitAsync(cancellationToken);
        }
        finally
        {
            // RetryAsync не бросает, поэтому после ожидания задача завершена. Если отменили само ожидание, задача ещё
            // читает токен — CTS не освобождаем (без таймера и регистраций это безопасно).
            if (BackgroundRetry.IsCompleted)
            {
                _stop.Dispose();
                _stop = null;
            }
        }
    }

    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await EnsureReadCommittedDefaultAsync(cancellationToken);

        if (_options.AutoMigrate)
            await _migrations.MigrateAsync(cancellationToken);

        // §4, §9.3: версия новее кода — не ошибка (схему поднял более новый релиз при rolling deploy, миграции аддитивны).
        var version = await _migrations.GetCurrentVersionAsync(cancellationToken);
        // Исторические сборки использовали те же номера, но требуют пересоздания схемы.
        if (version.HasValue)
        {
            await EnsureNotPreReleaseSchemaAsync(cancellationToken);
        }

        if (PgBoltMigrations.IsOutdated(version))
            throw new InvalidOperationException(
                $"PgBolt schema version in '{_options.Schema}' is {version?.ToString() ?? "absent"}, expected {PgBoltMigrations.LatestVersion}. " +
                "Apply PgBoltMigrations.GetScript(...) or enable AutoMigrate.");
        if (version > PgBoltMigrations.LatestVersion)
            Log.SchemaVersionNewer(_logger, _options.Schema, version, PgBoltMigrations.LatestVersion);

        var result = await _partitions.EnsureAsync(_options.PartitionDaysAhead, cancellationToken);
        if (result.TimedOut && !result.TodayPresent)
            throw new InvalidOperationException(
                $"PgBolt could not create today's partitions in '{_options.Schema}' within {_options.PartitionLockTimeout} (lock_timeout). " +
                "Enqueue is impossible without them; retry the start when long-running DDL/locks on jobs are gone.");

        // Часть 4 §6.1: после миграций и партиций, до Ready; ошибка — падение старта. До запуска фонового повтора партиций:
        // иначе при ошибке здесь он остался бы запущенным (хост не вызывает StopAsync после ошибки StartAsync).
        // Категория лога — PgBolt.Postgres.Hosting.PgBoltInitializer: под фильтр «PgBolt.Postgres» (§7) попадает по префиксу.
        if (_recurring.HasSync)
            RecurringSyncLog.Write(_logger, _recurring.Owner,
                await _store.SyncRecurringAsync(_recurring.Owner, _recurring.Definitions, _logger, cancellationToken));

        if (!result.TimedOut)
            return;
        Log.StartupFuturePartitionsLockTimeout(_logger);
        _stop = new CancellationTokenSource();
        BackgroundRetry = RetryAsync(_stop.Token);
    }

    /// <summary>
    /// Hot-path statements outside a caller's transaction rely on READ COMMITTED, the default isolation level (§1). A fresh connection reveals the
    /// effective default_transaction_isolation for the server, database, and role.
    /// </summary>
    private async Task EnsureReadCommittedDefaultAsync(CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT current_setting('transaction_isolation')", conn);
        var level = (string?)await cmd.ExecuteScalarAsync(ct);
        if (level != "read committed")
            throw new InvalidOperationException(
                $"PgBolt requires default_transaction_isolation = 'read committed' for its database/role, but a new connection runs at '{level}'. " +
                "Reset it with ALTER DATABASE/ALTER ROLE ... SET default_transaction_isolation = 'read committed' (or remove it from the connection options).");
    }

    /// <summary>
    /// V001 consolidates the initial migrations. Earlier schemas with version 1 or 2 cannot be upgraded if they lack recurring_jobs, batches,
    /// stats_minutes, queue_stats, queue_settings, or queue_stats.oldest_ready_seconds. A missing table or column identifies such a schema.
    /// </summary>
    private async Task EnsureNotPreReleaseSchemaAsync(CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM unnest(@names) AS n WHERE to_regclass(@schema || '.' || n) IS NULL) " +
            "OR NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass(@schema || '.queue_stats') " +
            "AND attname = 'oldest_ready_seconds' AND NOT attisdropped)", conn);
        cmd.Parameters.AddWithValue("names", new[] { "recurring_jobs", "batches", "stats_minutes", "queue_stats", "queue_settings" });
        cmd.Parameters.AddWithValue("schema", _options.Schema);
        if ((bool)(await cmd.ExecuteScalarAsync(ct))!)
            throw new InvalidOperationException(
                $"PgBolt schema '{_options.Schema}' was created by a pre-release PgBolt build and cannot be upgraded; " +
                $"drop it (DROP SCHEMA {_options.Schema} CASCADE) and restart.");
    }

    private async Task RetryAsync(CancellationToken ct)
    {
        var delay = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(delay, ct);
                if (!(await _partitions.EnsureAsync(_options.PartitionDaysAhead, ct)).TimedOut)
                    return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                Log.BackgroundPartitionCreationFailed(_logger, e);
            }
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromMinutes(1).Ticks));
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 6000, EventName = nameof(SchemaVersionNewer), Level = LogLevel.Warning,
            Message = "PgBolt: schema '{Schema}' has version {Version}, newer than this build knows ({Latest}); continuing because migrations are additive.")]
        internal static partial void SchemaVersionNewer(ILogger logger, string schema, int? version, int latest);

        [LoggerMessage(EventId = 6001, EventName = nameof(StartupFuturePartitionsLockTimeout), Level = LogLevel.Warning,
            Message = "PgBolt: future partitions were not created (lock_timeout); retrying in background.")]
        internal static partial void StartupFuturePartitionsLockTimeout(ILogger logger);

        [LoggerMessage(EventId = 6002, EventName = nameof(BackgroundPartitionCreationFailed), Level = LogLevel.Error,
            Message = "PgBolt: background partition creation failed.")]
        internal static partial void BackgroundPartitionCreationFailed(ILogger logger, Exception e);
    }
}
