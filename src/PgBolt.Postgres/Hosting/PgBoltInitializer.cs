using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Hosting;

/// <summary>Проверка изоляции, миграции и партиции на старте (§1, §4.4, §4.7).</summary>
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
    /// Готовность схемы (часть 3 §8.3): успех — после проверки изоляции, миграций и партиций на сегодня (фоновое создание
    /// будущих партиций не ждётся); исключение — при падении StartAsync; отмена — при отмене его токена.
    /// Координатор PgBolt.Server ждёт её явно, не полагаясь на порядок hosted services.
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
        if (PgBoltMigrations.IsOutdated(version))
            throw new InvalidOperationException(
                $"PgBolt schema version in '{_options.Schema}' is {version?.ToString() ?? "absent"}, expected {PgBoltMigrations.LatestVersion}. " +
                "Apply PgBoltMigrations.GetScript(...) or enable AutoMigrate.");
        if (version > PgBoltMigrations.LatestVersion)
            Log.SchemaVersionNewer(_logger, _options.Schema, version, PgBoltMigrations.LatestVersion);
        await EnsureNotPreReleaseSchemaAsync(cancellationToken);

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
    /// §1: statement'ы горячего пути вне пользовательской транзакции выполняются на уровне изоляции по умолчанию,
    /// а их SQL рассчитан на READ COMMITTED. Новое соединение показывает default_transaction_isolation сервера/БД/роли.
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
    /// Миграции объединены в одну V001, поэтому схемы прошлых сборок (версия 1 или 2, но без части объектов — recurring_jobs,
    /// batches, stats_minutes, queue_stats, queue_settings) и колонок (queue_stats.oldest_ready_seconds) не обновляются. Признак такой
    /// схемы — отсутствие любой из этих таблиц или колонки после миграций и проверки версии.
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
