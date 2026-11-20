using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>
/// Прошлые партиции и строки задач для тестов retention (часть 2 §12). today берётся из БД (сутки UTC); партиции создаются
/// тем же DDL, что PartitionManager. TestDb уже содержит партиции на сегодня и два дня вперёд.
/// </summary>
public abstract class RetentionTestBase(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    protected DateOnly Today { get; private set; }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Today = await Db.ScalarAsync<DateOnly>("SELECT (now() AT TIME ZONE 'UTC')::date");
    }

    protected DateOnly Ago(int days) => Today.AddDays(-days);

    protected static string Suffix(DateOnly day) => day.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
    protected static string Iso(DateOnly day) => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    protected static string J(DateOnly day) => $"jobs_{Suffix(day)}";
    protected static string E(DateOnly day) => $"job_events_{Suffix(day)}";

    protected static Guid IdOn(DateOnly day) =>
        Guid.CreateVersion7(new DateTimeOffset(day.ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero));

    protected async Task CreatePartitionsAsync(DateOnly day, bool jobs = true, bool events = true)
    {
        var lo = UuidV7.LowerBound(day);
        var hi = UuidV7.LowerBound(day.AddDays(1));
        if (jobs)
            await Db.ExecAsync($$"""
                CREATE TABLE {schema}.{{J(day)}} (LIKE {schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = 80);
                ALTER TABLE {schema}.jobs ATTACH PARTITION {schema}.{{J(day)}} FOR VALUES FROM ('{{lo}}') TO ('{{hi}}');
                """);
        if (events)
            await Db.ExecAsync($$"""
                CREATE TABLE {schema}.{{E(day)}} (LIKE {schema}.job_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
                ALTER TABLE {schema}.job_events ATTACH PARTITION {schema}.{{E(day)}}
                    FOR VALUES FROM ('{{Iso(day)}} 00:00:00+00') TO ('{{Iso(day.AddDays(1))}} 00:00:00+00');
                """);
    }

    /// <summary>Терминальные строки суток (state 2 — Succeeded, 3 — Failed, 4 — Cancelled), без строк queue.</summary>
    protected async Task<Guid[]> InsertTerminalAsync(DateOnly day, int count, short state = 2, string args = "{}")
    {
        var ids = Enumerable.Range(0, count).Select(_ => IdOn(day)).ToArray();
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at)
            SELECT id, 'test.job', 'q', 0, @args::jsonb, @state, 1, 3, now(), now() FROM unnest(@ids) AS id
            """, ("ids", ids), ("args", args), ("state", state));
        return ids;
    }

    /// <summary>Живые (Pending) задачи суток — обычным enqueue: строки jobs и queue.</summary>
    protected async Task<Guid[]> EnqueueLiveAsync(DateOnly day, int count, string queue = "q", string args = """{"n":1}""")
    {
        var ids = Enumerable.Range(0, count).Select(_ => IdOn(day)).ToArray();
        await Store.EnqueueAsync(new EnqueueBatch("test.job", queue, 0, 3, null, null, TimeSpan.Zero, ids,
            [.. ids.Select(_ => args)], new DateTime?[count], new string?[count]), null, Ct);
        return ids;
    }

    private protected RetentionRunner Runner(Action<PgBoltOptions>? configure = null, RetentionTuning? tuning = null,
        NpgsqlDataSource? dataSource = null)
    {
        var options = new PgBoltOptions { ConnectionString = "Host=unused", Schema = Db.Schema, JobsRetention = null, EventsRetention = null };
        configure?.Invoke(options);
        return new RetentionRunner(dataSource ?? Db.DataSource, options, tuning ?? RetentionTuning.Default);
    }

    protected Task<bool> ExistsAsync(string table) =>
        Db.ScalarAsync<bool>("SELECT to_regclass(@t) IS NOT NULL", ("t", $"{Db.Schema}.{table}"));

    protected Task<bool> AttachedAsync(string table) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass(@t))", ("t", $"{Db.Schema}.{table}"));

    protected Task<long> OidAsync(string table) =>
        Db.ScalarAsync<long>("SELECT to_regclass(@t)::oid::bigint", ("t", $"{Db.Schema}.{table}"));

    protected Task DetachAsync(string parent, string table) =>
        Db.ExecAsync($"ALTER TABLE {{schema}}.{parent} DETACH PARTITION {{schema}}.{table}");

    /// <summary>
    /// Ждёт, пока не меньше count сессий встанут в очередь блокировки таблицы (pg_locks, granted = false). Проверка oid
    /// в транзакции 2 не блокирует, поэтому ожидающая сессия — это DROP, уже прошедший проверку.
    /// </summary>
    protected Task WaitForLockWaitersAsync(string table, int count) =>
        WaitForLockWaitersAsync(() => Db.ScalarAsync<long>("SELECT count(*) FROM pg_locks WHERE relation = to_regclass(@t) AND NOT granted",
            ("t", $"{Db.Schema}.{table}")), count, table);

    /// <summary>
    /// То же по oid: держатель мог переименовать или удалить таблицу в незакоммиченной транзакции, а ожидающие ждут
    /// блокировку именно этого oid.
    /// </summary>
    protected Task WaitForLockWaitersAsync(long oid, int count) =>
        WaitForLockWaitersAsync(() => Db.ScalarAsync<long>("SELECT count(*) FROM pg_locks WHERE relation = @oid::oid AND NOT granted",
            ("oid", oid)), count, $"oid {oid}");

    private async Task WaitForLockWaitersAsync(Func<Task<long>> waiters, int count, string what)
    {
        var started = Stopwatch.GetTimestamp();
        while (await waiters() < count)
        {
            if (Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10))
                throw new TimeoutException($"Expected {count} lock waiters on {what}.");
            await Task.Delay(20, Ct);
        }
    }

    /// <summary>Держит блокировку в открытой транзакции; Dispose закрывает соединение и откатывает её.</summary>
    protected async Task<NpgsqlConnection> HoldAsync(string table, string lockMode)
    {
        var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var cmd = new NpgsqlCommand(Db.Sql($"BEGIN; LOCK TABLE {{schema}}.{table} IN {lockMode} MODE"), conn);
        await cmd.ExecuteNonQueryAsync(Ct);
        return conn;
    }
}

/// <summary>Записывает вызовы хуков RetentionRunner и выполняет Then.</summary>
internal sealed class HookRecorder
{
    public ConcurrentQueue<RetentionHookContext> Calls { get; } = new();

    public Func<RetentionHookContext, CancellationToken, Task>? Then { get; set; }

    public async Task Hook(RetentionHookContext context, CancellationToken ct)
    {
        Calls.Enqueue(context);
        if (Then is { } then)
            await then(context, ct);
    }

    public int Count(RetentionStep step, string parent) => Calls.Count(c => c.Step == step && c.Parent == parent);

    public RetentionTuning Tuning(int k = 10, TimeSpan? holdStatement = null, TimeSpan? holdIdle = null, string? batchBPrefix = null) =>
        new(k, holdStatement ?? TimeSpan.FromSeconds(2), holdIdle ?? TimeSpan.FromSeconds(1)) { Hook = Hook, BatchBTestPrefix = batchBPrefix };
}
