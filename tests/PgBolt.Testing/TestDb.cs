using Npgsql;

namespace PgBolt.Testing;

/// <summary>
/// Изолированная схема на тест. DataSource идёт через выбранный режим, Admin — всегда напрямую. <see cref="CreateInOwnDatabaseAsync"/> —
/// схема в отдельной базе, удаляемой в DisposeAsync.
/// </summary>
public sealed class TestDb : IAsyncDisposable
{
    private readonly string? _ownDatabase;
    private readonly string? _serverConnectionString;

    private TestDb(string schema, ConnectionMode mode, string connectionString, NpgsqlDataSource dataSource, NpgsqlDataSource admin,
        string? ownDatabase = null, string? serverConnectionString = null)
    {
        Schema = schema;
        Mode = mode;
        ConnectionString = connectionString;
        DataSource = dataSource;
        Admin = admin;
        _ownDatabase = ownDatabase;
        _serverConnectionString = serverConnectionString;
    }

    public string Schema { get; }
    public ConnectionMode Mode { get; }
    public string ConnectionString { get; }
    public NpgsqlDataSource DataSource { get; }
    public NpgsqlDataSource Admin { get; }

    public static async Task<TestDb> CreateAsync(PostgresFixture pg, ConnectionMode mode, bool migrate = true, int partitionDaysAhead = 2)
    {
        var schema = "t_" + Guid.NewGuid().ToString("N")[..16];
        var cs = pg.ConnectionString(mode);
        var db = new TestDb(schema, mode, cs, NpgsqlDataSource.Create(cs), NpgsqlDataSource.Create(pg.DirectConnectionString));
        await db.InitializeAsync(migrate, partitionDaysAhead);
        return db;
    }

    /// <summary>
    /// Схема в новой базе того же сервера (только напрямую: PgBouncer знает одну базу). Горизонт xmin для обычных таблиц
    /// считается по backend'ам своей базы, поэтому открытые снимки и ANALYZE параллельных тестов и autovacuum'а в общей
    /// базе не мешают page pruning'у таблиц этой. DisposeAsync удаляет базу (WITH (FORCE)).
    /// </summary>
    public static async Task<TestDb> CreateInOwnDatabaseAsync(PostgresFixture pg, int partitionDaysAhead = 2)
    {
        var database = "db_" + Guid.NewGuid().ToString("N")[..16];
        await using (var server = NpgsqlDataSource.Create(pg.DirectConnectionString))
        await using (var create = server.CreateCommand($"CREATE DATABASE {database}"))
            await create.ExecuteNonQueryAsync();

        var cs = new NpgsqlConnectionStringBuilder(pg.DirectConnectionString) { Database = database }.ConnectionString;
        var db = new TestDb("t_" + Guid.NewGuid().ToString("N")[..16], ConnectionMode.Direct, cs, NpgsqlDataSource.Create(cs),
            NpgsqlDataSource.Create(cs), database, pg.DirectConnectionString);
        try
        {
            await db.InitializeAsync(migrate: true, partitionDaysAhead);
        }
        catch
        {
            await db.DisposeAsync();
            throw;
        }
        return db;
    }

    private async Task InitializeAsync(bool migrate, int partitionDaysAhead)
    {
        if (!migrate)
        {
            await ExecAsync("CREATE SCHEMA {schema}");
            return;
        }
        await new PgBolt.Postgres.Schema.MigrationRunner(Admin, Schema).MigrateAsync();
        if (partitionDaysAhead >= 0)
            await new PgBolt.Postgres.Schema.PartitionManager(Admin, Schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(partitionDaysAhead);
    }

    public string Sql(string text) => text.Replace("{schema}", Schema, StringComparison.Ordinal);

    public async Task ExecAsync(string sql, params (string Name, object? Value)[] ps)
    {
        await using var cmd = Admin.CreateCommand(Sql(sql));
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<T> ScalarAsync<T>(string sql, params (string Name, object? Value)[] ps)
    {
        await using var cmd = Admin.CreateCommand(Sql(sql));
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        var result = await cmd.ExecuteScalarAsync();
        return result is DBNull or null ? default! : (T)result;
    }

    /// <summary>Сессии, ждущие блокировку, чей текущий statement упоминает {schema}.table (pg_stat_activity).</summary>
    public Task<long> LockWaitersAsync(string table = "") =>
        ScalarAsync<long>("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE @p",
            ("p", $"%{Schema}.{table}%"));

    /// <summary>Ждёт (до 10 с), пока не меньше count сессий встанут в ожидание блокировки по <see cref="LockWaitersAsync"/>.</summary>
    public async Task WaitForLockWaitersAsync(string table = "", int count = 1, CancellationToken ct = default)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        while (await LockWaitersAsync(table) < count)
        {
            if (System.Diagnostics.Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10))
                throw new TimeoutException($"Expected {count} sessions waiting for a lock in {Schema}.{table}.");
            await Task.Delay(20, ct);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DataSource.DisposeAsync();
        if (_ownDatabase is null)
        {
            await ExecAsync("DROP SCHEMA IF EXISTS {schema} CASCADE");
            await Admin.DisposeAsync();
            return;
        }
        await Admin.DisposeAsync();
        await using var server = NpgsqlDataSource.Create(_serverConnectionString!);
        await using var drop = server.CreateCommand($"DROP DATABASE IF EXISTS {_ownDatabase} WITH (FORCE)");
        await drop.ExecuteNonQueryAsync();
    }
}
