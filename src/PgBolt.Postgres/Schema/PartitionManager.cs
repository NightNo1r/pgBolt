using System.Globalization;
using Npgsql;

namespace PgBolt.Postgres.Schema;

/// <summary>Создание суточных партиций jobs/job_events через CREATE TABLE (LIKE) + ATTACH (§4.4).</summary>
public sealed class PartitionManager(NpgsqlDataSource dataSource, string schema, int jobsFillFactor, TimeSpan lockTimeout)
{
    private readonly string _schema = SchemaName.Validate(schema);

    public async Task<PartitionEnsureResult> EnsureAsync(int daysAhead, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(daysAhead);
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            var ms = (int)Math.Ceiling(lockTimeout.TotalMilliseconds);
            // Сначала advisory lock, затем lock_timeout: ожидание параллельного EnsureAsync/миграции (§4.4) не таймаутится.
            await ExecAsync(conn, tx, PgBoltMigrations.LockKeySql, ct, ("lock_key", _schema));
            await ExecAsync(conn, tx, $"SET LOCAL lock_timeout = '{ms}ms'", ct);

            var today = await TodayAsync(conn, tx, ct);
            var existing = await ExistingAsync(conn, tx, ct);
            var created = 0;
            for (var day = today; day <= today.AddDays(daysAhead); day = day.AddDays(1))
            {
                var suffix = day.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
                if (!existing.Contains($"jobs_{suffix}"))
                {
                    await ExecAsync(conn, tx, JobsDdl(day, suffix), ct);
                    created++;
                }
                if (!existing.Contains($"job_events_{suffix}"))
                {
                    await ExecAsync(conn, tx, EventsDdl(day, suffix), ct);
                    created++;
                }
            }
            await tx.CommitAsync(ct);
            return new PartitionEnsureResult(created, TimedOut: false, TodayPresent: true);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            await tx.RollbackAsync(ct);
            return new PartitionEnsureResult(0, TimedOut: true, TodayPresent: await TodayPresentAsync(ct));
        }
    }

    private string JobsDdl(DateOnly day, string suffix) => $"""
        CREATE TABLE {_schema}.jobs_{suffix} (LIKE {_schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
            WITH (fillfactor = {jobsFillFactor});
        ALTER TABLE {_schema}.jobs ATTACH PARTITION {_schema}.jobs_{suffix}
            FOR VALUES FROM ('{UuidV7.LowerBound(day)}') TO ('{UuidV7.LowerBound(day.AddDays(1))}');
        """;

    private string EventsDdl(DateOnly day, string suffix) => $"""
        CREATE TABLE {_schema}.job_events_{suffix} (LIKE {_schema}.job_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
        ALTER TABLE {_schema}.job_events ATTACH PARTITION {_schema}.job_events_{suffix}
            FOR VALUES FROM ('{Iso(day)} 00:00:00+00') TO ('{Iso(day.AddDays(1))} 00:00:00+00');
        """;

    private static string Iso(DateOnly day) => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private async Task<bool> TodayPresentAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var today = await TodayAsync(conn, null, ct);
        var existing = await ExistingAsync(conn, null, ct);
        var suffix = today.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        return existing.Contains($"jobs_{suffix}") && existing.Contains($"job_events_{suffix}");
    }

    private static async Task<DateOnly> TodayAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT (now() AT TIME ZONE 'UTC')::date", conn, tx);
        return (DateOnly)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private async Task<HashSet<string>> ExistingAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct)
    {
        const string sql = """
            SELECT c.relname
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = p.relnamespace
            WHERE n.nspname = @schema AND p.relname IN ('jobs', 'job_events')
            """;
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        cmd.Parameters.AddWithValue("schema", _schema);
        var set = new HashSet<string>(StringComparer.Ordinal);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            set.Add(r.GetString(0));
        return set;
    }

    private static async Task ExecAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, CancellationToken ct, params (string Name, object Value)[] ps)
    {
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
