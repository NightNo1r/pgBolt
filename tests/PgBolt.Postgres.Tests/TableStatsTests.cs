using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Чтение размера и числа мёртвых строк таблиц queue и completions своей схемы.</summary>
public abstract class TableStatsTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Returns_queue_and_completions_of_own_schema_only()
    {
        await using var other = await TestDb.CreateAsync(Pg, Mode);

        var rows = await Store.ReadTableStatsAsync(Ct);

        Assert.Equal(["completions", "queue"], rows.Select(r => r.Table).Order(StringComparer.Ordinal));
        Assert.All(rows, r => Assert.True(r.SizeBytes > 0));
    }
}

public sealed class TableStatsTests_Direct(PostgresFixture pg) : TableStatsTests(pg, ConnectionMode.Direct)
{
    // Статистика backend'а сбрасывается в общий снимок, когда он уходит в idle после statement'а с pg_stat_force_next_flush,
    // поэтому вставка, удаление и сброс идут на одном соединении вне явной транзакции.
    [Fact]
    public async Task Dead_tuples_appear_after_delete()
    {
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var cmd = new NpgsqlCommand(Db.Sql("""
            INSERT INTO {schema}.queue (job_id, queue, run_at, ready, max_attempts)
            SELECT gen_random_uuid(), 'q', now(), true, 3 FROM generate_series(1, 100)
            """), conn);
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = Db.Sql("DELETE FROM {schema}.queue");
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "SELECT pg_stat_force_next_flush()";
        await cmd.ExecuteNonQueryAsync(Ct);

        var rows = await Store.ReadTableStatsAsync(Ct);

        Assert.True(rows.Single(r => r.Table == "queue").DeadTuples > 0);
    }
}

public sealed class TableStatsTests_Bouncer(PostgresFixture pg) : TableStatsTests(pg, ConnectionMode.Bouncer);
