using Npgsql;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Measure;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.LoadTest.Tests.Measure;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class LockProbeTests(PostgresFixture pg)
{
    [Fact]
    public async Task Dequeue_locks_grow_with_partitions()
    {
        var (at15, at60) = await TotalsAt15And60Async(StatementClasses.Dequeue);

        // Custom plan dequeue блокирует каждую партицию и её индексы: не меньше 4 отношений на партицию.
        Assert.True(at60 - at15 >= 4 * 45, $"при 15 партициях {at15}, при 60 — {at60}");
    }

    [Fact]
    public async Task Ack_locks_do_not_grow_with_partitions()
    {
        var (at15, at60) = await TotalsAt15And60Async(StatementClasses.Ack);

        Assert.InRange(at60, at15 - 2, at15 + 2);
    }

    [Theory]
    [InlineData(StatementClasses.Dequeue)]
    [InlineData(StatementClasses.Ack)]
    [InlineData(StatementClasses.Enqueue)]
    public async Task Detects_plan_kind_and_deallocates(string statement)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct, partitionDaysAhead: 14);
        // Один сеанс без сброса при возврате в пул: проба и проверка ниже работают на одном backend'е.
        var cs = new NpgsqlConnectionStringBuilder(db.ConnectionString) { MaxPoolSize = 1, NoResetOnClose = true }
            .ConnectionString;
        var s = Statement(db, statement);

        // PREPARE выводит типы позиционных параметров из текста statement'а: ошибка вывода — исключение здесь.
        await new LockProbe(cs).DetectPlanAsync(s.Sql, s.Bind, ct);

        await using var conn = await StandConnection.OpenAsync(cs, LockProbe.ConnectionPurpose, ct);
        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM pg_prepared_statements", conn);
        Assert.Equal(0L, await cmd.ExecuteScalarAsync(ct));
    }

    private async Task<(long At15, long At60)> TotalsAt15And60Async(string statement)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct, partitionDaysAhead: 14);
        var s = Statement(db, statement);
        var probe = new LockProbe(db.ConnectionString);

        var at15 = (await probe.MeasureAsync(s.Sql, s.Bind, ct)).Total;
        await new PartitionManager(db.Admin, db.Schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(59, ct);
        var at60 = (await probe.MeasureAsync(s.Sql, s.Bind, ct)).Total;
        return (at15, at60);
    }

    private static (string Statement, string Sql, Action<NpgsqlParameterCollection> Bind) Statement(TestDb db,
        string statement) =>
        LockProbe.HotPath(new PgBoltStore(db.DataSource, db.Schema, usePreparedStatements: false))
            .Single(s => s.Statement == statement);
}
