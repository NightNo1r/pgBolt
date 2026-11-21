using System.Text.Json;
using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>
/// Планы dequeue на большой очереди: 300 000 готовых строк, jobs — 10 суточных партиций по 140 000 строк. Без Seq Scan по queue
/// и партициям jobs. Только Direct (EXPLAIN), Slow.
/// </summary>
[Trait("Category", "Slow")]
public sealed class DequeueLimitedPlanTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    private const int SeedTimeoutSeconds = 120;

    protected override Task<TestDb> CreateDbAsync() => TestDb.CreateAsync(Pg, ConnectionMode.Direct, partitionDaysAhead: 9);

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        // id как у UUIDv7: миллисекунды внутри суток d (партиция jobs того же дня), затем уникальный g.
        await using var seed = Db.Admin.CreateCommand(Db.Sql("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at)
            SELECT (lpad(to_hex(((extract(epoch FROM (now() AT TIME ZONE 'UTC')::date + d) * 1000)::bigint + g)), 12, '0')
                    || '7' || lpad(to_hex(d * 1000000 + g), 19, '0'))::uuid,
                   'test.job', CASE WHEN g <= 30000 THEN 'q' ELSE 'done' END, 0, '{}', CASE WHEN g <= 30000 THEN 0 ELSE 2 END, 3, now()
            FROM generate_series(0, 9) d, generate_series(1, 140000) g;
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts)
            SELECT id, 'q', 0, now() - interval '1 minute', true, 3 FROM {schema}.jobs WHERE queue = 'q';
            ANALYZE {schema}.jobs, {schema}.queue;
            """));
        // Массовая загрузка на общем раннере может занять больше стандартных 30 секунд.
        seed.CommandTimeout = SeedTimeoutSeconds;
        await seed.ExecuteNonQueryAsync(Ct);

        Assert.Equal(300_000L, await CountAsync("queue"));
        Assert.Equal(1_400_000L, await CountAsync("jobs"));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    public async Task Limited_dequeue_uses_indexes(int n)
    {
        await Db.ExecAsync("INSERT INTO {schema}.queue_settings (queue, default_max_concurrency) VALUES ('q', 50)");

        var nodes = await PlanAsync(Store.DequeueLimitedSql, n, analyze: false);

        AssertNoScansOfWholeTables(nodes);
        Assert.Contains(nodes, x => Is(x, "Index Only Scan", "queue_leased"));
        Assert.Contains(nodes, x => Is(x, "Index Scan", "queue_fetch"));
    }

    [Fact]
    public async Task Paused_limited_dequeue_does_not_read_candidates()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue_settings (queue, override_concurrency_set, override_max_concurrency) VALUES ('q', true, 0)
            """);

        var nodes = await PlanAsync(Store.DequeueLimitedSql, 100, analyze: true);

        var fetch = Assert.Single(nodes, x => Is(x, "Index Scan", "queue_fetch"));
        Assert.Equal(0, fetch.GetProperty("Actual Loops").GetInt32());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    public async Task Plain_dequeue_uses_indexes(int n)
    {
        var nodes = await PlanAsync(Store.DequeueSql, n, analyze: false);

        AssertNoScansOfWholeTables(nodes);
        Assert.Contains(nodes, x => Is(x, "Index Scan", "queue_fetch"));
    }

    private static void AssertNoScansOfWholeTables(List<JsonElement> nodes)
    {
        // Соединение двух ограниченных пачек по hash допустимо. Захват обязан обращаться к queue по индексам.
        Assert.DoesNotContain(nodes, x => x.GetProperty("Node Type").GetString() == "Seq Scan"
            && x.GetProperty("Relation Name").GetString()! is var r && (r == "queue" || r.StartsWith("jobs_", StringComparison.Ordinal)));
    }

    private static bool Is(JsonElement node, string type, string index) =>
        node.GetProperty("Node Type").GetString() == type
        && node.TryGetProperty("Index Name", out var name) && name.GetString() == index;

    /// <summary>EXPLAIN statement'а dequeue в откатываемой транзакции (ANALYZE исполняет его).</summary>
    private async Task<List<JsonElement>> PlanAsync(string sql, int n, bool analyze)
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand($"EXPLAIN ({(analyze ? "ANALYZE, " : "")}FORMAT JSON) {sql}", conn, tx);
        cmd.Parameters.AddWithValue("queue", "q");
        cmd.Parameters.AddWithValue("n", n);
        cmd.Parameters.AddWithValue("server", ServerA);
        cmd.Parameters.AddWithValue("lease", Lease);
        var plan = (string)(await cmd.ExecuteScalarAsync(Ct))!;
        await tx.RollbackAsync(Ct);
        using var doc = JsonDocument.Parse(plan);
        return [.. Flatten(doc.RootElement[0].GetProperty("Plan")).Select(x => x.Clone())];
    }

    private static IEnumerable<JsonElement> Flatten(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
            foreach (var child in children.EnumerateArray())
                foreach (var x in Flatten(child))
                    yield return x;
    }
}
