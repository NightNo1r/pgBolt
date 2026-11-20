using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>§12 Batches: fan-out 100 000, стоимость порции, частичные индексы захватов. Только Direct (EXPLAIN), Slow.</summary>
[Trait("Category", "Slow")]
public sealed class WorkflowResolverPlanTests(PostgresFixture pg) : WorkflowTestBase(pg, ConnectionMode.Direct)
{
    /// <summary>Активированный batch (state 0) с count ждущими членами (код 5) и учтённым сигналом Activated.</summary>
    private async Task<Guid> ActivatedFanOutAsync(int count)
    {
        var batch = Guid.CreateVersion7();
        var members = Enumerable.Range(0, count).Select(_ => Guid.CreateVersion7()).ToArray();
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@b, 0, cardinality(@ids), now());
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, parent_id, batch_id, continuation, notify)
            SELECT id, 'client.test', 'fan', 0, '{}', 0, 3, now(), @b, @b, 5, true FROM unnest(@ids) id;
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition, batch_id, notify)
            SELECT id, 'fan', 0, 'infinity', false, 3, @b, 5, @b, true FROM unnest(@ids) id;
            INSERT INTO {schema}.completions (node_id, kind) VALUES (@b, 5);
            ANALYZE {schema}.queue;
            """, ("b", batch), ("ids", members));
        Assert.Equal(1, (await Flow.CountCompletionsAsync(500, Ct)).Walking);
        return batch;
    }

    [Fact]
    public async Task Fan_out_of_100000_members_is_walked_in_bounded_portions()
    {
        var batch = await ActivatedFanOutAsync(100_000);
        var passes = 0;
        PgBolt.Postgres.Storage.ResolveResult r;
        do
        {
            r = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);
            passes++;
            Assert.True(r.Rows <= 5000, $"pass {passes} walked {r.Rows} rows");
            Assert.True(r.MaxPortion <= 1000, $"pass {passes} portion {r.MaxPortion}");
        }
        while (r.Captured > 0 && passes < 100);

        Assert.Equal(22, passes);   // 20 проходов по 5000, 21-й находит конец и удаляет сигнал, 22-й пуст
        Assert.Equal(100_000L, await Db.ScalarAsync<long>(
            "SELECT count(*) FROM {schema}.queue WHERE batch_id = @b AND ready AND await_condition IS NULL", ("b", batch)));
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Walk_portion_reads_do_not_grow_with_cursor_position()
    {
        var batch = await ActivatedFanOutAsync(20_000);
        var early = await IndexScanBuffersAsync(batch, cursor: null);
        for (var i = 0; i < 3; i++)
            await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);   // 15 000 запущено: мёртвые версии позади курсора
        var cursor = await Db.ScalarAsync<Guid>("SELECT cursor FROM {schema}.completions WHERE node_id = @b", ("b", batch));

        var late = await IndexScanBuffersAsync(batch, cursor);

        Assert.True(late <= early * 2 + 20, $"queue_awaiting scan: {early} buffers at start, {late} after 15 000 launched rows");
    }

    /// <summary>Буферы узла Index Scan по queue_awaiting в EXPLAIN (ANALYZE, BUFFERS) statement'а этапа 0; транзакция откатывается.</summary>
    private async Task<long> IndexScanBuffersAsync(Guid batch, Guid? cursor)
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + Flow.WalkJobsSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("nodes", [batch]));
        cmd.Parameters.Add(new NpgsqlParameter<short[]>("kinds", [5]));
        cmd.Parameters.Add(new NpgsqlParameter("cursors", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = new Guid?[] { cursor } });
        cmd.Parameters.Add(new NpgsqlParameter<int>("lim", 1000));
        cmd.Parameters.Add(new NpgsqlParameter<short>("kind_cancelled", (short)JobEventKind.Cancelled));
        var plan = (string)(await cmd.ExecuteScalarAsync(Ct))!;
        await tx.RollbackAsync(Ct);
        using var doc = JsonDocument.Parse(plan);
        var scan = Assert.Single(Flatten(doc.RootElement[0].GetProperty("Plan")),
            n => n.TryGetProperty("Index Name", out var name) && name.GetString() == "queue_awaiting");
        return scan.GetProperty("Shared Hit Blocks").GetInt64() + scan.GetProperty("Shared Read Blocks").GetInt64();
    }

    /// <summary>
    /// Огромный batch занимает почти всю queue (других строк нет): n_distinct(parent_id) по выборке ANALYZE — 1–2, и без
    /// закрепления n_distinct поиск детей по runtime parent_id планируется как Seq Scan (Т1) или обход queue_pkey (Т2).
    /// </summary>
    [Fact]
    public async Task Child_lookups_stay_on_queue_awaiting_when_one_parent_dominates_the_queue()
    {
        var big = Guid.CreateVersion7();
        var small = Guid.CreateVersion7();
        await Db.ExecAsync($$"""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@big, 0, 1000000, now()), (@small, 0, 3, now());
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition, batch_id, notify)
            SELECT {{SequentialId}}, 'fan', 0, 'infinity', false, 3, b.id, 5, b.id, true
            FROM (VALUES (@big, 1, 1000), (@small, 1001, 1003)) b(id, lo, hi), generate_series(b.lo, b.hi) g;
            INSERT INTO {schema}.completions (node_id, kind, batch_id) SELECT gen_random_uuid(), 2, @big FROM generate_series(1, 500);
            ANALYZE {schema}.queue;
            """, ("big", big), ("small", small));
        var countEarly = await ChildLookupAsync(Flow.CountSql, CountParameters());
        var walkEarly = await ChildLookupAsync(Flow.WalkJobsSql, WalkParameters(small, cursor: null));

        await Db.ExecAsync($$"""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition, batch_id, notify)
            SELECT {{SequentialId}}, 'fan', 0, 'infinity', false, 3, @big, 5, @big, true FROM generate_series(1004, 100003) g;
            ANALYZE {schema}.queue;
            """, ("big", big));
        var countLate = await ChildLookupAsync(Flow.CountSql, CountParameters());
        var walkLate = await ChildLookupAsync(Flow.WalkJobsSql, WalkParameters(small, cursor: null));

        Assert.All(new[] { countEarly, countLate, walkEarly, walkLate },
            s => Assert.True(s.Index == "queue_awaiting", $"child lookup on queue: {s.Node} {s.Index}, {s.Buffers} buffers"));
        Assert.True(countLate.Buffers <= countEarly.Buffers * 2 + 20,
            $"Т1, 500 leaf signals: {countEarly.Buffers} buffers at 1 003 rows, {countLate.Buffers} at 100 003 rows");
        Assert.True(walkLate.Buffers <= walkEarly.Buffers * 2 + 20,
            $"Т2, node with 3 children: {walkEarly.Buffers} buffers at 1 003 rows, {walkLate.Buffers} at 100 003 rows");
    }

    /// <summary>job_id как у UUIDv7 (миллисекунды clock_timestamp, затем уникальный g): queue_pkey коррелирует с heap, как в работе.</summary>
    private const string SequentialId =
        "(lpad(to_hex((extract(epoch FROM clock_timestamp()) * 1000)::bigint), 12, '0') || '7' || lpad(to_hex(g), 19, '0'))::uuid";

    private static NpgsqlParameter[] CountParameters() => [new NpgsqlParameter<int>("n", 500)];

    private static NpgsqlParameter[] WalkParameters(Guid node, Guid? cursor) =>
    [
        .. WalkBatchesParameters(node, 5, cursor),
        new NpgsqlParameter<short>("kind_cancelled", (short)JobEventKind.Cancelled),
    ];

    private static NpgsqlParameter[] WalkBatchesParameters(Guid node, short kind, Guid? cursor) =>
    [
        new NpgsqlParameter<Guid[]>("nodes", [node]),
        new NpgsqlParameter<short[]>("kinds", [kind]),
        new NpgsqlParameter("cursors", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = new Guid?[] { cursor } },
        new NpgsqlParameter<int>("lim", 1000),
    ];

    /// <summary>
    /// Единственный узел плана, ищущий строки relation по равенству column (детей узла), в EXPLAIN (ANALYZE, BUFFERS)
    /// statement'а: индекс (null — Seq Scan / Bitmap Heap Scan) и буферы за все циклы. Транзакция откатывается.
    /// </summary>
    private async Task<(string? Index, long Buffers, string Node)> ChildLookupAsync(
        string sql, NpgsqlParameter[] ps, string relation = "queue", string column = "parent_id")
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, conn, tx);
        cmd.Parameters.AddRange(ps);
        var plan = (string)(await cmd.ExecuteScalarAsync(Ct))!;
        await tx.RollbackAsync(Ct);
        using var doc = JsonDocument.Parse(plan);
        var scan = Assert.Single(Flatten(doc.RootElement[0].GetProperty("Plan")), n =>
            n.TryGetProperty("Relation Name", out var rel) && rel.GetString() == relation
            && new[] { "Index Cond", "Filter", "Recheck Cond" }.Any(p => n.TryGetProperty(p, out var c) && c.GetString()!.Contains($"{column} = ")));
        return (scan.TryGetProperty("Index Name", out var index) ? index.GetString() : null,
                scan.GetProperty("Shared Hit Blocks").GetInt64() + scan.GetProperty("Shared Read Blocks").GetInt64(),
                scan.GetProperty("Node Type").GetString()!);
    }

    /// <summary>Как для queue: тысячи batch-продолжений одного родителя-задачи — поиск по batches.parent_id (Т1, Т2 этап 1).</summary>
    [Fact]
    public async Task Continuation_batch_lookups_stay_on_batches_awaiting_when_one_parent_dominates()
    {
        var parent = Guid.CreateVersion7();
        var small = Guid.CreateVersion7();   // терминальный batch: сигнал Subscribed (7) обходит его 3 batch-продолжения
        await Db.ExecAsync($$"""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@small, 2, 1, now());
            INSERT INTO {schema}.batches (id, parent_id, condition, state, total, created_at)
            SELECT {{SequentialId}}, b.id, 1, 1, 1, now()
            FROM (VALUES (@parent, 1, 20000), (@small, 20001, 20003)) b(id, lo, hi), generate_series(b.lo, b.hi) g;
            -- Завершённые продолжения того же родителя, как в реальных данных: batches_parent (любое состояние) больше
            -- batches_awaiting (только state = 1); без них индексы идентичны и планировщик выбирает любой из двух.
            INSERT INTO {schema}.batches (id, parent_id, condition, state, total, created_at)
            SELECT {{SequentialId}}, @parent, 1, 2, 1, now() FROM generate_series(1, 60000) g;
            INSERT INTO {schema}.completions (node_id, kind) SELECT gen_random_uuid(), 2 FROM generate_series(1, 500);
            ANALYZE {schema}.batches;
            """, ("parent", parent), ("small", small));

        var count = await ChildLookupAsync(Flow.CountSql, CountParameters(), "batches");
        var walk = await ChildLookupAsync(Flow.WalkBatchesSql, WalkBatchesParameters(small, 7, cursor: null), "batches");

        Assert.All(new[] { count, walk },
            s => Assert.True(s.Index == "batches_awaiting", $"child lookup on batches: {s.Node} {s.Index}, {s.Buffers} buffers"));
    }

    /// <summary>Тысячи вложенных batches одного внешнего — поиск по batches.parent_batch_id (Т2 этап 1 для Activated).</summary>
    [Fact]
    public async Task Nested_batch_lookups_stay_on_batches_nested_when_one_outer_batch_dominates()
    {
        var outer = Guid.CreateVersion7();
        var small = Guid.CreateVersion7();
        await Db.ExecAsync($$"""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@outer, 0, 20000, now()), (@small, 0, 3, now());
            INSERT INTO {schema}.batches (id, parent_batch_id, state, total, created_at)
            SELECT {{SequentialId}}, b.id, 1, 1, now()
            FROM (VALUES (@outer, 1, 20000), (@small, 20001, 20003)) b(id, lo, hi), generate_series(b.lo, b.hi) g;
            ANALYZE {schema}.batches;
            """, ("outer", outer), ("small", small));

        var walk = await ChildLookupAsync(Flow.WalkBatchesSql, WalkBatchesParameters(small, 5, cursor: null), "batches", "parent_batch_id");

        Assert.True(walk.Index == "batches_nested", $"child lookup on batches: {walk.Node} {walk.Index}, {walk.Buffers} buffers");
    }

    [Fact]
    public async Task Captures_use_their_partial_indexes()   // §12 Batches: EXPLAIN захватов Т1 и Т2
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.completions (node_id, kind, counted) SELECT gen_random_uuid(), 2, g % 2 = 0 FROM generate_series(1, 2000) g;
            INSERT INTO {schema}.completions (node_id, kind) SELECT gen_random_uuid(), 6 FROM generate_series(1, 200);
            ANALYZE {schema}.completions;
            """);

        Assert.Contains("completions_count", await IndexesOfPlanAsync(Flow.CountSql));
        Assert.Contains("completions_walk", await IndexesOfPlanAsync(Flow.WalkCaptureSql));
    }

    [Fact]
    public async Task Ready_check_reads_only_the_capture_partial_indexes()   // §6.4: проверка простоя по таймеру
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.completions (node_id, kind, counted, retry_after)
            SELECT gen_random_uuid(), CASE WHEN g % 3 = 0 THEN 6 ELSE 2 END, g % 3 = 1, now() + interval '1 hour'
            FROM generate_series(1, 30000) g;
            ANALYZE {schema}.completions;
            """);

        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + Flow.ReadyCompletionsSql, conn);
        using var doc = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(Ct))!);
        var scans = Flatten(doc.RootElement[0].GetProperty("Plan"))
            .Where(n => n.TryGetProperty("Relation Name", out var rel) && rel.GetString() == "completions").ToList();

        Assert.All(scans, n => Assert.True(n.TryGetProperty("Index Name", out _), $"completions scan: {n.GetProperty("Node Type")}"));
        Assert.Equal(["completions_cancel", "completions_count", "completions_walk"],
            scans.Select(n => n.GetProperty("Index Name").GetString()!).Order());
    }

    [Fact]
    public async Task Count_readiness_probes_read_only_walk_and_cancel_partial_indexes()   // §6.4: пропуск этапов без работы
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.completions (node_id, kind, counted, retry_after)
            SELECT gen_random_uuid(), CASE WHEN g % 3 = 0 THEN 6 ELSE 2 END, g % 3 = 1, now() + interval '1 hour'
            FROM generate_series(1, 30000) g;
            ANALYZE {schema}.completions;
            """);

        // Сигналы отложены: Т1 ничего не захватывает, EXPLAIN ANALYZE в откатываемой транзакции ничего не меняет.
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + Flow.CountSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<int>("n", 500));
        using var doc = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(Ct))!);
        var scans = Flatten(doc.RootElement[0].GetProperty("Plan"))
            .Where(n => n.TryGetProperty("Relation Name", out var rel) && rel.GetString() == "completions"
                        && n.GetProperty("Node Type").GetString() != "ModifyTable").ToList();

        Assert.All(scans, n => Assert.True(n.TryGetProperty("Index Name", out _), $"completions scan: {n.GetProperty("Node Type")}"));
        var indexes = scans.Select(n => n.GetProperty("Index Name").GetString()!).ToList();
        Assert.Contains("completions_walk", indexes);
        Assert.Contains("completions_cancel", indexes);
    }

    private async Task<List<string>> IndexesOfPlanAsync(string sql)
    {
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var off = new NpgsqlCommand("SET enable_seqscan = off", conn))
            await off.ExecuteNonQueryAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, conn);
        cmd.Parameters.Add(new NpgsqlParameter<int>("n", 500));
        using var doc = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(Ct))!);
        return [.. Flatten(doc.RootElement[0].GetProperty("Plan"))
            .Where(n => n.TryGetProperty("Index Name", out _))
            .Select(n => n.GetProperty("Index Name").GetString()!)];
    }

    private static IEnumerable<JsonElement> Flatten(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
            foreach (var child in children.EnumerateArray())
                foreach (var n in Flatten(child))
                    yield return n;
    }
}
