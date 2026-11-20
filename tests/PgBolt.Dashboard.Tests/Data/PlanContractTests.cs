using System.Text.Json;
using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Views;

namespace PgBolt.Dashboard.Tests.Data;

/// <summary>§12: планы statement'ов экранов на заполненной схеме (EXPLAIN ровно тех statement'ов, что выполняет store). Только Direct.</summary>
[Trait("Category", "Slow")]
public sealed class PlanContractTests(PlanFixture fx) : IClassFixture<PlanFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private DashboardStore NewStore() =>
        new(fx.Db.DataSource, fx.Db.Schema, usePreparedStatements: false, new PgBoltDashboardOptions(), TimeProvider.System);

    /// <summary>Store, чьи statement'ы перед выполнением попадают в plans как (SQL, EXPLAIN JSON).</summary>
    private DashboardStore Observed(List<(string Sql, string Plan)> plans)
    {
        var store = NewStore();
        store.OnStatement = async (sql, bind, ct) =>
        {
            await using var cmd = fx.Db.Admin.CreateCommand("EXPLAIN (FORMAT JSON) " + sql);
            bind(cmd.Parameters);
            plans.Add((sql, (string)(await cmd.ExecuteScalarAsync(ct))!));
        };
        return store;
    }

    [Fact]
    public async Task Every_screen_statement_avoids_seq_scans()
    {
        var plans = new List<(string Sql, string Plan)>();
        var store = Observed(plans);

        await store.GetOverviewAsync(Ct);
        await store.GetThroughputDayAsync(Ct);
        await store.ListQueuesAsync(Ct);
        await store.CountLeasedAsync(fx.Queues, Ct);
        await store.GetServersAsync(Ct);
        await store.ListJobTypesAsync(Ct);
        await store.GetJobTypeAsync(fx.Types[0], Ct);
        foreach (var kind in Enum.GetValues<JobState>())
        {
            var queue = kind == JobState.Enqueued ? fx.Queues[0] : null;
            var first = await store.ListJobsAsync(kind, queue, null, null, Ct);
            // Курсор фильтрованных списков — Next нефильтрованной первой страницы: у фильтра Next может не быть (Enqueued q0 + тип).
            var next = Assert.IsType<string>(first.Next);
            await store.ListJobsAsync(kind, queue, null, next, Ct);
            await store.ListJobsAsync(kind, fx.Queues[1], null, null, Ct);
            await store.ListJobsAsync(kind, fx.Queues[1], null, next, Ct);
            await store.ListJobsAsync(kind, queue, fx.Types[1], null, Ct);
            await store.ListJobsAsync(kind, queue, fx.Types[1], next, Ct);
        }
        await PagesAsync(after => store.ListContinuationsAsync(fx.ParentId, batchParent: false, fx.ParentCreatedAt, after, Ct));
        await PagesAsync(after => store.ListContinuationsAsync(fx.ParentId, batchParent: true, fx.ParentCreatedAt, after, Ct));
        await PagesAsync(after => store.ListBatchJobsAsync(fx.BatchId, fx.BatchCreatedAt, after, Ct));
        await PagesAsync(after => store.ListNestedBatchesAsync(fx.BatchId, after, Ct));
        await PagesAsync(after => store.ListBatchContinuationsAsync(fx.ParentId, after, Ct));
        await PagesAsync(after => store.ListBatchesAsync(null, after, Ct));
        await PagesAsync(after => store.ListBatchesAsync(BatchState.Failed, after, Ct));
        // 50 расписаний — одна страница: форма с курсором — от явного курсора.
        await store.ListRecurringAsync(null, Ct);
        await store.ListRecurringAsync(Cursor.Encode(new ListKey(Guid.Empty, Text: fx.RecurringId), CursorShape.Text), Ct);
        await PagesAsync(after => store.ListRecurringRunsAsync(fx.RecurringId, after, Ct));

        // Все statement'ы store: обзор 9 (обзор, час, день, очереди, взятые строки очередей, серверы, задачи обслуживания, типы, тип);
        // queue-списки 13 (Scheduled, Enqueued, Awaiting — простая и порционная формы, каждая с курсором и без; Processing);
        // Succeeded, Failed, Cancelled — 6; связанные списки — 18 (9 списков, каждый с курсором и без).
        Assert.Equal(46, plans.Select(p => p.Sql).Distinct().Count());
        foreach (var (sql, plan) in plans)
            AssertNoSeqScan(sql, plan);
    }

    [Fact]
    public async Task Scheduled_with_many_awaiting_rows_bounds_queue_delayed_by_infinity()
    {
        var plans = new List<(string Sql, string Plan)>();
        await Observed(plans).ListJobsAsync(JobState.Scheduled, null, null, null, Ct);

        var (_, plan) = Assert.Single(plans);
        var scan = Assert.Single(Flatten(Root(plan)), n => IsIndexScan(n) && IndexName(n) == "queue_delayed");
        Assert.Contains("'infinity'", scan.GetProperty("Index Cond").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(JobState.Failed, "job_events_failed")]
    [InlineData(JobState.Cancelled, "job_events_cancelled")]
    public async Task Failed_and_cancelled_read_index_order_without_sort(JobState kind, string index)
    {
        var plans = new List<(string Sql, string Plan)>();
        var store = Observed(plans);
        await PagesAsync(after => store.ListJobsAsync(kind, null, null, after, Ct));
        await PagesAsync(after => store.ListJobsAsync(kind, null, fx.Types[1], after, Ct));

        Assert.NotEmpty(plans);
        foreach (var (sql, plan) in plans)
        {
            var root = Root(plan);
            var message = $"{sql}\n{plan}";
            // (1) Sort над диапазоном индекса job_events: тела CTE (InitPlan / SubPlan) в обход не входят.
            foreach (var sort in Flatten(root).Where(IsSort))
                Assert.False(Below(sort).Any(IsEventsIndexScan), $"Sort over job_events index range:\n{message}");
            // (2) CTE k — Limit → (Merge) Append из Index Scan [Backward] по партициям, без Sort.
            var k = Assert.Single(Flatten(root), n => n.TryGetProperty("Subplan Name", out var name) && name.GetString() == "CTE k");
            Assert.False(Flatten(k).Any(IsSort), $"Sort in CTE k:\n{message}");
        }
        // (3) Index Scan по индексу родителя.
        await AssertIndexScanAsync(plans, index);
    }

    /// <summary>
    /// Взятые строки читаются только через queue_leased. Index Only Scan планировщик выбирает лишь при заполненной visibility map,
    /// а её VACUUM не выставляет, пока горизонт xmin держат параллельные тесты: допустимы и Index Scan, и bitmap по queue_leased.
    /// </summary>
    [Fact]
    public async Task Count_leased_reads_only_queue_leased_index()
    {
        var plans = new List<(string Sql, string Plan)>();
        await Observed(plans).CountLeasedAsync(fx.Queues[..2], Ct);

        var (sql, plan) = Assert.Single(plans);
        AssertNoSeqScan(sql, plan);
        Assert.True(Flatten(Root(plan)).Select(IndexName).OfType<string>().Distinct().SequenceEqual(["queue_leased"]), $"{sql}\n{plan}");
    }

    [Fact]
    public async Task Deep_enqueued_page_uses_queue_fetch_in_both_branches()
    {
        var plans = new List<(string Sql, string Plan)>();
        var store = Observed(plans);
        var first = await store.ListJobsAsync(JobState.Enqueued, fx.Queues[0], null, null, Ct);
        plans.Clear();
        await store.ListJobsAsync(JobState.Enqueued, fx.Queues[0], null, Assert.IsType<string>(first.Next), Ct);

        var (sql, plan) = Assert.Single(plans);
        Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("MATERIALIZED", sql, StringComparison.Ordinal);
        var conds = Flatten(Root(plan)).Where(n => IsIndexScan(n) && IndexName(n) == "queue_fetch")
            .Select(n => n.GetProperty("Index Cond").GetString()!).ToList();
        Assert.Equal(2, conds.Count);
        foreach (var column in new[] { "queue", "priority", "run_at" })
            Assert.Contains(column, conds[0], StringComparison.Ordinal);
        foreach (var column in new[] { "queue", "priority" })
            Assert.Contains(column, conds[1], StringComparison.Ordinal);
    }

    [Fact]
    public async Task Continuations_use_jobs_parent_and_recurring_uses_jobs_recurring()
    {
        var plans = new List<(string Sql, string Plan)>();
        var store = Observed(plans);

        await store.ListContinuationsAsync(fx.ParentId, batchParent: false, fx.ParentCreatedAt, null, Ct);
        // Bitmap Index Scan + Sort допустим: планировщик оценивает parent_id = @parent и continuation IN (1, 2) как независимые
        // колонки (а continuation задана ровно у строк с parent_id) и занижает число строк ниже LIMIT. §12 требует только
        // отсутствия Seq Scan; индекс jobs_parent используется.
        await AssertIndexScanAsync(plans, "jobs_parent", bitmap: true);

        plans.Clear();
        await store.ListRecurringRunsAsync(fx.RecurringId, null, Ct);
        await AssertIndexScanAsync(plans, "jobs_recurring");
    }

    /// <summary>Первая страница и страница по её Next (Next обязан быть).</summary>
    private static async Task PagesAsync<T>(Func<string?, Task<Page<T>>> list)
    {
        var first = await list(null);
        await list(Assert.IsType<string>(first.Next));
    }

    /// <summary>Каждый план содержит Index Scan (при bitmap — или Bitmap Index Scan) по индексу родителя parentIndex (по партициям).</summary>
    private async Task AssertIndexScanAsync(List<(string Sql, string Plan)> plans, string parentIndex, bool bitmap = false)
    {
        Assert.NotEmpty(plans);
        foreach (var (sql, plan) in plans)
        {
            var parents = new List<string>();
            foreach (var scan in Flatten(Root(plan)).Where(n => IsIndexScan(n) || (bitmap && NodeType(n) == "Bitmap Index Scan")))
                parents.Add(await ParentIndexAsync(IndexName(scan)!));
            Assert.True(parents.Contains(parentIndex), $"no Index Scan by {parentIndex}:\n{sql}\n{plan}");
        }
    }

    /// <summary>Имя индекса родителя для индекса партиции (автоматические имена), иначе само имя.</summary>
    private Task<string> ParentIndexAsync(string indexName) => fx.Db.ScalarAsync<string>("""
        SELECT coalesce((SELECT p.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent
                         JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = @idx AND n.nspname = @s), @idx)
        """, ("idx", indexName), ("s", fx.Db.Schema));

    private static void AssertNoSeqScan(string sql, string plan)
    {
        var seq = Flatten(Root(plan)).Where(n => NodeType(n) == "Seq Scan" && n.TryGetProperty("Relation Name", out var r)
            && r.GetString() is { } name && (name == "queue" || name.StartsWith("jobs", StringComparison.Ordinal)
                || name.StartsWith("job_events", StringComparison.Ordinal)));
        Assert.False(seq.Any(), $"Seq Scan:\n{sql}\n{plan}");
    }

    private static JsonElement Root(string plan) => JsonSerializer.Deserialize<JsonElement>(plan)[0].GetProperty("Plan");

    private static IEnumerable<JsonElement> Children(JsonElement node) =>
        node.TryGetProperty("Plans", out var children) ? children.EnumerateArray() : [];

    /// <summary>Узел и все потомки, включая InitPlan / SubPlan (тела CTE).</summary>
    private static IEnumerable<JsonElement> Flatten(JsonElement node) => Children(node).SelectMany(Flatten).Prepend(node);

    /// <summary>Потомки по потоку строк: Outer / Inner / Member / Subquery, без InitPlan / SubPlan.</summary>
    private static IEnumerable<JsonElement> Below(JsonElement node) => Children(node)
        .Where(c => c.GetProperty("Parent Relationship").GetString() is "Outer" or "Inner" or "Member" or "Subquery")
        .SelectMany(c => Below(c).Prepend(c));

    private static string? NodeType(JsonElement n) => n.GetProperty("Node Type").GetString();

    private static string? IndexName(JsonElement n) => n.TryGetProperty("Index Name", out var name) ? name.GetString() : null;

    private static bool IsSort(JsonElement n) => NodeType(n) is "Sort" or "Incremental Sort";

    private static bool IsIndexScan(JsonElement n) => NodeType(n) is "Index Scan" or "Index Only Scan";

    private static bool IsEventsIndexScan(JsonElement n) => IsIndexScan(n) && n.TryGetProperty("Relation Name", out var r)
        && r.GetString()!.StartsWith("job_events", StringComparison.Ordinal);
}
