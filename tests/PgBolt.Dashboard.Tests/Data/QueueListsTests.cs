using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Dashboard.Views;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Data;

public abstract class QueueListsTests(PostgresFixture pg, ConnectionMode mode) : DashboardTestBase(pg, mode)
{
    private const string Type = "dash.job";

    /// <summary>Фиксированная точка отсчёта run_at: тесты не зависят от смены минуты.</summary>
    private static readonly DateTime T = new(DateTime.UtcNow.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour, DateTimeKind.Utc);

    private Task<Page<JobListRow>> ListAsync(JobState kind, string? queue = null, string? type = null, string? after = null) =>
        Store.ListJobsAsync(kind, queue, type, after, Ct);

    /// <summary>Все страницы по Next, начиная с первой.</summary>
    private async Task<List<Page<JobListRow>>> AllPagesAsync(JobState kind, string? queue = null, string? type = null)
    {
        var pages = new List<Page<JobListRow>> { await ListAsync(kind, queue, type) };
        while (pages[^1].Next is { } next)
            pages.Add(await ListAsync(kind, queue, type, next));
        return pages;
    }

    private async Task<Guid> SeedOneAsync(string queueRow, DateTime? runAt = null, short priority = 0, string queue = "default",
        string type = Type) =>
        (await SeedJobsAsync(1, type, queue, 0, queueRow, runAt, priority)).Single();

    [Fact]
    public async Task Scheduled_is_ordered_by_run_at_and_excludes_awaiting_and_ready()
    {
        var in3 = await SeedOneAsync("scheduled", T.AddHours(3));
        var in1 = await SeedOneAsync("scheduled", T.AddHours(1));
        var in2 = await SeedOneAsync("scheduled", T.AddHours(2));
        await SeedOneAsync("enqueued");
        await SeedOneAsync("awaiting");

        var page = await ListAsync(JobState.Scheduled);

        Assert.Equal([in1, in2, in3], page.Rows.Select(r => r.Id));
        Assert.Equal([T.AddHours(1), T.AddHours(2), T.AddHours(3)], page.Rows.Select(r => r.RunAt!.Value));
        Assert.Null(page.Next);
        Assert.Null(page.ScannedTo);
    }

    [Fact]
    public async Task Scheduled_pages_with_equal_run_at_cover_each_job_once()
    {
        var ids = await SeedJobsAsync(120, Type, "default", 0, "scheduled", T.AddHours(1));

        var pages = await AllPagesAsync(JobState.Scheduled);

        Assert.Equal([50, 50, 20], pages.Select(p => p.Rows.Count));
        Assert.Equal(ids, pages.SelectMany(p => p.Rows).Select(r => r.Id));
        Assert.Null(pages[2].Next);
    }

    [Fact]
    public async Task Enqueued_requires_queue()
    {
        await Assert.ThrowsAsync<DashboardBadRequestException>(() => ListAsync(JobState.Enqueued));
    }

    [Fact]
    public async Task Enqueued_is_ordered_by_priority_desc_then_run_at()
    {
        var p0 = await SeedOneAsync("enqueued", T.AddMinutes(-10), 0, "a");
        var p5Late = await SeedOneAsync("enqueued", T.AddMinutes(-5), 5, "a");
        var p5Early = await SeedOneAsync("enqueued", T.AddMinutes(-20), 5, "a");
        var pMinus1 = await SeedOneAsync("enqueued", T.AddMinutes(-30), -1, "a");
        await SeedOneAsync("enqueued", T.AddMinutes(-30), 9, "b");

        var page = await ListAsync(JobState.Enqueued, "a");

        Assert.Equal([p5Early, p5Late, p0, pMinus1], page.Rows.Select(r => r.Id));
        Assert.Null(page.Next);
    }

    [Fact]
    public async Task Enqueued_pages_with_equal_priority_and_run_at_cover_each_job_once()
    {
        var high = await SeedJobsAsync(70, Type, "a", 0, "enqueued", T, priority: 3);
        var low = await SeedJobsAsync(40, Type, "a", 0, "enqueued", T, priority: 1);

        var pages = await AllPagesAsync(JobState.Enqueued, "a");

        Assert.Equal([50, 50, 10], pages.Select(p => p.Rows.Count));
        Assert.Equal(high.Concat(low), pages.SelectMany(p => p.Rows).Select(r => r.Id));
        Assert.Null(pages[2].Next);
    }

    [Fact]
    public async Task Processing_is_keyset_by_leased_at_then_id()
    {
        await SeedJobsAsync(60, Type, "default", 0, "enqueued", T, runAtStep: TimeSpan.FromMilliseconds(1));
        var leased = new List<Guid>();
        for (var i = 0; i < 60; i++)
            leased.Add((await LeaseAsync()).Id);

        var first = await ListAsync(JobState.Processing);
        var second = await ListAsync(JobState.Processing, after: first.Next);

        Assert.Equal(leased.Take(50), first.Rows.Select(r => r.Id));
        Assert.NotNull(first.Next);
        Assert.Equal(leased.Skip(50), second.Rows.Select(r => r.Id));
        Assert.Null(second.Next);
    }

    [Fact]
    public async Task Processing_filters_by_queue_and_type()
    {
        await SeedOneAsync("enqueued");
        await SeedOneAsync("enqueued", queue: "b", type: "other");
        var a = await LeaseAsync();
        var b = await LeaseAsync("b");

        Assert.Equal([a.Id], (await ListAsync(JobState.Processing, queue: "default")).Rows.Select(r => r.Id));
        Assert.Equal([b.Id], (await ListAsync(JobState.Processing, type: "other")).Rows.Select(r => r.Id));
        Assert.Empty((await ListAsync(JobState.Processing, queue: "default", type: "other")).Rows);
    }

    [Fact]
    public async Task Awaiting_pages_by_cursor()
    {
        var ids = await SeedJobsAsync(51, Type, "default", 0, "awaiting");

        var first = await ListAsync(JobState.Awaiting);
        var second = await ListAsync(JobState.Awaiting, after: first.Next);

        Assert.Equal(ids.Reverse().Take(50), first.Rows.Select(r => r.Id));
        Assert.Equal([ids[0]], second.Rows.Select(r => r.Id));
        Assert.Null(second.Next);
    }

    [Fact]
    public async Task Awaiting_is_newest_first()
    {
        var ids = await SeedJobsAsync(3, Type, "default", 0, "awaiting");

        var page = await ListAsync(JobState.Awaiting);

        Assert.Equal(ids.Reverse(), page.Rows.Select(r => r.Id));
    }

    [Fact]
    public async Task Type_filter_scans_in_portions_and_reports_progress()
    {
        Time.Step = TimeSpan.FromSeconds(1);
        await SeedJobsAsync(4500, "noise", "default", 0, "scheduled", T.AddHours(1), runAtStep: TimeSpan.FromMilliseconds(1));
        var needles = await SeedJobsAsync(5, "needle", "default", 0, "scheduled", T.AddHours(2));

        var page = await ListAsync(JobState.Scheduled, type: "needle");
        Assert.Empty(page.Rows);
        Assert.NotNull(page.ScannedTo);
        Assert.NotNull(page.Next);

        while (page.Rows.Count == 0 && page.Next is { } next)
            page = await ListAsync(JobState.Scheduled, type: "needle", after: next);

        Assert.Equal(needles, page.Rows.Select(r => r.Id));
        Assert.Null(page.ScannedTo);
        Assert.Null(page.Next);
    }

    [Fact]
    public async Task Queue_filter_on_awaiting_uses_portions()
    {
        await SeedJobsAsync(3, Type, "a", 0, "awaiting");
        var b = await SeedJobsAsync(2, Type, "b", 0, "awaiting");
        var statements = new List<string>();
        Store.OnStatement = (sql, _, _) =>
        {
            statements.Add(sql);
            return Task.CompletedTask;
        };

        var page = await ListAsync(JobState.Awaiting, queue: "b");

        Assert.Equal(b.Reverse(), page.Rows.Select(r => r.Id));
        Assert.Contains("MATERIALIZED", Assert.Single(statements), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Enqueued_type_filter_follows_mixed_order()
    {
        var x1Early = await SeedOneAsync("enqueued", T.AddMinutes(-20), 1, "a", "x");
        await SeedOneAsync("enqueued", T.AddMinutes(-30), 2, "a", "y");
        var x2Late = await SeedOneAsync("enqueued", T.AddMinutes(-5), 2, "a", "x");
        var x2Early = await SeedOneAsync("enqueued", T.AddMinutes(-10), 2, "a", "x");
        await SeedOneAsync("enqueued", T.AddMinutes(-40), 1, "a", "y");
        var x1Late = await SeedOneAsync("enqueued", T.AddMinutes(-1), 1, "a", "x");

        var page = await ListAsync(JobState.Enqueued, "a", "x");

        Assert.Equal([x2Early, x2Late, x1Early, x1Late], page.Rows.Select(r => r.Id));
        Assert.All(page.Rows, r => Assert.Equal("x", r.Type));
    }

    [Fact]
    public async Task Filtered_portion_lists_page_by_cursor()
    {
        var x2 = await SeedJobsAsync(30, "x", "a", 0, "enqueued", T, priority: 2);
        await SeedJobsAsync(10, "y", "a", 0, "enqueued", T, priority: 2);
        var x1 = await SeedJobsAsync(30, "x", "a", 0, "enqueued", T, priority: 1);
        var b = await SeedJobsAsync(60, Type, "b", 0, "awaiting");
        await SeedJobsAsync(10, Type, "c", 0, "awaiting");

        var enqueued = await AllPagesAsync(JobState.Enqueued, "a", "x");
        var awaiting = await AllPagesAsync(JobState.Awaiting, "b");

        Assert.Equal([50, 10], enqueued.Select(p => p.Rows.Count));
        Assert.Equal(x2.Concat(x1), enqueued.SelectMany(p => p.Rows).Select(r => r.Id));
        Assert.Equal([50, 10], awaiting.Select(p => p.Rows.Count));
        Assert.Equal(b.Reverse(), awaiting.SelectMany(p => p.Rows).Select(r => r.Id));
    }

    [Fact]
    public async Task Rows_carry_job_columns()
    {
        var scheduled = await SeedOneAsync("scheduled", T.AddHours(1), 7, "q1", "t.scheduled");
        await SeedOneAsync("enqueued", T, 0, "q2", "t.processing");
        var leased = await LeaseAsync("q2");

        var row = Assert.Single((await ListAsync(JobState.Scheduled)).Rows);
        Assert.Equal(scheduled, row.Id);
        Assert.Equal(("t.scheduled", "q1", (short)7, 0, 3), (row.Type, row.Queue, row.Priority, row.Attempt, row.MaxAttempts));
        Assert.Equal(await Db.ScalarAsync<DateTime>("SELECT created_at FROM {schema}.jobs WHERE id = @id", ("id", scheduled)), row.CreatedAt);
        Assert.Equal(T.AddHours(1), row.RunAt);
        Assert.Null(row.StartedAt);
        Assert.Null(row.ServerId);
        Assert.False(row.CancelRequested);

        var processing = Assert.Single((await ListAsync(JobState.Processing)).Rows);
        Assert.Equal(leased.Id, processing.Id);
        Assert.Equal(1, processing.Attempt);
        Assert.Equal(await Db.ScalarAsync<DateTime>("SELECT leased_at FROM {schema}.queue WHERE job_id = @id", ("id", leased.Id)),
            processing.StartedAt);
        Assert.Equal(await Db.ScalarAsync<Guid>("SELECT lease_owner FROM {schema}.queue WHERE job_id = @id", ("id", leased.Id)),
            processing.ServerId);
    }
}

public sealed class QueueListsTests_Direct(PostgresFixture pg) : QueueListsTests(pg, ConnectionMode.Direct);
public sealed class QueueListsTests_Bouncer(PostgresFixture pg) : QueueListsTests(pg, ConnectionMode.Bouncer);
