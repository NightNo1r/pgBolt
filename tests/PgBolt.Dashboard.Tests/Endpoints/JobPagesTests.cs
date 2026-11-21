using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Dashboard.Views;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

public sealed partial class JobPagesTests(PostgresFixture pg)
{
    private static readonly Guid Server = Guid.Parse("00000000-0000-0000-0000-0000000000f3");
    private static readonly JsonElement EmptyArgs = JsonDocument.Parse("{}").RootElement;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static PgBoltStore Core(DashboardHost host) => host.Services.GetRequiredService<PgBoltStore>();

    private static IJobClient Client(DashboardHost host) => host.Services.GetRequiredService<IJobClient>();

    private static JobSeeder Seeder(TestDb db, DashboardHost host) => new(db, Core(host));

    private static Task PublishAsync(DashboardHost host) =>
        Core(host).PublishJobTypesAsync([new JobTypeRecord("dash.job", """{"type":"object"}""", "default", 3, 0, null)], "tests", Ct);

    private static async Task<Guid> EnqueueAsync(DashboardHost host, JsonElement args, int maxAttempts = 3) =>
        (await Client(host).EnqueueAsync("dash.job", args, new EnqueueOptions { MaxAttempts = maxAttempts }, Ct)).Id!.Value.Value;

    /// <summary>Одна задача в состоянии вкладки kind; Failed / Cancelled — с событием терминального перехода.</summary>
    private static async Task<Guid> SeedOneAsync(TestDb db, DashboardHost host, JobState kind)
    {
        var seeder = Seeder(db, host);
        switch (kind)
        {
            case JobState.Processing:
                await seeder.SeedJobsAsync(1, "dash.job", "default", 0, "enqueued");
                return (await seeder.LeaseAsync()).Id;
            case JobState.Failed or JobState.Cancelled:
                var state = (short)(kind == JobState.Failed ? 3 : 4);
                var id = (await seeder.SeedJobsAsync(1, "dash.job", "default", state, "none"))[0];
                await db.ExecAsync("INSERT INTO {schema}.job_events (job_id, at, kind) SELECT id, finished_at, @kind FROM {schema}.jobs WHERE id = @id",
                    ("id", id), ("kind", (short)(kind == JobState.Failed ? 2 : 4)));
                return id;
            default:
                var (jobState, row) = kind switch
                {
                    JobState.Scheduled => ((short)0, "scheduled"),
                    JobState.Enqueued => ((short)0, "enqueued"),
                    JobState.Awaiting => ((short)0, "awaiting"),
                    _ => ((short)2, "none"),
                };
                return (await seeder.SeedJobsAsync(1, "dash.job", "default", jobState, row))[0];
        }
    }

    [Theory]
    [InlineData(JobState.Scheduled)]
    [InlineData(JobState.Enqueued)]
    [InlineData(JobState.Processing)]
    [InlineData(JobState.Awaiting)]
    [InlineData(JobState.Succeeded)]
    [InlineData(JobState.Failed)]
    [InlineData(JobState.Cancelled)]
    public async Task Each_list_tab_renders(JobState kind)
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var id = await SeedOneAsync(db, host, kind);
        var name = kind.ToString().ToLowerInvariant();

        var page = await BodyAsync(host.GetAsync($"/pgbolt/jobs/{name}" + (kind == JobState.Enqueued ? "?queue=default" : "")));

        Assert.Contains($"<a href=\"jobs/{name}\" aria-current=\"page\">", page, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(page, "aria-current=\"page\""));
        Assert.Contains($"href=\"jobs/{id}\"", page, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Enqueued_without_queue_offers_queue_choice()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await db.ExecAsync("INSERT INTO {schema}.queue_stats VALUES ('default', 0, 1, 0, 0, now()), ('отчёты q', 0, 0, 0, 0, now())");
        await using var host = await DashboardHost.StartAsync(db);
        var id = await SeedOneAsync(db, host, JobState.Enqueued);

        foreach (var path in new[] { "/pgbolt/jobs/enqueued", "/pgbolt/jobs/enqueued?queue=" })
        {
            var page = await BodyAsync(host.GetAsync(path));
            Assert.Contains("<a href=\"jobs/enqueued?queue=default\" hx-push-url=\"true\">", page, StringComparison.Ordinal);
            Assert.Contains($"<a href=\"jobs/enqueued?queue={Uri.EscapeDataString("отчёты q")}\" hx-push-url=\"true\">", page,
                StringComparison.Ordinal);
            Assert.DoesNotContain("<table", page, StringComparison.Ordinal);
        }

        var list = await BodyAsync(host.GetAsync("/pgbolt/jobs/enqueued?queue=default"));
        Assert.Contains("<table", list, StringComparison.Ordinal);
        Assert.Contains($"href=\"jobs/{id}\"", list, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Failed_and_cancelled_tabs_are_captioned_with_events_retention()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using (var week = await DashboardHost.StartAsync(db, core: o => o.EventsRetention = TimeSpan.FromDays(7)))
        {
            Assert.Contains("Transitions over 7 days", await BodyAsync(week.GetAsync("/pgbolt/jobs/failed")), StringComparison.Ordinal);
            Assert.Contains("Transitions over 7 days", await BodyAsync(week.GetAsync("/pgbolt/jobs/cancelled")), StringComparison.Ordinal);
            Assert.DoesNotContain("Transitions over", await BodyAsync(week.GetAsync("/pgbolt/jobs/succeeded")), StringComparison.Ordinal);
        }

        await using var forever = await DashboardHost.StartAsync(db, core: o => o.EventsRetention = null);
        Assert.Contains("Transitions over the retention period", await BodyAsync(forever.GetAsync("/pgbolt/jobs/failed")), StringComparison.Ordinal);
        Assert.Contains("Transitions over the retention period", await BodyAsync(forever.GetAsync("/pgbolt/jobs/cancelled")), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Lease_expired_without_message_is_explained()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        var id = await EnqueueAsync(host, EmptyArgs, maxAttempts: 1);
        await Seeder(db, host).LeaseAsync();
        await db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() - interval '1 second' WHERE job_id = @id", ("id", id));
        await Core(host).SweepExpiredLeasesAsync(100, Ct);

        var row = RowRegex().Matches(await BodyAsync(host.GetAsync("/pgbolt/jobs/failed"))).Single(m => m.Value.Contains(id.ToString(),
            StringComparison.Ordinal)).Value;

        Assert.Contains("Lease expired on the final attempt", row, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Incomplete_page_offers_continue_search()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db,
            builder: b => b.Services.AddSingleton<TimeProvider>(new StepTimeProvider { Step = TimeSpan.FromSeconds(1) }));
        var start = DateTime.UtcNow.AddHours(1);
        var seeder = Seeder(db, host);
        await seeder.SeedJobsAsync(4500, "noise", "default", 0, "scheduled", start, runAtStep: TimeSpan.FromMilliseconds(1));
        await seeder.SeedJobsAsync(1, "needle", "default", 0, "scheduled", start.AddHours(1));

        var page = await BodyAsync(host.GetAsync("/pgbolt/jobs/scheduled?type=needle"));

        Assert.Matches("Scanned through <time datetime=\"[^\"]+\">[^<]+</time>", page);
        Assert.Matches("<a href=\"jobs/scheduled\\?type=needle&amp;after=[^\"]+\" hx-push-url=\"true\">Continue search</a>", page);
    }

    [Fact]
    public async Task Filters_and_cursor_live_in_query_string()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await Seeder(db, host).SeedJobsAsync(55, "dash.job", "default", 0, "scheduled", runAtStep: TimeSpan.FromMilliseconds(1));

        var page = await BodyAsync(host.GetAsync("/pgbolt/jobs/scheduled?queue=default&type=dash.job"));

        var form = Regex.Match(page, "<form[^>]*>.*?</form>", RegexOptions.Singleline).Value;
        Assert.Contains("method=\"get\"", form, StringComparison.Ordinal);
        Assert.Contains("hx-push-url=\"true\"", form, StringComparison.Ordinal);
        Assert.Contains("name=\"queue\" value=\"default\"", form, StringComparison.Ordinal);
        Assert.Contains("name=\"type\" value=\"dash.job\"", form, StringComparison.Ordinal);
        Assert.Equal(50, JobLinkRegex().Count(page));
        var next = Regex.Match(page, "<a href=\"(jobs/scheduled\\?queue=default&amp;type=dash.job&amp;after=[^\"]+)\" hx-push-url=\"true\">Next page</a>");
        Assert.True(next.Success);

        var second = await BodyAsync(host.GetAsync("/pgbolt/" + WebUtility.HtmlDecode(next.Groups[1].Value)));

        Assert.Equal(5, JobLinkRegex().Count(second));
        Assert.DoesNotContain("Next page", second, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Invalid_cursor_is_400_fragment()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        using var response = await host.GetAsync("/pgbolt/jobs/failed?after=!!", htmx: true);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.DoesNotContain("<!DOCTYPE", await response.Content.ReadAsStringAsync(Ct), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Unknown_job_is_404()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        using var job = await host.GetAsync($"/pgbolt/jobs/{Guid.CreateVersion7()}");
        using var list = await host.GetAsync("/pgbolt/jobs/bogus");

        await AssertNotFoundPageAsync(job);
        await AssertNotFoundPageAsync(list);
    }

    [Fact]
    public async Task Card_shows_args_events_parent_batch_and_continuations()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        var wf = Client(host).Workflow();
        IJobStep member = null!, job = null!;
        var batch = wf.StartBatch(b =>
        {
            member = b.Enqueue("dash.job", EmptyArgs);
            job = member.ContinueWith("dash.job", JsonDocument.Parse("""{"k":"значение"}""").RootElement);
            for (var i = 0; i < 55; i++)
                job.ContinueWith("dash.job", EmptyArgs);
        });
        await wf.SubmitAsync(Ct);
        var id = job.Id.Value;
        await db.ExecAsync("""
            INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message) VALUES
                (@id, now() - interval '1 minute', 1, 1, 'first try'), (@id, now(), 5, NULL, NULL)
            """, ("id", id));

        var card = await BodyAsync(host.GetAsync($"/pgbolt/jobs/{id}"));

        Assert.Contains($"href=\"jobs/{member.Id.Value}\"", card, StringComparison.Ordinal);
        Assert.Contains($"href=\"batches/{batch.Id.Value}\"", card, StringComparison.Ordinal);
        Assert.Contains("&quot;k&quot;: &quot;значение&quot;", card, StringComparison.Ordinal);
        Assert.Contains("first try", card, StringComparison.Ordinal);
        Assert.Contains("Retry", card, StringComparison.Ordinal);
        Assert.Contains("Requeued", card, StringComparison.Ordinal);
        var lazy = Tag(card, $"hx-get=\"jobs/{id}/continuations\"");
        Assert.StartsWith("<ul", lazy, StringComparison.Ordinal);
        Assert.Contains("hx-trigger=\"load\"", lazy, StringComparison.Ordinal);
        Assert.Contains("hx-target=\"this\"", lazy, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"innerHTML\"", lazy, StringComparison.Ordinal);
        Assert.Contains($"hx-get=\"jobs/{id}/batch-continuations\"", card, StringComparison.Ordinal);
        Assert.DoesNotContain("hx-disinherit", card, StringComparison.Ordinal);

        var first = await BodyAsync(host.GetAsync($"/pgbolt/jobs/{id}/continuations", htmx: true));

        Assert.DoesNotContain("<title>", first, StringComparison.Ordinal);
        Assert.Equal(50, JobLinkRegex().Count(first));
        Assert.All(AnchorRegex().Matches(first).Select(m => m.Value), a =>
        {
            Assert.DoesNotContain("hx-target", a, StringComparison.Ordinal);
            Assert.DoesNotContain("hx-swap", a, StringComparison.Ordinal);
        });
        Assert.DoesNotContain("hx-disinherit", first, StringComparison.Ordinal);
        var more = Regex.Match(first, $"<[a-z]+ [^>]*hx-get=\"(jobs/{id}/continuations\\?after=[^\"]+)\"[^>]*>");
        Assert.True(more.Success);
        Assert.Contains("hx-target=\"this\"", more.Value, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"outerHTML\"", more.Value, StringComparison.Ordinal);

        var rest = await BodyAsync(host.GetAsync("/pgbolt/" + WebUtility.HtmlDecode(more.Groups[1].Value), htmx: true));

        Assert.Equal(5, JobLinkRegex().Count(rest));
        Assert.DoesNotContain("?after=", rest, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Card_escapes_markup_in_args_message_and_exception()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        var id = await EnqueueAsync(host, JsonDocument.Parse("""{"x":"<script>alert(1)</script>"}""").RootElement);
        var leased = await Seeder(db, host).LeaseAsync();
        await Core(host).FailAsync(id, leased.LeaseId, Server, JobEventKind.Failed, "\"><img src=x onerror=alert(1)>", "<b>trace</b>", Ct);

        foreach (var body in new[] { await BodyAsync(host.GetAsync($"/pgbolt/jobs/{id}")), await BodyAsync(host.GetAsync("/pgbolt/jobs/failed")) })
        {
            Assert.DoesNotContain("<script>alert", body, StringComparison.Ordinal);
            Assert.DoesNotContain("<img src=x", body, StringComparison.Ordinal);
            Assert.Contains("&lt;img", body, StringComparison.Ordinal);
        }
        var card = await BodyAsync(host.GetAsync($"/pgbolt/jobs/{id}"));
        Assert.Contains("&lt;script&gt;", card, StringComparison.Ordinal);
        Assert.Contains("&lt;b&gt;trace", card, StringComparison.Ordinal);
        Assert.DoesNotContain("<b>trace", card, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Batch_card_shows_progress_members_nested_and_continuations()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        var wf = Client(host).Workflow();
        IJobStep member = null!, continuation = null!;
        IBatchStep inner = null!, nested = null!, batchContinuation = null!;
        var outer = wf.StartBatch(b =>
        {
            member = b.Enqueue("dash.job", EmptyArgs);
            inner = member.ContinueWithBatch(c =>
            {
                c.Enqueue("dash.job", EmptyArgs);
                c.Enqueue("dash.job", EmptyArgs);
                nested = c.StartBatch(n => n.Enqueue("dash.job", EmptyArgs));
            });
            continuation = inner.ContinueWith("dash.job", EmptyArgs);
            batchContinuation = inner.ContinueWithBatch(d => d.Enqueue("dash.job", EmptyArgs));
        });
        await wf.SubmitAsync(Ct);
        var id = inner.Id.Value;
        await db.ExecAsync("UPDATE {schema}.batches SET succeeded = 1 WHERE id = @id", ("id", id));
        var info = (await Client(host).GetBatchAsync(inner.Id, Ct))!;

        var card = await BodyAsync(host.GetAsync($"/pgbolt/batches/{id}"));

        Assert.Matches("<main id=\"main\">\\s*<article", card);
        Assert.Contains($"href=\"batches/{outer.Id.Value}\"", card, StringComparison.Ordinal);
        Assert.Contains($"href=\"jobs/{member.Id.Value}\"", card, StringComparison.Ordinal);
        Assert.Contains($"1 / {info.Total}", card, StringComparison.Ordinal);
        foreach (var part in new[] { "jobs", "nested", "continuations", "batch-continuations" })
        {
            var lazy = Tag(card, $"hx-get=\"batches/{id}/{part}\"");
            Assert.Contains("hx-trigger=\"load\"", lazy, StringComparison.Ordinal);
            Assert.Contains("hx-target=\"this\"", lazy, StringComparison.Ordinal);
            Assert.Contains("hx-swap=\"innerHTML\"", lazy, StringComparison.Ordinal);
        }

        var jobs = await BodyAsync(host.GetAsync($"/pgbolt/batches/{id}/jobs", htmx: true));
        var nestedList = await BodyAsync(host.GetAsync($"/pgbolt/batches/{id}/nested", htmx: true));
        var continuations = await BodyAsync(host.GetAsync($"/pgbolt/batches/{id}/continuations", htmx: true));
        var batchContinuations = await BodyAsync(host.GetAsync($"/pgbolt/batches/{id}/batch-continuations", htmx: true));

        Assert.Equal(2, JobLinkRegex().Count(jobs));
        Assert.Contains($"href=\"batches/{nested.Id.Value}\"", nestedList, StringComparison.Ordinal);
        Assert.Contains($"href=\"jobs/{continuation.Id.Value}\"", continuations, StringComparison.Ordinal);
        Assert.Contains($"href=\"batches/{batchContinuation.Id.Value}\"", batchContinuations, StringComparison.Ordinal);
        Assert.All(new[] { jobs, nestedList, continuations, batchContinuations },
            f => Assert.DoesNotContain("<title>", f, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Batches_tab_filters_by_state()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        var ids = new List<BatchId>();
        for (var i = 0; i < 2; i++)
        {
            var batch = Client(host).Workflow().StartBatch(b => b.Enqueue("dash.job", EmptyArgs));
            await batch.SubmitAsync(Ct);
            ids.Add(batch.Id);
        }
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client(host).CancelBatchAsync(ids[0], Ct));
        await ResolverPasses.SettleAsync(Core(host), cancelMembers: true, Ct);

        var all = await BodyAsync(host.GetAsync("/pgbolt/batches"));
        var cancelled = await BodyAsync(host.GetAsync("/pgbolt/batches?state=Cancelled"));
        using var invalid = await host.GetAsync("/pgbolt/batches?state=bogus");

        Assert.All(ids, b => Assert.Contains($"href=\"batches/{b.Value}\"", all, StringComparison.Ordinal));
        Assert.Contains($"href=\"batches/{ids[0].Value}\"", cancelled, StringComparison.Ordinal);
        Assert.DoesNotContain($"href=\"batches/{ids[1].Value}\"", cancelled, StringComparison.Ordinal);
        Assert.Contains("<a href=\"batches?state=Cancelled\" aria-current=\"page\">", cancelled, StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
    }

    [Fact]
    public async Task Recurring_with_slash_and_cyrillic_id_opens()
    {
        const string Id = "отчёт/ежедневный ?x";
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        await host.Services.GetRequiredService<IRecurringJobManager>().AddOrUpdateAsync(Id, "dash.job", "0 0 * * *", EmptyArgs, ct: Ct);
        var runs = await Seeder(db, host).SeedJobsAsync(55, "dash.job", "default", 2, "none");
        await db.ExecAsync("UPDATE {schema}.jobs SET recurring_id = @id WHERE id = ANY(@ids)", ("id", Id), ("ids", runs));

        var list = await BodyAsync(host.GetAsync("/pgbolt/recurring"));
        var link = Regex.Match(list, "href=\"(recurring/item\\?id=[^\"]+)\"").Groups[1].Value;

        Assert.Equal("recurring/item?id=" + Uri.EscapeDataString(Id), link);
        Assert.StartsWith("recurring/item?id=%D0%BE", link, StringComparison.Ordinal);
        var card = await BodyAsync(host.GetAsync("/pgbolt/" + link));
        Assert.Contains(Id, card, StringComparison.Ordinal);
        Assert.Equal(50, JobLinkRegex().Count(card));
        var next = Regex.Match(card, "<a href=\"(recurring/item\\?id=[^\"&]+&amp;after=[^\"]+)\" hx-push-url=\"true\">Next page</a>");
        Assert.True(next.Success);

        var second = await BodyAsync(host.GetAsync("/pgbolt/" + WebUtility.HtmlDecode(next.Groups[1].Value)));

        Assert.Equal(5, JobLinkRegex().Count(second));
    }

    [Fact]
    public async Task Unknown_recurring_or_batch_is_404()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        using var recurring = await host.GetAsync("/pgbolt/recurring/item?id=" + Uri.EscapeDataString("нет/такого"));
        using var batch = await host.GetAsync($"/pgbolt/batches/{Guid.CreateVersion7()}");
        using var noId = await host.GetAsync("/pgbolt/recurring/item");

        await AssertNotFoundPageAsync(recurring);
        await AssertNotFoundPageAsync(batch);
        Assert.Equal(HttpStatusCode.BadRequest, noId.StatusCode);
    }

    [Theory]
    [InlineData("jobs/{0}/continuations")]
    [InlineData("jobs/{0}/batch-continuations")]
    [InlineData("batches/{0}/jobs")]
    [InlineData("batches/{0}/nested")]
    [InlineData("batches/{0}/continuations")]
    [InlineData("batches/{0}/batch-continuations")]
    public async Task Fragment_of_unknown_parent_is_404(string path)
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        using var response = await host.GetAsync("/pgbolt/" + string.Format(CultureInfo.InvariantCulture, path, Guid.CreateVersion7()), htmx: true);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    private static async Task<string> BodyAsync(Task<HttpResponseMessage> request)
    {
        using var response = await request;
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return await response.Content.ReadAsStringAsync(Ct);
    }

    /// <summary>404 страницей ошибки dashboard, а не промахом маршрута.</summary>
    private static async Task AssertNotFoundPageAsync(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Contains("Error 404", await response.Content.ReadAsStringAsync(Ct), StringComparison.Ordinal);
    }

    /// <summary>Открывающий тег элемента с атрибутом attribute.</summary>
    private static string Tag(string html, string attribute) =>
        Regex.Match(html, "<[a-z]+[^>]*" + Regex.Escape(attribute) + "[^>]*>").Value;

    [GeneratedRegex("href=\"jobs/[0-9a-f]{8}-[0-9a-f-]{27}\"")]
    private static partial Regex JobLinkRegex();

    [GeneratedRegex("<a [^>]*>")]
    private static partial Regex AnchorRegex();

    [GeneratedRegex("<tr>.*?</tr>", RegexOptions.Singleline)]
    private static partial Regex RowRegex();
}
