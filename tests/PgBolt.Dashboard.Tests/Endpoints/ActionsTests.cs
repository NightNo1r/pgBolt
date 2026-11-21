using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

public sealed partial class ActionsTests(PostgresFixture pg)
{
    private static readonly JsonElement EmptyArgs = JsonDocument.Parse("{}").RootElement;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static PgBoltStore Core(DashboardHost host) => host.Services.GetRequiredService<PgBoltStore>();

    private static IJobClient Client(DashboardHost host) => host.Services.GetRequiredService<IJobClient>();

    private static IRecurringJobManager Recurring(DashboardHost host) => host.Services.GetRequiredService<IRecurringJobManager>();

    private static JobSeeder Seeder(TestDb db, DashboardHost host) => new(db, Core(host));

    private static Task PublishAsync(DashboardHost host, string name = "dash.job") =>
        Core(host).PublishJobTypesAsync([new JobTypeRecord(name, """{"type":"object"}""", "default", 3, 0, null)], "tests", Ct);

    private static async Task<Guid> SeedAsync(TestDb db, DashboardHost host, short state, string queueRow) =>
        (await Seeder(db, host).SeedJobsAsync(1, "dash.job", "default", state, queueRow))[0];

    private static async Task<JobState> StateAsync(DashboardHost host, Guid id) => (await Client(host).GetAsync(new JobId(id), Ct))!.State;

    private static Task<HttpResponseMessage> PostAsync(DashboardHost host, string path, params string[] ids) =>
        host.PostAsync("/pgbolt/" + path, ids.Select(id => new KeyValuePair<string, string>("ids", id)));

    [Fact]
    public async Task Cancel_trigger_requeue_single_job()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var id = await SeedAsync(db, host, 0, "scheduled");

        var (trigger, triggered) = await ActAsync(PostAsync(host, $"actions/jobs/{id}/trigger"));

        Assert.StartsWith("<tr", trigger, StringComparison.Ordinal);
        Assert.DoesNotContain("<title>", trigger, StringComparison.Ordinal);
        Assert.Contains(">Enqueued</span>", trigger, StringComparison.Ordinal);
        Assert.Equal("triggered", triggered);
        Assert.Equal(JobState.Enqueued, await StateAsync(host, id));

        var (cancel, cancelled) = await ActAsync(PostAsync(host, $"actions/jobs/{id}/cancel"));

        Assert.Contains(">Cancelled</span>", cancel, StringComparison.Ordinal);
        Assert.Equal("cancelled", cancelled);
        var job = (await Client(host).GetAsync(new JobId(id), Ct))!;
        Assert.Equal(JobState.Cancelled, job.State);
        Assert.Contains(job.Events, e => e.Kind == JobEventKind.Cancelled && e.Message == "dashboard: anonymous");

        var failed = await SeedAsync(db, host, 3, "none");
        var (requeue, requeued) = await ActAsync(PostAsync(host, $"actions/jobs/{failed}/requeue"));

        Assert.Contains(">Enqueued</span>", requeue, StringComparison.Ordinal);
        Assert.Equal("requeued", requeued);
        Assert.Equal(JobState.Enqueued, await StateAsync(host, failed));
    }

    [Theory]
    [InlineData("scheduled", "cancel")]
    [InlineData("failed", "requeue")]
    public async Task Row_after_action_keeps_tab_columns(string tab, string action)
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var id = tab == "failed" ? await SeedAsync(db, host, 3, "none") : await SeedAsync(db, host, 0, "scheduled");
        await db.ExecAsync("INSERT INTO {schema}.job_events (job_id, at, kind) SELECT id, finished_at, 2 FROM {schema}.jobs WHERE id = @id AND state = 3",
            ("id", id));
        var page = await BodyAsync(host.GetAsync($"/pgbolt/jobs/{tab}"));
        var header = Regex.Match(page, "<thead>.*?</thead>", RegexOptions.Singleline).Value;
        var url = Regex.Match(Row(page, id), $"hx-post=\"(actions/jobs/{id}/{action}[^\"]*)\"").Groups[1].Value;

        var (row, _) = await ActAsync(PostAsync(host, WebUtility.HtmlDecode(url)));

        Assert.Equal(Regex.Count(header, "<th[ >]"), Regex.Count(row, "<td[ >]"));
        Assert.Contains($"hx-post=\"{url}\"".Replace(action, action == "cancel" ? "requeue" : "cancel", StringComparison.Ordinal), row,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Card_view_returns_card()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var id = await SeedAsync(db, host, 0, "scheduled");

        var (card, _) = await ActAsync(PostAsync(host, $"actions/jobs/{id}/trigger?view=card"));

        Assert.StartsWith("<article", card, StringComparison.Ordinal);
        Assert.DoesNotContain("<title>", card, StringComparison.Ordinal);
        Assert.Contains($"hx-post=\"actions/jobs/{id}/cancel?view=card\"", card, StringComparison.Ordinal);
        Assert.Contains("hx-target=\"closest article\"", card, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Race_is_reported_not_failed()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var succeeded = await SeedAsync(db, host, 2, "none");
        var enqueued = await SeedAsync(db, host, 0, "enqueued");

        var (_, cancel) = await ActAsync(PostAsync(host, $"actions/jobs/{succeeded}/cancel"));
        var (_, requeue) = await ActAsync(PostAsync(host, $"actions/jobs/{enqueued}/requeue"));

        Assert.Equal("job already finished", cancel);
        Assert.Equal("not applicable: job has not finished", requeue);
        Assert.Equal(JobState.Succeeded, await StateAsync(host, succeeded));
    }

    [Fact]
    public async Task Unknown_job_action_is_404()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        using var response = await PostAsync(host, $"actions/jobs/{Guid.CreateVersion7()}/cancel");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Bulk_actions_report_done_and_not_applicable()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var scheduled = await Seeder(db, host).SeedJobsAsync(3, "dash.job", "default", 0, "scheduled");
        var succeeded = await SeedAsync(db, host, 2, "none");

        var summary = await BodyAsync(PostAsync(host, "actions/jobs/trigger", [.. scheduled.Select(i => i.ToString()), succeeded.ToString()]));

        Assert.Contains("Applied 3, not applicable 1", summary, StringComparison.Ordinal);
        Assert.Contains(succeeded.ToString(), summary, StringComparison.Ordinal);
        Assert.DoesNotContain(scheduled[0].ToString(), summary, StringComparison.Ordinal);
        Assert.DoesNotContain("<title>", summary, StringComparison.Ordinal);
        foreach (var id in scheduled)
            Assert.Equal(JobState.Enqueued, await StateAsync(host, id));

        using var tooMany = await PostAsync(host, "actions/jobs/cancel", [.. Enumerable.Range(0, 101).Select(_ => Guid.CreateVersion7().ToString())]);
        using var invalid = await PostAsync(host, "actions/jobs/cancel", "not-a-guid");
        Assert.Equal(HttpStatusCode.BadRequest, tooMany.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);

        var once = await SeedAsync(db, host, 0, "scheduled");
        var repeated = await BodyAsync(PostAsync(host, "actions/jobs/trigger", once.ToString(), once.ToString()));
        Assert.Contains("Applied 1, not applicable 0", repeated, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Batch_cancel()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        var wf = Client(host).Workflow();
        var batch = wf.StartBatch(b =>
        {
            b.Enqueue("dash.job", EmptyArgs);
            b.Enqueue("dash.job", EmptyArgs);
        });
        await wf.SubmitAsync(Ct);
        var path = $"actions/batches/{batch.Id.Value}/cancel";
        var button = Tag(await BodyAsync(host.GetAsync($"/pgbolt/batches/{batch.Id.Value}")), $"hx-post=\"{path}\"");
        Assert.Contains("hx-target=\"closest article\"", button, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"outerHTML\"", button, StringComparison.Ordinal);

        var (card, first) = await ActAsync(PostAsync(host, path));

        Assert.StartsWith("<article", card, StringComparison.Ordinal);
        Assert.DoesNotContain("<title>", card, StringComparison.Ordinal);
        Assert.Equal("cancellation requested", first);
        Assert.True((await Client(host).GetBatchAsync(batch.Id, Ct))!.CancellationRequested);
        Assert.DoesNotContain($"hx-post=\"{path}\"", card, StringComparison.Ordinal);

        var (_, second) = await ActAsync(PostAsync(host, path));
        Assert.Equal("cancellation already requested", second);

        await ResolverPasses.SettleAsync(Core(host), cancelMembers: true, Ct);
        var (_, finished) = await ActAsync(PostAsync(host, path));
        Assert.Equal("batch already finished", finished);

        using var unknown = await PostAsync(host, $"actions/batches/{Guid.CreateVersion7()}/cancel");
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);
    }

    [Fact]
    public async Task Recurring_actions_accept_any_id()
    {
        const string Id = "a/b ?c";
        var q = Uri.EscapeDataString(Id);
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        await Recurring(host).AddOrUpdateAsync(Id, "dash.job", "0 0 * * *", EmptyArgs, ct: Ct);

        var list = await BodyAsync(host.GetAsync("/pgbolt/recurring"));
        Assert.Contains($"hx-post=\"actions/recurring/pause?id={q}&amp;view=row\"", list, StringComparison.Ordinal);
        Assert.Contains($"hx-post=\"actions/recurring/delete?id={q}&amp;view=row\"", list, StringComparison.Ordinal);

        var (paused, _) = await ActAsync(PostAsync(host, $"actions/recurring/pause?id={q}"));
        Assert.StartsWith("<tr", paused, StringComparison.Ordinal);
        Assert.Contains($"actions/recurring/resume?id={q}", paused, StringComparison.Ordinal);
        Assert.True((await Recurring(host).GetAsync(Id, Ct))!.Paused);

        var (resumed, _) = await ActAsync(PostAsync(host, $"actions/recurring/resume?id={q}&view=card"));
        Assert.StartsWith("<article", resumed, StringComparison.Ordinal);
        Assert.Contains($"hx-post=\"actions/recurring/pause?id={q}&amp;view=card\"", resumed, StringComparison.Ordinal);
        Assert.False((await Recurring(host).GetAsync(Id, Ct))!.Paused);

        var (_, triggered) = await ActAsync(PostAsync(host, $"actions/recurring/trigger?id={q}"));
        var job = Guid.Parse(GuidRegex().Match(triggered).Value);
        Assert.Equal(Id, (await Client(host).GetAsync(new JobId(job), Ct))!.RecurringJobId);

        var (removed, _) = await ActAsync(PostAsync(host, $"actions/recurring/delete?id={q}"));
        Assert.StartsWith("<tr", removed, StringComparison.Ordinal);
        Assert.Contains("schedule deleted", removed, StringComparison.Ordinal);
        Assert.Null(await Recurring(host).GetAsync(Id, Ct));
        using var again = await PostAsync(host, $"actions/recurring/delete?id={q}");
        using var noId = await PostAsync(host, "actions/recurring/pause");
        Assert.Equal(HttpStatusCode.NotFound, again.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, noId.StatusCode);

        // Code-first строка (owner) не удаляется; тип, исчезнувший из job_types, — уведомление вместо ошибки.
        await PublishAsync(host, "dash.gone");
        await Recurring(host).AddOrUpdateAsync("owned", "dash.job", "0 0 * * *", EmptyArgs, ct: Ct);
        await Recurring(host).AddOrUpdateAsync("gone", "dash.gone", "0 0 * * *", EmptyArgs, ct: Ct);
        await db.ExecAsync("UPDATE {schema}.recurring_jobs SET owner = 'svc' WHERE id = 'owned'");
        await db.ExecAsync("DELETE FROM {schema}.job_types WHERE name = 'dash.gone'");

        var owners = await BodyAsync(host.GetAsync("/pgbolt/recurring"));
        Assert.Contains("actions/recurring/pause?id=owned", owners, StringComparison.Ordinal);
        Assert.DoesNotContain("actions/recurring/delete?id=owned", owners, StringComparison.Ordinal);

        var (_, owned) = await ActAsync(PostAsync(host, "actions/recurring/delete?id=owned"));
        Assert.Contains("schedule defined in code (owner svc)", owned, StringComparison.Ordinal);
        Assert.NotNull(await Recurring(host).GetAsync("owned", Ct));

        var (row, gone) = await ActAsync(PostAsync(host, "actions/recurring/trigger?id=gone"));
        Assert.StartsWith("<tr", row, StringComparison.Ordinal);
        Assert.Contains("job type dash.gone is not published", gone, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Bulk_buttons_have_own_target_and_do_not_capture_links()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var id = await SeedAsync(db, host, 0, "scheduled");

        var page = await BodyAsync(host.GetAsync("/pgbolt/jobs/scheduled"));

        var form = Regex.Matches(page, "<form[^>]*>.*?</form>", RegexOptions.Singleline).Single(m => m.Value.Contains("name=\"ids\"",
            StringComparison.Ordinal)).Value;
        var open = Regex.Match(form, "<form[^>]*>").Value;
        foreach (var attribute in new[] { "hx-post", "hx-target", "hx-swap" })
            Assert.DoesNotContain(attribute, open, StringComparison.Ordinal);
        Assert.Contains($"name=\"ids\" value=\"{id}\"", form, StringComparison.Ordinal);
        foreach (var action in new[] { "cancel", "trigger", "requeue" })
        {
            var button = Tag(form, $"hx-post=\"actions/jobs/{action}\"");
            Assert.StartsWith("<button", button, StringComparison.Ordinal);
            Assert.Contains("type=\"button\"", button, StringComparison.Ordinal);
            Assert.Contains("hx-target=\"#bulk-result\"", button, StringComparison.Ordinal);
            Assert.Contains("hx-swap=\"innerHTML\"", button, StringComparison.Ordinal);
        }
        Assert.All(Regex.Matches(form, "<a [^>]*href=\"jobs/[^\"]*\"[^>]*>").Select(m => m.Value), a =>
        {
            Assert.DoesNotContain("hx-target", a, StringComparison.Ordinal);
            Assert.DoesNotContain("hx-swap", a, StringComparison.Ordinal);
        });
        Assert.DoesNotContain("hx-disinherit", page, StringComparison.Ordinal);
        Assert.DoesNotContain("bulk-result", form.Replace("hx-target=\"#bulk-result\"", "", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("<div id=\"bulk-result\"", page, StringComparison.Ordinal);

        var summary = await BodyAsync(PostAsync(host, "actions/jobs/trigger", id.ToString()));
        Assert.Contains("Applied 1, not applicable 0", summary, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Buttons_render_only_for_applicable_states()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var scheduled = await SeedAsync(db, host, 0, "scheduled");
        var failed = await SeedAsync(db, host, 3, "none");
        await db.ExecAsync("INSERT INTO {schema}.job_events (job_id, at, kind) SELECT id, finished_at, 2 FROM {schema}.jobs WHERE id = @id",
            ("id", failed));
        var succeeded = await SeedAsync(db, host, 2, "none");
        await SeedAsync(db, host, 0, "enqueued");
        var processing = (await Seeder(db, host).LeaseAsync()).Id;

        var actions = new Dictionary<Guid, string>
        {
            [scheduled] = await BodyAsync(host.GetAsync("/pgbolt/jobs/scheduled")),
            [failed] = await BodyAsync(host.GetAsync("/pgbolt/jobs/failed")),
            [succeeded] = await BodyAsync(host.GetAsync("/pgbolt/jobs/succeeded")),
            [processing] = await BodyAsync(host.GetAsync("/pgbolt/jobs/processing")),
        }.ToDictionary(p => p.Key, p => ActionRegex().Matches(Row(p.Value, p.Key)).Select(m => m.Groups["a"].Value).Order().ToArray());

        Assert.Equal(["cancel", "trigger"], actions[scheduled]);
        Assert.Equal(["requeue"], actions[failed]);
        Assert.Empty(actions[succeeded]);
        Assert.Equal(["cancel"], actions[processing]);
        var button = Tag(Row(await BodyAsync(host.GetAsync("/pgbolt/jobs/scheduled")), scheduled), $"hx-post=\"actions/jobs/{scheduled}/cancel?view=row&amp;kind=scheduled\"");
        Assert.Contains("hx-target=\"closest tr\"", button, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"outerHTML\"", button, StringComparison.Ordinal);
        var card = await BodyAsync(host.GetAsync($"/pgbolt/jobs/{failed}"));
        Assert.Contains($"hx-post=\"actions/jobs/{failed}/requeue?view=card\"", card, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Read_only_hides_buttons_and_rejects_post()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, o => o.ReadOnly = true);
        var id = await SeedAsync(db, host, 0, "scheduled");

        await AssertNoActionsAsync(host, id);
        using var response = await PostAsync(host, $"actions/jobs/{id}/trigger");
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal(JobState.Scheduled, await StateAsync(host, id));
    }

    [Fact]
    public async Task Actions_policy_denial_hides_buttons_and_rejects_post()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, o => o.ActionsPolicy = "Ops",
            builder: b => TestAuthentication.Add(b.Services, o => o.AddPolicy("Ops", p => p.RequireRole("Ops"))));
        var id = await SeedAsync(db, host, 0, "scheduled");
        host.Client.DefaultRequestHeaders.Add(TestAuthentication.UserHeader, "alice");

        await AssertNoActionsAsync(host, id);
        using (var denied = await PostAsync(host, $"actions/jobs/{id}/trigger"))
            Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);

        host.Client.DefaultRequestHeaders.Add(TestAuthentication.RolesHeader, "Ops");
        using var allowed = await PostAsync(host, $"actions/jobs/{id}/trigger");
        Assert.Equal(HttpStatusCode.OK, allowed.StatusCode);
    }

    [Fact]
    public async Task Csrf_checks_reject_foreign_posts()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        var id = await SeedAsync(db, host, 0, "scheduled");
        var path = $"/pgbolt/actions/jobs/{id}/trigger";

        foreach (var request in new[]
                 {
                     host.PostAsync(path, [], htmx: false),
                     host.PostAsync(path, [], fetchSite: "cross-site"),
                     host.PostAsync(path, [], fetchSite: null, origin: "http://evil"),
                 })
        {
            using var response = await request;
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        }
        Assert.Equal(JobState.Scheduled, await StateAsync(host, id));

        using var same = await host.PostAsync(path, [], fetchSite: null, origin: "http://localhost");
        Assert.Equal(HttpStatusCode.OK, same.StatusCode);
    }

    [Fact]
    public async Task Each_action_is_logged()
    {
        var logs = new ListLogger();
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b => b.Logging.AddProvider(logs));
        var id = await SeedAsync(db, host, 0, "scheduled");

        await ActAsync(PostAsync(host, $"actions/jobs/{id}/trigger"));

        var entry = Assert.Single(logs.Entries, e => e.Level == LogLevel.Information && e.Message.StartsWith("PgBolt dashboard: ",
            StringComparison.Ordinal));
        Assert.Equal($"PgBolt dashboard: anonymous trigger job {id}: triggered.", entry.Message);
    }

    [Fact]
    public async Task Not_found_actions_are_logged()
    {
        var logs = new ListLogger();
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b => b.Logging.AddProvider(logs));
        var batch = Guid.CreateVersion7();

        using (var response = await PostAsync(host, $"actions/batches/{batch}/cancel"))
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        using (var response = await PostAsync(host, "actions/recurring/pause?id=none"))
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        Assert.Equal([$"PgBolt dashboard: anonymous cancel batch {batch}: batch not found.", "PgBolt dashboard: anonymous pause recurring none: schedule not found."],
            logs.Entries.Where(e => e.Category == "PgBolt.Dashboard.Actions").Select(e => e.Message));
    }

    [Fact]
    public async Task Inapplicable_trigger_reads_job_once()
    {
        var reads = 0;
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b =>
            DelegatingJobClient.Replace(b, inner => new CountingClient(inner, () => Interlocked.Increment(ref reads))));
        var id = await SeedAsync(db, host, 0, "enqueued");

        var (row, message) = await ActAsync(PostAsync(host, $"actions/jobs/{id}/trigger"));

        Assert.Equal("not applicable: job is not Scheduled", message);
        Assert.Contains(">Enqueued</span>", row, StringComparison.Ordinal);
        Assert.Equal(1, reads);
    }

    private sealed class CountingClient(IJobClient inner, Action onGet) : DelegatingJobClient(inner)
    {
        public override ValueTask<JobInfo?> GetAsync(JobId id, CancellationToken ct = default)
        {
            onGet();
            return base.GetAsync(id, ct);
        }
    }

    private static async Task AssertNoActionsAsync(DashboardHost host, Guid id)
    {
        var list = await BodyAsync(host.GetAsync("/pgbolt/jobs/scheduled"));
        var card = await BodyAsync(host.GetAsync($"/pgbolt/jobs/{id}"));
        Assert.Contains($"href=\"jobs/{id}\"", list, StringComparison.Ordinal);
        foreach (var page in new[] { list, card })
        {
            Assert.DoesNotContain("hx-post", page, StringComparison.Ordinal);
            Assert.DoesNotContain("name=\"ids\"", page, StringComparison.Ordinal);
        }
    }

    /// <summary>Тело ответа 200 и уведомление из HX-Trigger.</summary>
    private static async Task<(string Body, string Notify)> ActAsync(Task<HttpResponseMessage> request)
    {
        using var response = await request;
        var body = await response.Content.ReadAsStringAsync(Ct);
        Assert.True(response.StatusCode == HttpStatusCode.OK, body);
        using var trigger = JsonDocument.Parse(response.Headers.GetValues("HX-Trigger").Single());
        return (body, trigger.RootElement.GetProperty("pgbolt:notify").GetString()!);
    }

    private static async Task<string> BodyAsync(Task<HttpResponseMessage> request)
    {
        using var response = await request;
        var body = await response.Content.ReadAsStringAsync(Ct);
        Assert.True(response.StatusCode == HttpStatusCode.OK, body);
        return body;
    }

    private static string Row(string page, Guid id) =>
        Regex.Matches(page, "<tr>.*?</tr>", RegexOptions.Singleline).Single(m => m.Value.Contains($"href=\"jobs/{id}\"",
            StringComparison.Ordinal)).Value;

    /// <summary>Открывающий тег элемента с атрибутом attribute.</summary>
    private static string Tag(string html, string attribute) =>
        Regex.Match(html, "<[a-z]+[^>]*" + Regex.Escape(attribute) + "[^>]*>").Value;

    [GeneratedRegex("hx-post=\"actions/jobs/[0-9a-f-]{36}/(?<a>[a-z]+)\\?view=row&amp;kind=[a-z]+\"")]
    private static partial Regex ActionRegex();

    [GeneratedRegex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")]
    private static partial Regex GuidRegex();
}
