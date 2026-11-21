using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Dashboard.Tests.Unit;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

public sealed class EnqueueTests(PostgresFixture pg)
{
    private const string Order = "dash.order";
    private const string EnqueuePath = "/pgbolt/actions/types/enqueue?name=" + Order;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static IJobClient Client(DashboardHost host) => host.Services.GetRequiredService<IJobClient>();

    private static Task PublishAsync(DashboardHost host, string name = Order, string? schema = null) =>
        host.Services.GetRequiredService<PgBoltStore>().PublishJobTypesAsync(
            [new JobTypeRecord(name, schema ?? TestSchemas.OrderJson, "orders", 3, 0, null)], "tests", Ct);

    private static async Task<(HttpStatusCode Status, string Body)> SendAsync(Task<HttpResponseMessage> request)
    {
        using var response = await request;
        return (response.StatusCode, await response.Content.ReadAsStringAsync(Ct));
    }

    private static async Task<string> OkAsync(Task<HttpResponseMessage> request)
    {
        var (status, body) = await SendAsync(request);
        Assert.True(status == HttpStatusCode.OK, body);
        return body;
    }

    private static async Task<string> BadRequestAsync(Task<HttpResponseMessage> request)
    {
        var (status, body) = await SendAsync(request);
        Assert.True(status == HttpStatusCode.BadRequest, body);
        return body;
    }

    private static Guid JobLink(string html) => Guid.Parse(Regex.Match(html, "href=\"jobs/([0-9a-f-]{36})\"").Groups[1].Value);

    /// <summary>Открывающий тег элемента с атрибутом attribute.</summary>
    private static string Tag(string html, string attribute) =>
        Regex.Match(html, "<[a-z]+[^>]*" + Regex.Escape(attribute) + "[^>]*>").Value;

    /// <summary>Подпись поля (label) с элементом name.</summary>
    private static string Label(string html, string name) =>
        Regex.Matches(html, "<label[ >].*?</label>", RegexOptions.Singleline).Single(m => m.Value.Contains($"name=\"{name}\"",
            StringComparison.Ordinal)).Value;

    private static Task<long> JobCountAsync(TestDb db) => db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs");

    [Fact]
    public async Task Form_page_renders_fields_and_options()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var html = await OkAsync(host.GetAsync("/pgbolt/types/enqueue?name=" + Order));

        Assert.Contains("name=\"args.Name\"", html, StringComparison.Ordinal);
        Assert.Matches(new Regex("<fieldset>(?:(?!</fieldset>).)*name=\"args.Ship.City\"", RegexOptions.Singleline), html);
        Assert.Contains("<select name=\"args.Tint\"", html, StringComparison.Ordinal);
        Assert.Contains("type=\"checkbox\"", Tag(html, "name=\"args.Urgent\""), StringComparison.Ordinal);
        Assert.Contains("step=\"1\"", Tag(html, "name=\"args.Count\""), StringComparison.Ordinal);
        Assert.Contains("value=\"orders\"", Tag(html, "name=\"queue\""), StringComparison.Ordinal);
        foreach (var option in new[] { "priority", "delay", "runAt", "uniqueKey", "maxAttempts" })
            Assert.Contains($"name=\"{option}\"", html, StringComparison.Ordinal);
        Assert.Contains($"href=\"types/enqueue?name={Order}&amp;mode=json\"", html, StringComparison.Ordinal);
        Assert.Contains(">Form<", html, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Unsupported_schema_opens_json_mode_only()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host, "dash.array", TestSchemas.Of<WithArray>());

        var html = await OkAsync(host.GetAsync("/pgbolt/types/enqueue?name=dash.array"));

        Assert.Contains("<textarea name=\"json\"", html, StringComparison.Ordinal);
        Assert.DoesNotContain("name=\"args.", html, StringComparison.Ordinal);
        Assert.DoesNotContain("actions/types/json", html, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Enqueue_from_form_creates_job_and_links_card()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var created = await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm()));
        var unchecked_ = await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm().With("args.Urgent", null)));

        var job = (await Client(host).GetAsync(new JobId(JobLink(created)), Ct))!;
        Assert.Equal(Order, job.Name);
        Assert.Equal("orders", job.Queue);
        var expected = JsonNode.Parse($$"""
            {"Name":"n","Count":2,"Price":1.5,"Urgent":true,"Due":"2026-10-05T12:00:00Z","Ref":"{{TestSchemas.OrderRef}}","Tint":"Green",
             "Ship":{"City":"Москва","Zip":null},"Limit":null}
            """);
        Assert.True(JsonNode.DeepEquals(expected, JsonNode.Parse(job.Args.GetRawText())), job.Args.GetRawText());
        var other = (await Client(host).GetAsync(new JobId(JobLink(unchecked_)), Ct))!;
        Assert.False(other.Args.GetProperty("Urgent").GetBoolean());
    }

    [Fact]
    public async Task Parse_errors_do_not_hide_other_field_errors()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var html = await BadRequestAsync(host.PostAsync(EnqueuePath,
            TestSchemas.OrderForm().With("args.Name", "").With("args.Count", "abc").With("args.Tint", "Blue")));

        Assert.Single(Regex.Matches(Label(html, "args.Name"), "pgbolt-field-error"));
        Assert.Single(Regex.Matches(Label(html, "args.Count"), "pgbolt-field-error"));
        Assert.Contains("value is not in the list", Label(html, "args.Tint"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validation_errors_are_shown_at_fields()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var html = await BadRequestAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm().With("args.Name", "").With("args.Count", "abc")));

        Assert.Contains("pgbolt-field-error", Label(html, "args.Name"), StringComparison.Ordinal);
        Assert.Contains("pgbolt-field-error", Label(html, "args.Count"), StringComparison.Ordinal);
        Assert.DoesNotContain("pgbolt-field-error", Label(html, "args.Price"), StringComparison.Ordinal);
        Assert.Contains("value=\"abc\"", Tag(html, "name=\"args.Count\""), StringComparison.Ordinal);
        Assert.Equal(0, await JobCountAsync(db));
    }

    [Fact]
    public async Task Json_mode_validates_by_subset()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var wrong = await BadRequestAsync(host.PostAsync(EnqueuePath, [new("json", """{"Count":"x"}""")]));
        var malformed = await BadRequestAsync(host.PostAsync(EnqueuePath, [new("json", "{")]));

        Assert.Contains("<textarea name=\"json\"", wrong, StringComparison.Ordinal);
        Assert.Contains("Count: expected an integer", wrong, StringComparison.Ordinal);
        Assert.Contains("invalid JSON", malformed, StringComparison.Ordinal);
        Assert.Equal(0, await JobCountAsync(db));
    }

    [Fact]
    public async Task Json_prefill_comes_from_form_values()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var html = await OkAsync(host.PostAsync("/pgbolt/actions/types/json?name=" + Order,
            [new("args.Name", "n"), new("args.Count", "abc"), new("args.Price", "1.5"), new("args.Limit", "")]));

        Assert.StartsWith("<textarea name=\"json\"", html, StringComparison.Ordinal);
        var json = JsonNode.Parse(WebUtility.HtmlDecode(Regex.Match(html, "<textarea[^>]*>(.*)</textarea>", RegexOptions.Singleline).Groups[1].Value))!;
        Assert.Equal("n", (string?)json["Name"]);
        Assert.Equal("abc", (string?)json["Count"]);
        Assert.Equal(1.5m, (decimal?)json["Price"]);
        Assert.Null(json["Limit"]);
        Assert.False((bool?)json["Urgent"]);
    }

    [Fact]
    public async Task Unique_key_conflict_links_existing_job()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        var first = JobLink(await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm().With("uniqueKey", "k"))));

        var html = await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm().With("uniqueKey", "k")));

        Assert.Contains("an active job already exists", html, StringComparison.Ordinal);
        Assert.Equal(first, JobLink(html));
        Assert.Equal(1, await JobCountAsync(db));
    }

    [Fact]
    public async Task Finished_unique_key_race_has_no_link()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b =>
            DelegatingJobClient.Replace(b, inner => new EnqueueStubClient(inner, _ => new EnqueueResult(null, false))));
        await PublishAsync(host);

        var html = await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm().With("uniqueKey", "k")));

        Assert.Contains("a job with this key just finished", html, StringComparison.Ordinal);
        Assert.DoesNotContain("href=\"jobs/", html, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Vanished_type_reports_unknown_job()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);
        await OkAsync(host.GetAsync("/pgbolt/types/enqueue?name=" + Order));
        await db.ExecAsync("DELETE FROM {schema}.job_types WHERE name = @name", ("name", Order));

        var html = await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm()));

        Assert.Contains($"job type {Order} is not published", html, StringComparison.Ordinal);
        Assert.Equal(0, await JobCountAsync(db));
    }

    [Fact]
    public async Task Unknown_job_from_core_is_reported_as_message()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b =>
            DelegatingJobClient.Replace(b, inner => new EnqueueStubClient(inner, name => throw new UnknownJobException(name))));
        await PublishAsync(host);

        var html = await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm()));

        Assert.Contains($"job type {Order} is not published", html, StringComparison.Ordinal);
        Assert.DoesNotContain("href=\"jobs/", html, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Enqueue_form_and_json_button_have_own_target_and_swap()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var page = await OkAsync(host.GetAsync("/pgbolt/types/enqueue?name=" + Order));
        var created = await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm()));

        var form = Tag(page, "hx-post=\"actions/types/enqueue?name=" + Order + "\"");
        Assert.StartsWith("<form", form, StringComparison.Ordinal);
        Assert.Contains("hx-target=\"#enqueue-result\"", form, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"innerHTML\"", form, StringComparison.Ordinal);
        var button = Tag(page, "hx-post=\"actions/types/json?name=" + Order + "\"");
        Assert.Contains("hx-target=\"#enqueue-body\"", button, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"innerHTML\"", button, StringComparison.Ordinal);
        Assert.Contains("hx-include=\"closest form\"", button, StringComparison.Ordinal);
        var link = Tag(created, $"href=\"jobs/{JobLink(created)}\"");
        Assert.DoesNotContain("hx-target", link, StringComparison.Ordinal);
        Assert.DoesNotContain("hx-swap", link, StringComparison.Ordinal);
        Assert.DoesNotContain("hx-disinherit", page + created, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Enqueue_responses_contain_exactly_one_form()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var page = await OkAsync(host.GetAsync("/pgbolt/types/enqueue?name=" + Order));
        var invalid = await BadRequestAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm().With("args.Count", "abc")));
        var created = await OkAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm()));

        foreach (var html in new[] { page, invalid, created })
        {
            Assert.Equal(1, Regex.Count(html, "<form"));
            Assert.Equal(1, Regex.Count(html, "id=\"enqueue-body\""));
        }
        Assert.DoesNotContain("<title>", invalid + created, StringComparison.Ordinal);
        Assert.Equal(1, Regex.Count(page, "id=\"enqueue-result\""));
        Assert.DoesNotContain("id=\"enqueue-result\"", invalid + created, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Reserved_unique_key_prefix_is_field_error()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        var html = await BadRequestAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm().With("uniqueKey", "pgbolt:x")));

        Assert.Contains("pgbolt-field-error", Label(html, "uniqueKey"), StringComparison.Ordinal);
        Assert.Equal(0, await JobCountAsync(db));
    }

    [Fact]
    public async Task Delay_and_run_at_are_mutually_exclusive()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host);

        await BadRequestAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm().With("delay", "5").With("runAt", "2026-10-05T12:00:00Z")));

        Assert.Equal(0, await JobCountAsync(db));
    }

    [Fact]
    public async Task Enqueue_form_opens_for_type_name_with_special_characters()
    {
        const string name = "отчёт/ежедневный";
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await PublishAsync(host, name);
        var escaped = Uri.EscapeDataString(name);

        var page = await OkAsync(host.GetAsync("/pgbolt/types/enqueue?name=" + escaped));
        var created = await OkAsync(host.PostAsync("/pgbolt/actions/types/enqueue?name=" + escaped, TestSchemas.OrderForm()));

        Assert.Contains($"hx-post=\"actions/types/enqueue?name={escaped}\"", page, StringComparison.Ordinal);
        Assert.Equal(name, (await Client(host).GetAsync(new JobId(JobLink(created)), Ct))!.Name);
    }

    [Fact]
    public async Task Read_only_has_no_enqueue_form()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, dashboard: o => o.ReadOnly = true);
        await PublishAsync(host);

        var page = await OkAsync(host.GetAsync("/pgbolt/types/enqueue?name=" + Order));
        var (status, _) = await SendAsync(host.PostAsync(EnqueuePath, TestSchemas.OrderForm()));

        Assert.DoesNotContain("<form", page, StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.Forbidden, status);
        Assert.Equal(0, await JobCountAsync(db));
    }

    [Fact]
    public async Task Missing_type_is_not_found()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        var (status, _) = await SendAsync(host.GetAsync("/pgbolt/types/enqueue?name=missing"));

        Assert.Equal(HttpStatusCode.NotFound, status);
    }

    /// <summary>Клиент, у которого задача с ключом успела завершиться между запросами (core §5.1): EnqueueResult(null, false).</summary>
    /// <summary>EnqueueAsync по имени — заданный ответ вместо core.</summary>
    private sealed class EnqueueStubClient(IJobClient inner, Func<string, EnqueueResult> enqueue) : DelegatingJobClient(inner)
    {
        public override ValueTask<EnqueueResult> EnqueueAsync(string jobName, JsonElement args, EnqueueOptions? options = null,
            CancellationToken ct = default) => ValueTask.FromResult(enqueue(jobName));
    }
}
