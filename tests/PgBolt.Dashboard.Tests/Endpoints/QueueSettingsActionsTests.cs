using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

public sealed class QueueSettingsActionsTests(PostgresFixture pg)
{
    private const string PauseBadge = "<span class=\"pgbolt-state\">Пауза</span>";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static IQueueSettingsManager Manager(DashboardHost host) => host.Services.GetRequiredService<IQueueSettingsManager>();

    /// <summary>Очередь «a» есть в снимке (не выпадает из списка), значения из кода — лимит 3, вес 2.</summary>
    private static Task SeedAsync(TestDb db) => db.ExecAsync("""
        INSERT INTO {schema}.queue_stats VALUES ('a', 0, 0, 0, 0, now());
        INSERT INTO {schema}.queue_settings (queue, default_max_concurrency, default_weight) VALUES ('a', 3, 2);
        """);

    /// <summary>Хост с пользователем alice (схема Test) и, если задан, фейковым refresher'ом.</summary>
    private static async Task<DashboardHost> StartAsync(TestDb db, Action<PgBoltDashboardOptions>? dashboard = null,
        IQueueSettingsRefresher? refresher = null, ListLogger? logs = null, Action<PgBoltOptions>? core = null)
    {
        var host = await DashboardHost.StartAsync(db, dashboard, builder: b =>
        {
            TestAuthentication.Add(b.Services);
            if (refresher is not null)
                b.Services.AddSingleton(refresher);
            if (logs is not null)
                b.Logging.AddProvider(logs);
        }, core: core);
        host.Client.DefaultRequestHeaders.Add(TestAuthentication.UserHeader, "alice");
        return host;
    }

    private static Task<HttpResponseMessage> PostAsync(DashboardHost host, string path, string? op = null, string? value = null)
    {
        var form = new List<KeyValuePair<string, string>>();
        if (op is not null)
            form.Add(new("op", op));
        if (value is not null)
            form.Add(new("value", value));
        return host.PostAsync("/pgbolt/" + path, form);
    }

    [Fact]
    public async Task Limit_operations_write_overrides_and_return_row()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await StartAsync(db);
        var header = Regex.Match(await BodyAsync(host.GetAsync("/pgbolt/queues")), "<thead>.*?</thead>", RegexOptions.Singleline).Value;

        var (set, setMessage) = await ActAsync(PostAsync(host, "actions/queues/limit?queue=a", "set", "7"));

        Assert.StartsWith("<tr", set, StringComparison.Ordinal);
        Assert.Equal(Regex.Count(header, "<th[ >]"), Regex.Count(set, "<td[ >]"));
        Assert.Contains("0 / 7", set, StringComparison.Ordinal);
        Assert.Contains("переопределён: dashboard: alice", set, StringComparison.Ordinal);
        Assert.Equal("лимит 7", setMessage);
        var settings = (await Manager(host).GetAsync("a", Ct))!;
        Assert.Equal((true, 7, "dashboard: alice"), (settings.OverrideConcurrencySet, settings.OverrideMaxConcurrency,
            settings.OverrideConcurrencyUpdatedBy));

        var (paused, pauseMessage) = await ActAsync(PostAsync(host, "actions/queues/limit?queue=a", "pause"));

        Assert.Contains(PauseBadge, paused, StringComparison.Ordinal);
        Assert.Equal("пауза", pauseMessage);
        settings = (await Manager(host).GetAsync("a", Ct))!;
        Assert.Equal((true, "dashboard: alice"), (settings.Paused, settings.OverrideConcurrencyUpdatedBy));

        var (unlimited, unlimitedMessage) = await ActAsync(PostAsync(host, "actions/queues/limit?queue=a", "unlimited"));

        Assert.DoesNotContain(PauseBadge, unlimited, StringComparison.Ordinal);
        Assert.Equal("без лимита", unlimitedMessage);
        settings = (await Manager(host).GetAsync("a", Ct))!;
        Assert.Equal((true, null, 2, "dashboard: alice"), (settings.OverrideConcurrencySet, settings.EffectiveMaxConcurrency,
            settings.EffectiveWeight, settings.OverrideConcurrencyUpdatedBy));

        var (reset, resetMessage) = await ActAsync(PostAsync(host, "actions/queues/limit?queue=a", "reset"));

        Assert.Contains("0 / 3", reset, StringComparison.Ordinal);
        Assert.DoesNotContain("переопределён", reset, StringComparison.Ordinal);
        Assert.Equal("лимит из кода", resetMessage);
        Assert.False((await Manager(host).GetAsync("a", Ct))!.OverrideConcurrencySet);
    }

    [Fact]
    public async Task Weight_operations_write_overrides_and_return_row()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await StartAsync(db);

        var (set, setMessage) = await ActAsync(PostAsync(host, "actions/queues/weight?queue=a", "set", "1000"));

        Assert.StartsWith("<tr", set, StringComparison.Ordinal);
        Assert.Contains("<td>1000", set, StringComparison.Ordinal);
        Assert.Contains("переопределён: dashboard: alice", set, StringComparison.Ordinal);
        Assert.Equal("вес 1000", setMessage);
        var settings = (await Manager(host).GetAsync("a", Ct))!;
        Assert.Equal((true, 1000, "dashboard: alice", false), (settings.OverrideWeightSet, settings.OverrideWeight,
            settings.OverrideWeightUpdatedBy, settings.OverrideConcurrencySet));

        var (_, noneMessage) = await ActAsync(PostAsync(host, "actions/queues/weight?queue=a", "none"));

        Assert.Equal("без веса", noneMessage);
        settings = (await Manager(host).GetAsync("a", Ct))!;
        Assert.Equal((true, null, "dashboard: alice"), (settings.OverrideWeightSet, settings.EffectiveWeight,
            settings.OverrideWeightUpdatedBy));

        var (reset, resetMessage) = await ActAsync(PostAsync(host, "actions/queues/weight?queue=a", "reset"));

        Assert.Contains("<td>2", reset, StringComparison.Ordinal);
        Assert.Equal("вес из кода", resetMessage);
        Assert.False((await Manager(host).GetAsync("a", Ct))!.OverrideWeightSet);
    }

    [Fact]
    public async Task Override_only_queue_is_listed_and_resettable()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await StartAsync(db);
        await Manager(host).SetConcurrencyOverrideAsync("limited", 4, "ops", Ct);

        Assert.Contains("queue=limited\"", await BodyAsync(host.GetAsync("/pgbolt/queues")), StringComparison.Ordinal);

        var (row, message) = await ActAsync(PostAsync(host, "actions/queues/limit?queue=limited", "reset"));

        Assert.Equal("", row);
        Assert.Equal("лимит из кода", message);
        Assert.DoesNotContain("queue=limited\"", await BodyAsync(host.GetAsync("/pgbolt/queues")), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("limit", "set", "abc")]
    [InlineData("limit", "set", "0")]
    [InlineData("limit", "set", null)]
    [InlineData("weight", "set", "0")]
    [InlineData("weight", "set", "1001")]
    [InlineData("weight", "set", "")]
    public async Task Invalid_value_is_400(string setting, string op, string? value)
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await StartAsync(db);

        using var response = await PostAsync(host, $"actions/queues/{setting}?queue=a", op, value);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var settings = (await Manager(host).GetAsync("a", Ct))!;
        Assert.False(settings.OverrideConcurrencySet || settings.OverrideWeightSet);
    }

    [Fact]
    public async Task Unknown_op_is_404_and_missing_queue_is_400()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await StartAsync(db);

        using var unknownLimit = await PostAsync(host, "actions/queues/limit?queue=a", "none");
        using var unknownWeight = await PostAsync(host, "actions/queues/weight?queue=a", "pause");
        using var noOp = await PostAsync(host, "actions/queues/weight?queue=a", "");
        using var noQueue = await PostAsync(host, "actions/queues/limit", "pause");
        using var blankQueue = await PostAsync(host, "actions/queues/limit?queue=%20", "pause");

        Assert.Equal(HttpStatusCode.NotFound, unknownLimit.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, unknownWeight.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, noOp.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, noQueue.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, blankQueue.StatusCode);
    }

    [Fact]
    public async Task Read_only_hides_buttons_and_rejects_posts()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await StartAsync(db, o => o.ReadOnly = true);

        var page = await BodyAsync(host.GetAsync("/pgbolt/queues"));
        var rows = await BodyAsync(host.GetAsync("/pgbolt/queues/rows", htmx: true));

        Assert.Contains("queue=a\"", page, StringComparison.Ordinal);
        foreach (var html in new[] { page, rows })
        {
            Assert.DoesNotContain("hx-post", html, StringComparison.Ordinal);
            Assert.DoesNotContain("<button", html, StringComparison.Ordinal);
        }
        foreach (var (path, op) in new[] { ("actions/queues/limit?queue=a", "pause"), ("actions/queues/weight?queue=a", "none"),
                     ("actions/queues/cache/reset", null) })
        {
            using var response = await PostAsync(host, path, op);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        }
        var settings = (await Manager(host).GetAsync("a", Ct))!;
        Assert.False(settings.OverrideConcurrencySet || settings.OverrideWeightSet);
    }

    [Fact]
    public async Task Buttons_post_to_row_actions()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await db.ExecAsync("INSERT INTO {schema}.queue_stats VALUES ('b c', 0, 0, 0, 0, now())");
        await using var host = await StartAsync(db);

        var page = await BodyAsync(host.GetAsync("/pgbolt/queues"));

        foreach (var (setting, op) in new[] { ("limit", "unlimited"), ("limit", "pause"), ("limit", "reset"),
                     ("weight", "none"), ("weight", "reset") })
        {
            var button = Regex.Match(page, $"<button[^>]*hx-post=\"actions/queues/{setting}\\?queue=b%20c\"[^>]*value=\"{op}\"[^>]*>").Value;
            Assert.Contains("type=\"button\"", button, StringComparison.Ordinal);
            Assert.Contains("name=\"op\"", button, StringComparison.Ordinal);
            Assert.Contains("hx-target=\"closest tr\"", button, StringComparison.Ordinal);
            Assert.Contains("hx-swap=\"outerHTML\"", button, StringComparison.Ordinal);
        }
        Assert.Matches("<button[^>]*hx-post=\"actions/queues/cache/reset\"", page);
    }

    /// <summary>
    /// Enter в поле и «Задать» отправляют форму: op=set приходит от submit-кнопки (htmx добавляет значение нажатой кнопки,
    /// при неявной отправке браузер «нажимает» первую submit-кнопку). У формы нет hx-vals и скрытого op — иначе
    /// остальные кнопки отправляли бы op=set вместо своего.
    /// </summary>
    [Fact]
    public async Task Enter_in_value_field_submits_set()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await db.ExecAsync("INSERT INTO {schema}.queue_stats VALUES ('b c', 0, 0, 0, 0, now())");
        await using var host = await StartAsync(db);

        var page = await BodyAsync(host.GetAsync("/pgbolt/queues"));

        foreach (var setting in new[] { "limit", "weight" })
        {
            var form = Regex.Match(page, $"<form[^>]*hx-post=\"actions/queues/{setting}\\?queue=b%20c\"[^>]*>.*?</form>",
                RegexOptions.Singleline).Value;
            var tag = form[..(form.IndexOf('>') + 1)];
            Assert.Contains("hx-target=\"closest tr\"", tag, StringComparison.Ordinal);
            Assert.Contains("hx-swap=\"outerHTML\"", tag, StringComparison.Ordinal);
            Assert.DoesNotContain("hx-vals", form, StringComparison.Ordinal);
            Assert.DoesNotContain("type=\"hidden\"", form, StringComparison.Ordinal);
            var submits = Regex.Matches(form, "<button[^>]*type=\"submit\"[^>]*>").Select(m => m.Value).ToList();
            var set = Assert.Single(submits);
            Assert.Contains("name=\"op\"", set, StringComparison.Ordinal);
            Assert.Contains("value=\"set\"", set, StringComparison.Ordinal);
            Assert.DoesNotContain("hx-post", set, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Cache_reset_invalidates_cache_and_refreshes_servers()
    {
        var refresher = new CountingRefresher();
        var logs = new ListLogger();
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await StartAsync(db, refresher: refresher, logs: logs, core: o => o.QueueSettingsCacheDuration = TimeSpan.FromHours(1));
        Assert.Equal(3, (await Manager(host).GetAsync("a", Ct))!.EffectiveMaxConcurrency);
        await db.ExecAsync("UPDATE {schema}.queue_settings SET default_max_concurrency = 5");
        Assert.Equal(3, (await Manager(host).GetAsync("a", Ct))!.EffectiveMaxConcurrency);

        var (_, message) = await ActAsync(PostAsync(host, "actions/queues/cache/reset"));

        Assert.Equal("кэш настроек сброшен", message);
        Assert.Equal(1, refresher.Calls);
        Assert.Equal(5, (await Manager(host).GetAsync("a", Ct))!.EffectiveMaxConcurrency);
        Assert.Contains(logs.Entries, e => e.Level == LogLevel.Information
            && e.Message == "PgBolt dashboard: alice reset queue settings cache: кэш настроек сброшен.");

        await using var plain = await StartAsync(db);
        await ActAsync(PostAsync(plain, "actions/queues/cache/reset"));
    }

    [Fact]
    public async Task Each_action_is_logged()
    {
        var logs = new ListLogger();
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await StartAsync(db, logs: logs);

        await ActAsync(PostAsync(host, "actions/queues/limit?queue=a", "set", "7"));
        await ActAsync(PostAsync(host, "actions/queues/weight?queue=a", "reset"));

        Assert.Equal(["PgBolt dashboard: alice limit set queue a: лимит 7.", "PgBolt dashboard: alice weight reset queue a: вес из кода."],
            logs.Entries.Where(e => e.Category == "PgBolt.Dashboard.Actions" && e.Level == LogLevel.Information).Select(e => e.Message));
    }

    private sealed class CountingRefresher : IQueueSettingsRefresher
    {
        private int _calls;

        public int Calls => _calls;

        public void RefreshNow() => Interlocked.Increment(ref _calls);
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
}
