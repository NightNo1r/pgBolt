using System.Net;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Tests.Data;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

public sealed partial class OverviewPagesTests(PostgresFixture pg)
{
    private const string NotRunning = "The snapshot is stale. No process is running maintenance (<code>RunMaintenance</code>)";

    /// <summary>queue_stats (суммы 11 / 22 / 33 / 44), исходы текущей минуты и 30 минут назад, живой сервер, свежая строка stats.</summary>
    private static Task SeedAsync(TestDb db) => db.ExecAsync("""
        INSERT INTO {schema}.queue_stats VALUES ('a', 1, 2, 3, 4, now()), ('b c', 10, 20, 30, 40, now());
        INSERT INTO {schema}.stats_minutes VALUES
            (date_trunc('minute', now()), @live, 'a', 7, 2, 1),
            (date_trunc('minute', now()) - interval '30 minutes', @live, 'a', 5, 0, 0);
        INSERT INTO {schema}.servers VALUES (@live, 'alpha', 'h1', '{a}', 4, now() - interval '1 hour', now());
        INSERT INTO {schema}.maintenance_tasks (name, next_run_at, last_run_at) VALUES ('stats', now() + interval '8 seconds', now());
        """, ("live", OverviewStoreTests.Live));

    [Fact]
    public async Task Overview_page_of_empty_schema_shows_banner()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await db.ExecAsync("INSERT INTO {schema}.servers VALUES (@live, 'alpha', 'h1', '{a}', 4, now(), now())",
            ("live", OverviewStoreTests.Live));
        await using var host = await DashboardHost.StartAsync(db);

        var body = await BodyAsync(host.GetAsync("/pgbolt/"));

        Assert.Contains(NotRunning, body, StringComparison.Ordinal);
        Assert.Contains("No data", HourSection(body), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Overview_shows_totals_chart_and_polls_hour_panel()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await DashboardHost.StartAsync(db);

        var page = await BodyAsync(host.GetAsync("/pgbolt/"));

        foreach (var (tab, total) in new[] { ("scheduled", 11), ("enqueued", 22), ("processing", 33), ("awaiting", 44) })
            Assert.Matches($"<a href=\"jobs/{tab}\">[^<]+</a></dt>\\s*<dd>{total}</dd>", page);
        Assert.Matches("<a href=\"servers\">[^<]+</a></dt>\\s*<dd>1</dd>", page);
        var hour = HourSection(page);
        Assert.Contains("<svg", hour, StringComparison.Ordinal);
        Assert.Equal(60, GroupRegex().Count(hour));
        Assert.Contains("<dd>12</dd>", hour, StringComparison.Ordinal);
        var hourTag = Tag(page, "hx-get=\"overview/hour\"");
        Assert.Contains("hx-trigger=\"every 5000ms, pgbolt:refresh\"", hourTag, StringComparison.Ordinal);
        Assert.Contains("data-pgbolt-poll", hourTag, StringComparison.Ordinal);
        var dayTag = Tag(page, "hx-get=\"overview/day\"");
        Assert.Contains("hx-trigger=\"load, every 60000ms, pgbolt:refresh\"", dayTag, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"innerHTML\"", dayTag, StringComparison.Ordinal);
        Assert.Contains("data-pgbolt-poll", dayTag, StringComparison.Ordinal);
        Assert.Contains("Statistics are approximate", page, StringComparison.Ordinal);
        Assert.DoesNotContain(NotRunning, page, StringComparison.Ordinal);

        await db.ExecAsync("UPDATE {schema}.maintenance_tasks SET next_run_at = now() - interval '1 hour', last_run_at = now() - interval '1 hour'");
        var fragment = await BodyAsync(host.GetAsync("/pgbolt/overview/hour", htmx: true));

        Assert.Matches("<a href=\"jobs/awaiting\">[^<]+</a></dt>\\s*<dd>44</dd>", fragment);
        Assert.Matches("<a href=\"servers\">[^<]+</a></dt>\\s*<dd>1</dd>", fragment);
        Assert.Contains(NotRunning, fragment, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Stats_error_banner_shows_last_error_text()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await db.ExecAsync("""
            INSERT INTO {schema}.maintenance_tasks (name, next_run_at, last_run_at, last_error)
            VALUES ('stats', now() + interval '8 seconds', now(), '<b>boom</b>')
            """);
        await using var host = await DashboardHost.StartAsync(db);

        var page = await BodyAsync(host.GetAsync("/pgbolt/"));

        Assert.Contains("&lt;b&gt;boom&lt;/b&gt;", page, StringComparison.Ordinal);
        Assert.DoesNotContain("<b>boom", page, StringComparison.Ordinal);
        Assert.DoesNotContain(NotRunning, page, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Hour_and_day_fragments_render_alone()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await DashboardHost.StartAsync(db);

        var hour = await BodyAsync(host.GetAsync("/pgbolt/overview/hour", htmx: true));
        var day = await BodyAsync(host.GetAsync("/pgbolt/overview/day", htmx: true));
        var rows = await BodyAsync(host.GetAsync("/pgbolt/queues/rows", htmx: true));

        // <title> внутри <g> графика — подсказка SVG; htmx берёт заголовок только из <title> верхнего уровня.
        foreach (var fragment in new[] { hour, day, rows })
        {
            Assert.DoesNotContain("<!DOCTYPE", fragment, StringComparison.Ordinal);
            Assert.DoesNotContain("PgBolt</title>", fragment, StringComparison.Ordinal);
            Assert.False(fragment.TrimStart().StartsWith("<title>", StringComparison.Ordinal));
        }
        Assert.Contains("<svg", hour, StringComparison.Ordinal);
        Assert.Contains("<svg", day, StringComparison.Ordinal);
        Assert.Equal(96, GroupRegex().Count(day));
        Assert.DoesNotContain("<title>", rows, StringComparison.Ordinal);
        Assert.Contains("jobs/enqueued?queue=a\"", rows, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Polling_and_load_elements_have_explicit_swap()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await using var host = await DashboardHost.StartAsync(db);

        var overview = await BodyAsync(host.GetAsync("/pgbolt/"));
        var queues = await BodyAsync(host.GetAsync("/pgbolt/queues"));

        var triggered = TriggerTagRegex().Matches(overview).Concat(TriggerTagRegex().Matches(queues)).Select(m => m.Value).ToList();
        Assert.Equal(3, triggered.Count);
        Assert.All(triggered, tag =>
        {
            Assert.Contains("hx-swap=\"innerHTML\"", tag, StringComparison.Ordinal);
            Assert.Contains("hx-target=\"this\"", tag, StringComparison.Ordinal);
            Assert.DoesNotContain("show:", tag, StringComparison.Ordinal);
        });
        Assert.DoesNotContain("hx-disinherit", overview, StringComparison.Ordinal);
        Assert.DoesNotContain("hx-disinherit", queues, StringComparison.Ordinal);

        var tbody = TbodyRegex().Match(queues).Value;
        Assert.Contains("hx-get=\"queues/rows\"", tbody, StringComparison.Ordinal);
        var links = AnchorRegex().Matches(tbody).Select(m => m.Value).ToList();
        Assert.Contains("<a href=\"jobs/enqueued?queue=b%20c\">", links);
        Assert.All(links, link =>
        {
            Assert.DoesNotContain("hx-target", link, StringComparison.Ordinal);
            Assert.DoesNotContain("hx-swap", link, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Queues_page_lists_served_queue_with_zeros_and_polls_rows()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await db.ExecAsync("""
            INSERT INTO {schema}.servers VALUES (@live, 'alpha', 'h1', '{a}', 4, now(), now());
            INSERT INTO {schema}.maintenance_tasks (name, next_run_at, last_run_at) VALUES ('stats', now() + interval '8 seconds', now());
            """, ("live", OverviewStoreTests.Live));
        await using var host = await DashboardHost.StartAsync(db);

        var page = await BodyAsync(host.GetAsync("/pgbolt/queues"));

        Assert.Matches("<td><a href=\"jobs/enqueued\\?queue=a\">a</a></td>\\s*<td>0</td>\\s*<td>0</td>\\s*<td>0</td>\\s*<td>0</td>\\s*<td>1</td>", page);
        var tbody = Tag(page, "hx-get=\"queues/rows\"");
        Assert.StartsWith("<tbody", tbody, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"innerHTML\"", tbody, StringComparison.Ordinal);
        Assert.Contains("data-pgbolt-poll", tbody, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queues_page_and_rows_show_limit_pause_and_weight()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await SeedAsync(db);
        await db.ExecAsync("""
            INSERT INTO {schema}.queue_stats VALUES ('plain', 0, 0, 0, 0, now());
            INSERT INTO {schema}.queue_settings (queue, default_max_concurrency, default_weight) VALUES ('a', 3, 2);
            INSERT INTO {schema}.queue_settings (queue, default_max_concurrency, override_concurrency_set, override_max_concurrency,
                                                 override_concurrency_updated_by, override_concurrency_updated_at,
                                                 override_weight_set, override_weight, override_weight_updated_by, override_weight_updated_at)
            VALUES ('b c', 5, true, 0, 'ops', '2026-01-02T03:04:05Z', true, 7, 'ops2', '2026-01-02T03:04:06Z');
            """);
        await using var host = await DashboardHost.StartAsync(db);
        var seeder = new JobSeeder(db, host.Services.GetRequiredService<PgBoltStore>());
        await seeder.SeedJobsAsync(2, "dash.job", "a", 0, "enqueued");
        await seeder.LeaseAsync("a");

        var page = await BodyAsync(host.GetAsync("/pgbolt/queues"));
        var fragment = await BodyAsync(host.GetAsync("/pgbolt/queues/rows", htmx: true));

        var header = Regex.Match(page, "<thead>.*?</thead>", RegexOptions.Singleline).Value;
        Assert.Contains("<th>Limit</th>", header, StringComparison.Ordinal);
        Assert.Contains("<th>Weight</th>", header, StringComparison.Ordinal);
        var rows = RowRegex().Matches(TbodyRegex().Match(page).Value).Select(m => m.Value).ToList();
        Assert.Equal(rows, RowRegex().Matches(fragment).Select(m => m.Value));
        Assert.All(rows, r => Assert.Equal(Regex.Count(header, "<th[ >]"), Regex.Count(r, "<td[ >]")));

        var a = Cells(rows.Single(r => r.Contains("queue=a\"", StringComparison.Ordinal)));
        Assert.Matches("^1 / 3\\s*<span class=\"muted\">configured default</span>$", a[7]);
        Assert.Matches("^2\\s*<span class=\"muted\">configured default</span>$", a[8]);
        var paused = Cells(rows.Single(r => r.Contains("queue=b%20c\"", StringComparison.Ordinal)));
        Assert.Matches("^0 / 0\\s*<span class=\"pgbolt-state\">Paused</span>\\s*<span class=\"muted\">overridden by ops, <time datetime=\"2026-01-02T03:04:05.000Z\">", paused[7]);
        Assert.Matches("^7\\s*<span class=\"muted\">overridden by ops2, <time datetime=\"2026-01-02T03:04:06.000Z\">", paused[8]);
        var plain = Cells(rows.Single(r => r.Contains("queue=plain\"", StringComparison.Ordinal)));
        Assert.Equal(("—", "—"), (plain[7], plain[8]));

        Assert.Contains("Reset settings cache", page, StringComparison.Ordinal);
        Assert.Contains("Other pods pick up changes according to QueueSettingsRefreshInterval and their cache TTL", page, StringComparison.Ordinal);
        await using var readOnly = await DashboardHost.StartAsync(db, o => o.ReadOnly = true);
        var hidden = await BodyAsync(readOnly.GetAsync("/pgbolt/queues"));
        Assert.DoesNotContain("Reset settings cache", hidden, StringComparison.Ordinal);
        Assert.Contains("0 / 0", hidden, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Servers_page_marks_stale_and_shows_maintenance_tasks()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await db.ExecAsync("""
            INSERT INTO {schema}.servers VALUES
                (@live, 'alpha', 'h1', '{a,b}', 4, now() - interval '1 hour', now()),
                (@dead, 'beta', 'h2', '{c}', 2, now() - interval '1 hour', now() - interval '2 minutes');
            """, ("live", OverviewStoreTests.Live), ("dead", OverviewStoreTests.Dead));
        await OverviewStoreTests.SeedMaintenanceAsync(db);
        await using var host = await DashboardHost.StartAsync(db);

        var page = await BodyAsync(host.GetAsync("/pgbolt/servers"));
        var rows = RowRegex().Matches(page).Select(m => m.Value).ToList();

        Assert.Contains("unresponsive", rows.Single(r => r.Contains("beta", StringComparison.Ordinal)), StringComparison.Ordinal);
        Assert.DoesNotContain("unresponsive", rows.Single(r => r.Contains("alpha", StringComparison.Ordinal)), StringComparison.Ordinal);
        var retention = rows.Single(r => r.Contains("retention", StringComparison.Ordinal));
        var partitions = rows.Single(r => r.Contains("partitions", StringComparison.Ordinal));
        Assert.Contains("running", retention, StringComparison.Ordinal);
        Assert.DoesNotContain("running", partitions, StringComparison.Ordinal);
        Assert.Contains("<pre>disk full</pre>", partitions, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Job_types_page_lists_types_with_enqueue_links()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);
        await host.Services.GetRequiredService<PgBoltStore>().PublishJobTypesAsync(
            [new JobTypeRecord("ns.job/x y", """{"type":"object"}""", "q2", 3, 0, null)], "tests", Ct);

        var page = await BodyAsync(host.GetAsync("/pgbolt/types"));

        Assert.Contains("ns.job/x y", page, StringComparison.Ordinal);
        Assert.Contains("q2", page, StringComparison.Ordinal);
        Assert.Contains("href=\"types/enqueue?name=ns.job%2Fx%20y\"", page, StringComparison.Ordinal);

        await using var readOnly = await DashboardHost.StartAsync(db, o => o.ReadOnly = true);
        var hidden = await BodyAsync(readOnly.GetAsync("/pgbolt/types"));
        Assert.Contains("ns.job/x y", hidden, StringComparison.Ordinal);
        Assert.DoesNotContain("types/enqueue", hidden, StringComparison.Ordinal);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<string> BodyAsync(Task<HttpResponseMessage> request)
    {
        using var response = await request;
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return await response.Content.ReadAsStringAsync(Ct);
    }

    /// <summary>Открывающий тег элемента с атрибутом attribute.</summary>
    private static string Tag(string html, string attribute) =>
        Regex.Match(html, "<[a-z]+[^>]*" + Regex.Escape(attribute) + "[^>]*>").Value;

    /// <summary>Содержимое ячеек строки без крайних пробелов.</summary>
    private static List<string> Cells(string row) =>
        [.. Regex.Matches(row, "<td>(.*?)</td>", RegexOptions.Singleline).Select(m => m.Groups[1].Value.Trim())];

    /// <summary>Содержимое секции панели часа.</summary>
    private static string HourSection(string html) =>
        Regex.Match(html, "<section[^>]*hx-get=\"overview/hour\"[^>]*>(.*?)</section>", RegexOptions.Singleline).Groups[1].Value;

    [GeneratedRegex("<g>")]
    private static partial Regex GroupRegex();

    [GeneratedRegex("<[a-z]+[^>]*hx-trigger=[^>]*>")]
    private static partial Regex TriggerTagRegex();

    [GeneratedRegex("<tbody.*?</tbody>", RegexOptions.Singleline)]
    private static partial Regex TbodyRegex();

    [GeneratedRegex("<a [^>]*>")]
    private static partial Regex AnchorRegex();

    [GeneratedRegex("<tr>.*?</tr>", RegexOptions.Singleline)]
    private static partial Regex RowRegex();
}
