using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

public sealed partial class RenderingTests(PostgresFixture pg)
{
    [Fact]
    public async Task Full_page_without_htmx_header()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        using var response = await host.GetAsync("/pgbolt/");
        var body = await response.Content.ReadAsStringAsync(Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", response.Content.Headers.ContentType?.ToString());
        Assert.StartsWith("<!DOCTYPE html>", body, StringComparison.Ordinal);
        Assert.Contains("<base href=\"/pgbolt/\"", body, StringComparison.Ordinal);
        Assert.Contains("<meta name=\"htmx-config\"", body, StringComparison.Ordinal);
        Assert.Contains("<main id=\"main\">", body, StringComparison.Ordinal);
        Assert.Contains("<h1>Overview</h1>", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Fragment_with_htmx_header()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        var body = await BodyAsync(host.GetAsync("/pgbolt/", htmx: true));

        Assert.DoesNotContain("<!DOCTYPE", body, StringComparison.Ordinal);
        Assert.StartsWith("<title>", body, StringComparison.Ordinal);
        Assert.Contains("<h1>Overview</h1>", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Partial_fragment_has_no_title()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, map: app =>
        {
            var g = app.MapPgBoltDashboard();
            DashboardRoutes.Get(g, "/part", (HttpContext http) => DashboardView.PageAsync<ErrorView>(http, "t",
                new Dictionary<string, object?> { ["Status"] = 200, ["Message"] = "x" }, partial: true));
            return g;
        });

        var fragment = await BodyAsync(host.GetAsync("/pgbolt/part", htmx: true));
        var page = await BodyAsync(host.GetAsync("/pgbolt/part"));

        Assert.DoesNotContain("<title>", fragment, StringComparison.Ordinal);
        Assert.DoesNotContain("<!DOCTYPE", fragment, StringComparison.Ordinal);
        Assert.Contains("<p>x</p>", fragment, StringComparison.Ordinal);
        Assert.StartsWith("<!DOCTYPE html>", page, StringComparison.Ordinal);
        Assert.Contains("<title>t · PgBolt</title>", page, StringComparison.Ordinal);
    }

    [Fact]
    public async Task History_restore_gets_full_page()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        var body = await BodyAsync(host.GetAsync("/pgbolt/", htmx: true, historyRestore: true));

        Assert.StartsWith("<!DOCTYPE html>", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Html_responses_vary_and_are_not_cached()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        using var response = await host.GetAsync("/pgbolt/");

        Assert.Contains("HX-Request", response.Headers.Vary);
        Assert.Contains("HX-History-Restore-Request", response.Headers.Vary);
        Assert.Equal("no-store", response.Headers.CacheControl?.ToString());
    }

    [Fact]
    public async Task Htmx_config_disables_eval_and_history_cache_and_swaps_errors()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        var body = await BodyAsync(host.GetAsync("/pgbolt/"));
        var content = HtmxConfigRegex().Match(body).Groups[1].Value;
        using var config = JsonDocument.Parse(WebUtility.HtmlDecode(content));
        var root = config.RootElement;

        Assert.False(root.GetProperty("allowEval").GetBoolean());
        Assert.Equal(0, root.GetProperty("historyCacheSize").GetInt32());
        Assert.False(root.GetProperty("includeIndicatorStyles").GetBoolean());
        Assert.True(root.GetProperty("disableInheritance").GetBoolean());
        Assert.Contains(root.GetProperty("responseHandling").EnumerateArray(), r =>
            r.GetProperty("code").GetString() == "[45].."
            && r.GetProperty("swap").GetBoolean()
            && r.TryGetProperty("error", out var error) && error.GetBoolean());
    }

    [Fact]
    public async Task Shell_body_inherits_only_navigation_attributes()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        var body = await BodyAsync(host.GetAsync("/pgbolt/"));
        var tag = BodyTagRegex().Match(body).Value;

        Assert.Contains("hx-boost=\"true\"", tag, StringComparison.Ordinal);
        Assert.Contains("hx-target=\"#main\"", tag, StringComparison.Ordinal);
        Assert.Contains("hx-swap=\"innerHTML show:window:top\"", tag, StringComparison.Ordinal);
        Assert.Contains("hx-inherit=\"hx-boost hx-target hx-swap\"", tag, StringComparison.Ordinal);
        Assert.DoesNotContain("hx-disinherit", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Shell_has_no_inline_scripts()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        var body = await BodyAsync(host.GetAsync("/pgbolt/"));
        var scripts = ScriptTagRegex().Matches(body).Select(m => m.Value).ToList();
        var htmx = Assert.Single(scripts, s => s.Contains("htmx.min.js", StringComparison.Ordinal));
        var own = Assert.Single(scripts, s => s.Contains("pgbolt.js", StringComparison.Ordinal));

        Assert.All(scripts, s => Assert.Contains("src=", s, StringComparison.Ordinal));
        Assert.DoesNotContain("hx-on", body, StringComparison.Ordinal);
        Assert.DoesNotContain("javascript:", body, StringComparison.Ordinal);
        Assert.Contains("defer", own, StringComparison.Ordinal);
        Assert.DoesNotContain("defer", htmx, StringComparison.Ordinal);
        Assert.True(body.IndexOf("<meta name=\"htmx-config\"", StringComparison.Ordinal) < body.IndexOf(htmx, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Assets_are_served_by_content_hash_and_cached_forever()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        var body = await BodyAsync(host.GetAsync("/pgbolt/"));
        var htmxUrl = ScriptTagRegex().Matches(body).Select(m => SrcRegex().Match(m.Value).Groups[1].Value)
            .Single(s => s.EndsWith("/htmx.min.js", StringComparison.Ordinal));
        var cssUrl = StylesheetRegex().Match(body).Groups[1].Value;
        Assert.Matches("^assets/[0-9a-f]{16}/htmx.min.js$", htmxUrl);

        using var js = await host.GetAsync("/pgbolt/" + htmxUrl);
        Assert.Equal(HttpStatusCode.OK, js.StatusCode);
        Assert.Equal("text/javascript", js.Content.Headers.ContentType?.MediaType);
        Assert.Equal("public, max-age=31536000, immutable", js.Headers.CacheControl?.ToString());
        Assert.Equal(ReadResource("htmx.min.js"), await js.Content.ReadAsByteArrayAsync(Ct));

        using var css = await host.GetAsync("/pgbolt/" + cssUrl);
        Assert.Equal(HttpStatusCode.OK, css.StatusCode);
        Assert.Equal("text/css", css.Content.Headers.ContentType?.MediaType);

        using var wrongHash = await host.GetAsync("/pgbolt/assets/0000000000000000/htmx.min.js");
        Assert.Equal(HttpStatusCode.NotFound, wrongHash.StatusCode);
    }

    [Fact]
    public async Task Base_path_includes_path_base_and_outer_group()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db,
            beforeMap: app => app.UsePathBase("/app"),
            map: app => app.MapGroup("/admin").MapPgBoltDashboard("/pgbolt"));

        var body = await BodyAsync(host.GetAsync("/app/admin/pgbolt/"));

        Assert.Contains("<base href=\"/app/admin/pgbolt/\"", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Map_without_AddPgBolt_throws_with_hint()
    {
        var withoutCore = WebApplication.CreateSlimBuilder();
        withoutCore.WebHost.UseTestServer();
        withoutCore.Services.AddPgBoltDashboard();
        await using var appWithoutCore = withoutCore.Build();

        var noCore = Assert.Throws<InvalidOperationException>(() => appWithoutCore.MapPgBoltDashboard());
        Assert.Contains("AddPgBolt(", noCore.Message, StringComparison.Ordinal);

        var withoutDashboard = WebApplication.CreateSlimBuilder();
        withoutDashboard.WebHost.UseTestServer();
        withoutDashboard.Services.AddPgBolt(o => o.ConnectionString = "Host=localhost");
        await using var appWithoutDashboard = withoutDashboard.Build();

        var noDashboard = Assert.Throws<InvalidOperationException>(() => appWithoutDashboard.MapPgBoltDashboard());
        Assert.Contains("AddPgBoltDashboard", noDashboard.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Errors_render_with_status_as_fragment_and_page()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        var logs = new ListLogger();
        await using var host = await DashboardHost.StartAsync(db,
            builder: b => b.Logging.AddProvider(logs),
            map: app =>
            {
                var g = app.MapPgBoltDashboard();
                DashboardRoutes.Get(g, "/boom/{kind}", string (string kind) =>
                {
                    switch (kind)
                    {
                        case "404": throw new DashboardNotFoundException("job");
                        case "400": throw new DashboardBadRequestException("bad cursor");
                        case "503": throw new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
                        default: throw new InvalidOperationException("secret detail");
                    }
                });
                return g;
            });

        foreach (var (kind, status) in new[] { ("404", 404), ("400", 400), ("503", 503), ("500", 500) })
        {
            using var page = await host.GetAsync("/pgbolt/boom/" + kind);
            using var fragment = await host.GetAsync("/pgbolt/boom/" + kind, htmx: true);
            Assert.Equal(status, (int)page.StatusCode);
            Assert.Equal(status, (int)fragment.StatusCode);
            Assert.StartsWith("<!DOCTYPE html>", await page.Content.ReadAsStringAsync(Ct), StringComparison.Ordinal);
            Assert.DoesNotContain("<!DOCTYPE", await fragment.Content.ReadAsStringAsync(Ct), StringComparison.Ordinal);
        }

        var timeout = await BodyAsync(host.GetAsync("/pgbolt/boom/503", htmx: true));
        Assert.Contains("Query exceeded QueryTimeout. Narrow the filter.", timeout, StringComparison.Ordinal);

        var internalError = await BodyAsync(host.GetAsync("/pgbolt/boom/500"));
        Assert.Contains("InvalidOperationException", internalError, StringComparison.Ordinal);
        Assert.DoesNotContain("   at ", internalError, StringComparison.Ordinal);

        Assert.Contains(logs.Entries, e => e.Level == LogLevel.Error && e.Exception is InvalidOperationException);
        Assert.Contains(logs.Entries, e => e.Level == LogLevel.Error && e.Exception is PostgresException);

        var toMain = await BodyAsync(host.GetAsync("/pgbolt/boom/404", htmx: true, htmxTarget: "main"));
        var toPanel = await BodyAsync(host.GetAsync("/pgbolt/boom/404", htmx: true));
        Assert.StartsWith("<title>", toMain, StringComparison.Ordinal);
        Assert.DoesNotContain("<title>", toPanel, StringComparison.Ordinal);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<string> BodyAsync(Task<HttpResponseMessage> request)
    {
        using var response = await request;
        return await response.Content.ReadAsStringAsync(Ct);
    }

    private static byte[] ReadResource(string file)
    {
        using var stream = typeof(PgBoltDashboardOptions).Assembly.GetManifestResourceStream("PgBolt.Dashboard.Assets." + file)!;
        using var copy = new MemoryStream();
        stream.CopyTo(copy);
        return copy.ToArray();
    }

    [GeneratedRegex("<meta name=\"htmx-config\" content=\"([^\"]*)\"")]
    private static partial Regex HtmxConfigRegex();

    [GeneratedRegex("<body[^>]*>")]
    private static partial Regex BodyTagRegex();

    [GeneratedRegex("<script[^>]*>")]
    private static partial Regex ScriptTagRegex();

    [GeneratedRegex("src=\"([^\"]*)\"")]
    private static partial Regex SrcRegex();

    [GeneratedRegex("<link rel=\"stylesheet\" href=\"([^\"]*)\"")]
    private static partial Regex StylesheetRegex();
}
