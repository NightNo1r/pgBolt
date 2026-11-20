using System.Net;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

public sealed class AccessEndpointTests(PostgresFixture pg)
{
    private static readonly IPAddress Remote = IPAddress.Parse("10.0.0.5");

    [Fact]
    public async Task Local_request_is_allowed_without_authorization()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, remote: IPAddress.Loopback);

        using var response = await host.GetAsync("/pgbolt/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Remote_request_without_authorization_is_forbidden()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using (var remote = await DashboardHost.StartAsync(db, remote: Remote))
            Assert.Equal(HttpStatusCode.Forbidden, await StatusAsync(remote));
        await using (var unknown = await DashboardHost.StartAsync(db, nullRemote: true))
            Assert.Equal(HttpStatusCode.Forbidden, await StatusAsync(unknown));
        await using (var proxied = await DashboardHost.StartAsync(db, remote: IPAddress.Loopback))
        {
            proxied.Client.DefaultRequestHeaders.Add("X-Forwarded-For", "1.2.3.4");
            Assert.Equal(HttpStatusCode.Forbidden, await StatusAsync(proxied));
        }
    }

    [Fact]
    public async Task AllowAnonymousRemote_allows_remote()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, o => o.AllowAnonymousRemote = true, remote: Remote);

        Assert.Equal(HttpStatusCode.OK, await StatusAsync(host));
    }

    [Fact]
    public async Task RequireAuthorization_on_group_allows_remote_authenticated_user()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b => TestAuthentication.Add(b.Services),
            map: app => app.MapPgBoltDashboard().RequireAuthorization(), remote: Remote);

        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(host));
        host.Client.DefaultRequestHeaders.Add(TestAuthentication.UserHeader, "alice");
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(host));
    }

    [Fact]
    public async Task Fallback_policy_counts_as_authorization()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db,
            builder: b => TestAuthentication.Add(b.Services, o => o.FallbackPolicy = new AuthorizationPolicyBuilder().RequireAuthenticatedUser().Build()),
            remote: Remote);
        host.Client.DefaultRequestHeaders.Add(TestAuthentication.UserHeader, "alice");

        Assert.Equal(HttpStatusCode.OK, await StatusAsync(host));
    }

    [Fact]
    public async Task AllowAnonymous_on_group_is_not_authorization()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b => TestAuthentication.Add(b.Services),
            map: app => app.MapPgBoltDashboard().RequireAuthorization().AllowAnonymous(), remote: Remote);

        Assert.Equal(HttpStatusCode.Forbidden, await StatusAsync(host));
    }

    [Fact]
    public async Task ActionsPolicy_without_authorization_services_fails_map()
    {
        var b = WebApplication.CreateSlimBuilder();
        b.WebHost.UseTestServer();
        b.Services.AddPgBolt(o => o.ConnectionString = "Host=localhost");
        b.Services.AddPgBoltDashboard(o => o.ActionsPolicy = "Ops");
        await using var app = b.Build();

        var e = Assert.Throws<InvalidOperationException>(() => app.MapPgBoltDashboard());
        Assert.Contains("AddAuthorization", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CanAct_reflects_read_only_and_policy()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using (var readOnly = await DashboardHost.StartAsync(db, o => o.ReadOnly = true, map: MapCanAct))
            Assert.Equal("False", await BodyAsync(readOnly.GetAsync("/pgbolt/can-act")));

        await using var host = await DashboardHost.StartAsync(db, o => o.ActionsPolicy = "Ops",
            builder: b => TestAuthentication.Add(b.Services, o => o.AddPolicy("Ops", p => p.RequireRole("Ops"))), map: MapCanAct);
        host.Client.DefaultRequestHeaders.Add(TestAuthentication.UserHeader, "alice");
        Assert.Equal("False", await BodyAsync(host.GetAsync("/pgbolt/can-act")));
        host.Client.DefaultRequestHeaders.Add(TestAuthentication.RolesHeader, "Ops");
        Assert.Equal("True", await BodyAsync(host.GetAsync("/pgbolt/can-act")));
    }

    [Fact]
    public async Task Denied_response_is_rendered_with_context()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, remote: Remote);

        using var response = await host.GetAsync("/pgbolt/");
        var body = await response.Content.ReadAsStringAsync(Ct);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.StartsWith("<!DOCTYPE html>", body, StringComparison.Ordinal);
        Assert.Contains("<base href=\"/pgbolt/\"", body, StringComparison.Ordinal);
        Assert.Contains("доступ только с локальных запросов", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Startup_warns_when_dashboard_is_local_only()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);

        var local = await StartupWarningsAsync(db);
        var authorized = await StartupWarningsAsync(db, builder: b => TestAuthentication.Add(b.Services),
            map: app => app.MapPgBoltDashboard().RequireAuthorization());
        var anonymous = await StartupWarningsAsync(db, o => o.AllowAnonymousRemote = true);

        Assert.Contains("RequireAuthorization", Assert.Single(local), StringComparison.Ordinal);
        Assert.Empty(authorized);
        Assert.Contains("anonymous remote access", Assert.Single(anonymous), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Startup_warns_only_for_unauthorized_group()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);

        var warnings = await StartupWarningsAsync(db, builder: b => TestAuthentication.Add(b.Services), map: app =>
        {
            app.MapPgBoltDashboard("/secure").RequireAuthorization();
            return app.MapPgBoltDashboard("/open");
        });

        Assert.Contains("RequireAuthorization", Assert.Single(warnings), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Startup_reports_unregistered_actions_policy()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        var logs = new ListLogger();

        await using (await DashboardHost.StartAsync(db, o => o.ActionsPolicy = "Missing", b =>
        {
            b.Logging.AddProvider(logs);
            TestAuthentication.Add(b.Services);
        }))
        {
        }
        await using (await DashboardHost.StartAsync(db, o => o.ActionsPolicy = "Ops", b =>
        {
            b.Logging.AddProvider(logs);
            TestAuthentication.Add(b.Services, o => o.AddPolicy("Ops", p => p.RequireRole("Ops")));
        }))
        {
        }

        var error = Assert.Single(logs.Entries, e => e.Level == LogLevel.Error);
        Assert.Contains("'Missing'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Unregistered_actions_policy_is_reported_once_for_several_groups()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        var logs = new ListLogger();

        await using (await DashboardHost.StartAsync(db, o => o.ActionsPolicy = "Missing", b =>
        {
            b.Logging.AddProvider(logs);
            TestAuthentication.Add(b.Services);
        }, map: app =>
        {
            app.MapPgBoltDashboard("/a");
            return app.MapPgBoltDashboard("/b");
        }))
        {
        }

        Assert.Single(logs.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public async Task Startup_check_tolerates_group_outside_main_endpoint_source()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        var logs = new ListLogger();

        await using (await DashboardHost.StartAsync(db, o => o.ActionsPolicy = "Missing", b =>
        {
            b.Logging.AddProvider(logs);
            TestAuthentication.Add(b.Services);
        }, map: app =>
        {
            new DetachedRoutes(app.Services).MapPgBoltDashboard();
            return app.MapGroup("/none");
        }))
        {
        }

        Assert.DoesNotContain(logs.Entries, e => e.Level >= LogLevel.Error);
    }

    /// <summary>Свой IEndpointRouteBuilder: его источники не попадают в EndpointDataSource хоста.</summary>
    private sealed class DetachedRoutes(IServiceProvider services) : IEndpointRouteBuilder
    {
        public IServiceProvider ServiceProvider => services;

        public ICollection<EndpointDataSource> DataSources { get; } = [];

        public IApplicationBuilder CreateApplicationBuilder() => new ApplicationBuilder(services);
    }

    [Fact]
    public async Task Remote_anonymous_asset_request_is_forbidden()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, remote: Remote);

        using var asset = await host.GetAsync("/pgbolt/" + CssUrl(host));

        Assert.Equal(HttpStatusCode.Forbidden, asset.StatusCode);
    }

    [Fact]
    public async Task Local_asset_request_does_not_evaluate_actions_policy()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        // Незарегистрированная политика: её вычисление бросило бы InvalidOperationException.
        await using var host = await DashboardHost.StartAsync(db, o => o.ActionsPolicy = "Missing",
            builder: b => TestAuthentication.Add(b.Services), remote: IPAddress.Loopback);

        using var asset = await host.GetAsync("/pgbolt/" + CssUrl(host));

        Assert.Equal(HttpStatusCode.OK, asset.StatusCode);
    }

    private static string CssUrl(DashboardHost host) => host.Services.GetRequiredService<DashboardAssets>().RelativeUrl("pgbolt.css");

    [Fact]
    public async Task Host_endpoint_in_dashboard_group_fails_with_clear_message()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, map: app =>
        {
            var g = app.MapPgBoltDashboard();
            g.MapGet("/host", () => "host");
            return g;
        });

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => host.GetAsync("/pgbolt/host"));

        Assert.Contains("dashboard group", e.Message, StringComparison.Ordinal);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static RouteGroupBuilder MapCanAct(WebApplication app)
    {
        var g = app.MapPgBoltDashboard();
        DashboardRoutes.Get(g, "/can-act", (HttpContext http) => DashboardContext.Of(http).CanAct.ToString());
        return g;
    }

    private static async Task<List<string>> StartupWarningsAsync(TestDb db, Action<PgBoltDashboardOptions>? dashboard = null,
        Action<WebApplicationBuilder>? builder = null, Func<WebApplication, RouteGroupBuilder>? map = null)
    {
        var logs = new ListLogger();
        await using var host = await DashboardHost.StartAsync(db, dashboard, b =>
        {
            b.Logging.AddProvider(logs);
            builder?.Invoke(b);
        }, map: map);
        return logs.Entries.Where(e => e.Level == LogLevel.Warning && e.Message.StartsWith("PgBolt dashboard", StringComparison.Ordinal))
            .Select(e => e.Message).ToList();
    }

    private static async Task<HttpStatusCode> StatusAsync(DashboardHost host)
    {
        using var response = await host.GetAsync("/pgbolt/");
        return response.StatusCode;
    }

    private static async Task<string> BodyAsync(Task<HttpResponseMessage> request)
    {
        using var response = await request;
        return await response.Content.ReadAsStringAsync(Ct);
    }
}
