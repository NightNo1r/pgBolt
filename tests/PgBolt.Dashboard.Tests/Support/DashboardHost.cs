using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgBolt.Postgres;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Support;

/// <summary>WebApplication на TestServer с PgBolt core (схема TestDb) и dashboard.</summary>
public sealed class DashboardHost : IAsyncDisposable
{
    private readonly WebApplication _app;

    private DashboardHost(WebApplication app)
    {
        _app = app;
        Client = app.GetTestClient();
    }

    public HttpClient Client { get; }

    public IServiceProvider Services => _app.Services;

    public static async Task<DashboardHost> StartAsync(TestDb db, Action<PgBoltDashboardOptions>? dashboard = null,
        Action<WebApplicationBuilder>? builder = null, Action<WebApplication>? beforeMap = null,
        Func<WebApplication, RouteGroupBuilder>? map = null, IPAddress? remote = null, bool nullRemote = false,
        Action<PgBoltOptions>? core = null)
    {
        // Окружение не из переменных процесса: в Development WebApplication подключает DeveloperExceptionPage, и исключения
        // endpoint'ов становятся ответом 500 вместо исключения в клиенте.
        var b = WebApplication.CreateSlimBuilder(new WebApplicationOptions { EnvironmentName = Environments.Production });
        b.WebHost.UseTestServer();
        b.Services.AddPgBolt(o =>
        {
            o.DataSource = db.DataSource;
            o.Schema = db.Schema;
            o.AutoMigrate = false;
            core?.Invoke(o);
        });
        b.Services.AddPgBoltDashboard(dashboard);
        builder?.Invoke(b);

        var app = b.Build();
        app.Use((ctx, next) =>
        {
            ctx.Connection.RemoteIpAddress = nullRemote ? null : remote ?? IPAddress.Loopback;
            return next(ctx);
        });
        beforeMap?.Invoke(app);
        _ = map?.Invoke(app) ?? app.MapPgBoltDashboard();
        await app.StartAsync();
        return new DashboardHost(app);
    }

    public Task<HttpResponseMessage> GetAsync(string path, bool htmx = false, bool historyRestore = false, string? htmxTarget = null)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, path);
        if (htmx)
            request.Headers.Add("HX-Request", "true");
        if (historyRestore)
            request.Headers.Add("HX-History-Restore-Request", "true");
        if (htmxTarget is not null)
            request.Headers.Add("HX-Target", htmxTarget);
        return Client.SendAsync(request);
    }

    public Task<HttpResponseMessage> PostAsync(string path, IEnumerable<KeyValuePair<string, string>> form, bool htmx = true,
        string? fetchSite = "same-origin", string? origin = null)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, path) { Content = new FormUrlEncodedContent(form) };
        if (htmx)
            request.Headers.Add("HX-Request", "true");
        if (fetchSite is not null)
            request.Headers.Add("Sec-Fetch-Site", fetchSite);
        if (origin is not null)
            request.Headers.Add("Origin", origin);
        return Client.SendAsync(request);
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
