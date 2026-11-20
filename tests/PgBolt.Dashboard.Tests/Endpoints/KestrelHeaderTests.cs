using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

/// <summary>Настоящий Kestrel (не TestServer): не-ASCII в заголовке ответа он отвергает.</summary>
public sealed class KestrelHeaderTests(PostgresFixture pg)
{
    [Fact]
    public async Task Cyrillic_notification_passes_kestrel()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        var b = WebApplication.CreateSlimBuilder();
        b.WebHost.UseKestrel().UseUrls("http://127.0.0.1:0");
        b.Services.AddPgBolt(o =>
        {
            o.DataSource = db.DataSource;
            o.Schema = db.Schema;
            o.AutoMigrate = false;
        });
        b.Services.AddPgBoltDashboard();
        await using var app = b.Build();
        app.Use((ctx, next) =>
        {
            ctx.Connection.RemoteIpAddress = IPAddress.Loopback;
            return next(ctx);
        });
        app.MapPgBoltDashboard();
        await app.StartAsync(ct);
        var id = (await new JobSeeder(db, app.Services.GetRequiredService<PgBoltStore>()).SeedJobsAsync(1, "dash.job", "default", 0,
            "scheduled"))[0];
        using var http = new HttpClient { BaseAddress = new Uri(app.Urls.Single()) };
        using var request = new HttpRequestMessage(HttpMethod.Post, $"/pgbolt/actions/jobs/{id}/trigger") { Content = new FormUrlEncodedContent([]) };
        request.Headers.Add("HX-Request", "true");
        request.Headers.Add("Sec-Fetch-Site", "same-origin");

        using var response = await http.SendAsync(request, ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var header = response.Headers.GetValues("HX-Trigger").Single();
        Assert.Contains("\\u", header, StringComparison.Ordinal);
        using var json = JsonDocument.Parse(header);
        Assert.Equal("запущена", json.RootElement.GetProperty("pgbolt:notify").GetString());
        await app.StopAsync(ct);
    }
}
