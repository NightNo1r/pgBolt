using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

/// <summary>Exercises header encoding with Kestrel, which rejects non-ASCII response header values.</summary>
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

        // A user-supplied owner keeps the notification non-ASCII even in the English dashboard.
        await db.ExecAsync("""
            INSERT INTO {schema}.recurring_jobs
                (id, type, args, cron, time_zone, misfire, catch_up_window, overlap, owner, created_at, updated_at)
            VALUES ('daily', 'dash.job', '{}', '0 0 * * *', 'UTC', 0, '1 day', 0, 'отчёты', now(), now())
            """);

        using var http = new HttpClient { BaseAddress = new Uri(app.Urls.Single()) };
        using var request = new HttpRequestMessage(HttpMethod.Post, "/pgbolt/actions/recurring/delete?id=daily") { Content = new FormUrlEncodedContent([]) };
        request.Headers.Add("HX-Request", "true");
        request.Headers.Add("Sec-Fetch-Site", "same-origin");

        using var response = await http.SendAsync(request, ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var header = response.Headers.GetValues("HX-Trigger").Single();
        Assert.Contains("\\u", header, StringComparison.Ordinal);
        using var json = JsonDocument.Parse(header);
        Assert.Equal("schedule defined in code (owner отчёты), cannot delete", json.RootElement.GetProperty("pgbolt:notify").GetString());
        await app.StopAsync(ct);
    }
}
