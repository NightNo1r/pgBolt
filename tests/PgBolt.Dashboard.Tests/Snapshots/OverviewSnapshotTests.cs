using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Components.Pages;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Views;
using static VerifyXunit.Verifier;

namespace PgBolt.Dashboard.Tests.Snapshots;

public sealed class OverviewSnapshotTests
{
    [Fact]
    public async Task Overview()
    {
        var start = new DateTime(2026, 10, 5, 11, 1, 0, DateTimeKind.Utc);
        var hour = Enumerable.Range(0, 60)
            .Select(i => new ThroughputPoint(start.AddMinutes(i), i % 7 == 0 ? 8 : i % 5, i % 10 == 0 ? 2 : 0, i % 15 == 0 ? 1 : 0))
            .ToList();
        var data = new OverviewData(new QueueTotals(1, 2, 3, 4), 2, new StatsFreshness(StatsFreshnessKind.Failing, "disk <full>"), hour);
        var context = new DashboardContext("/pgbolt", CanAct: true, TimeSpan.FromSeconds(5), TimeSpan.FromDays(7),
            "assets/0/pgbolt.css", "assets/0/htmx.min.js", "assets/0/pgbolt.js");
        await using var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        var http = new DefaultHttpContext { RequestServices = services };

        var html = await DashboardView.RenderAsync(http, typeof(OverviewPage), new Dictionary<string, object?>
        {
            [nameof(OverviewPage.Data)] = data,
            [nameof(OverviewPage.Context)] = context,
        });

        await Verify(html, "html");
    }
}
