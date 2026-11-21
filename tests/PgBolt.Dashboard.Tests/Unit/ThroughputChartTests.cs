using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Views;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed class ThroughputChartTests
{
    [Fact]
    public async Task Only_retried_is_no_data_without_bars()
    {
        var at = new DateTime(2026, 10, 5, 11, 0, 0, DateTimeKind.Utc);
        await using var services = new ServiceCollection().AddLogging().BuildServiceProvider();

        var html = await DashboardView.RenderAsync(new DefaultHttpContext { RequestServices = services }, typeof(ThroughputChart),
            new Dictionary<string, object?> { [nameof(ThroughputChart.Points)] = new[] { new ThroughputPoint(at, 0, 0, 3) } });

        Assert.Contains("No data", html, StringComparison.Ordinal);
        Assert.DoesNotContain("<svg", html, StringComparison.Ordinal);
    }
}
