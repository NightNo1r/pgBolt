using PgBolt.Dashboard.Views;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed class StatsFreshnessTests
{
    private static readonly DateTime T0 = new(2026, 10, 5, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan StaleAfter = TimeSpan.FromMinutes(1);

    [Fact]
    public void No_row_is_not_running() =>
        Assert.Equal(StatsFreshnessKind.NotRunning, StatsFreshness.Evaluate(null, T0, StaleAfter).Kind);

    [Fact]
    public void Overdue_beyond_stale_after_plus_half_claim_gap_is_not_running()
    {
        var next = T0.AddSeconds(8);
        var row = new StatsTaskRow(next, T0, null);

        var overdue = StatsFreshness.Evaluate(row, next + TimeSpan.FromMinutes(1) + TimeSpan.FromSeconds(4) + TimeSpan.FromMilliseconds(1), StaleAfter);
        var tolerated = StatsFreshness.Evaluate(row, next + TimeSpan.FromMinutes(1) + TimeSpan.FromSeconds(3), StaleAfter);

        Assert.Equal(StatsFreshnessKind.NotRunning, overdue.Kind);
        Assert.Equal(StatsFreshnessKind.Fresh, tolerated.Kind);
    }

    [Fact]
    public void Long_interval_has_no_false_banner()
    {
        var next = T0.AddMinutes(8);

        var freshness = StatsFreshness.Evaluate(new StatsTaskRow(next, T0, null), next.AddMinutes(3), StaleAfter);

        Assert.Equal(StatsFreshnessKind.Fresh, freshness.Kind);
    }

    [Fact]
    public void Last_error_is_failing_with_text()
    {
        var next = T0.AddSeconds(8);
        var row = new StatsTaskRow(next, T0, "boom");

        var fresh = StatsFreshness.Evaluate(row, next, StaleAfter);
        var overdue = StatsFreshness.Evaluate(row, next.AddMinutes(10), StaleAfter);

        Assert.Equal(new StatsFreshness(StatsFreshnessKind.Failing, "boom"), fresh);
        Assert.Equal(StatsFreshnessKind.NotRunning, overdue.Kind);
    }
}
