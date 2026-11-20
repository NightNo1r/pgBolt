using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class StatementClassStatsTests
{
    [Fact]
    public void Zero_calls_and_plans_have_no_means()
    {
        var stats = StatementsReader.Stats(calls: 0, plans: 0, totalPlan: 1.5, totalExec: 2.5, trackPlanning: true);

        Assert.Null(stats.MeanPlanTime);
        Assert.Null(stats.MeanExecTime);
        Assert.Equal(1.5, stats.TotalPlanTime);
        Assert.Equal(2.5, stats.TotalExecTime);
    }

    [Fact]
    public void Means_divide_totals_by_calls_and_plan_needs_track_planning()
    {
        var stats = StatementsReader.Stats(calls: 4, plans: 4, totalPlan: 2, totalExec: 8, trackPlanning: false);

        Assert.Equal(new StatementClassStats(4, null, 2, null, 8), stats);
    }

    [Fact]
    public void Mean_plan_time_divides_by_plans_not_calls()
    {
        // Generic plan: план строится редко, выполнений много.
        var stats = StatementsReader.Stats(calls: 100, plans: 2, totalPlan: 10, totalExec: 50, trackPlanning: true);

        Assert.Equal(5, stats.MeanPlanTime);
        Assert.Equal(0.5, stats.MeanExecTime);
    }
}
