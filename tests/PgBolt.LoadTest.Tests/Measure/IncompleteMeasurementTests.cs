using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class IncompleteMeasurementTests
{
    [Fact]
    public void PartialVacuumCannotPass()
    {
        var verdicts = new[] { Verdict.Pass, Verdict.NoData, Verdict.Pass };
        Assert.Equal(Verdict.NoData, SloEvaluator.VacuumStatus(verdicts));
        Assert.Equal(Verdict.Fail, SloEvaluator.VacuumStatus([.. verdicts, Verdict.Fail]));
    }

    [Fact]
    public void MissingInnerReadIsVisible()
    {
        var measured = new StatementClassStats(1, 1, 1, 1, 1);
        var stats = new StatementsSummary(new Dictionary<string, StatementClassStats>
        {
            [StatementClasses.DequeueLimited] = measured,
        });
        var missing = stats.MissingStats([StatementClasses.DequeueLimited, StatementClasses.ReadJobs, StatementClasses.QueueLock]);
        Assert.Contains(StatementClasses.ReadJobs, missing);
        Assert.Contains(StatementClasses.QueueLock, missing);
        Assert.DoesNotContain(StatementClasses.ReadJobs, stats.MissingStats([StatementClasses.DequeueLimited, StatementClasses.QueueLock]));
    }
}
