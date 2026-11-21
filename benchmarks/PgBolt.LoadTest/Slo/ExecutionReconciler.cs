using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Reconciles executions per issued <c>Seq</c>. Jobs with <c>FailFirstAttempt</c> should execute twice and the rest once. Excludes <c>Seq</c>
/// values from <c>EnqueueManyAsync</c> calls that threw because a timeout after commit leaves the commit outcome unknown.
/// </summary>
internal static class ExecutionReconciler
{
    public static (long Missing, long Duplicated, long ExcludedFailedCalls) Reconcile(
        ExecutionCounter counter, long issued, IReadOnlySet<long> failFirst, IReadOnlySet<long> failedCallSeqs)
    {
        long missing = 0, duplicated = 0, excluded = 0;
        for (long seq = 0; seq < issued; seq++)
        {
            if (failedCallSeqs.Contains(seq))
            {
                excluded++;
                continue;
            }

            var expected = failFirst.Contains(seq) ? 2 : 1;
            var actual = counter.Get(seq);
            if (actual < expected) missing++;
            else if (actual > expected) duplicated++;
        }

        return (missing, duplicated, excluded);
    }
}
