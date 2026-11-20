using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Сверка счётчика выполнений: у каждого выданного <c>Seq</c> ожидается 2 выполнения при
/// <c>FailFirstAttempt</c> и 1 у остальных. <c>Seq</c> вызовов <c>EnqueueManyAsync</c>, завершившихся
/// исключением, не сверяются: строки могли закоммититься (таймаут после commit) или нет.
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
