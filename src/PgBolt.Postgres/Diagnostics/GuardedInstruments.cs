using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>Синхронная запись измерений под защитой: слушатель <see cref="Meter"/> может бросить, сбой уходит в 8001.</summary>
internal static class GuardedInstruments
{
    /// <summary>Нулевое и отрицательное число — не событие: измерение не пишется.</summary>
    public static void Add(Counter<long> counter, long value, in TagList tags, PgBoltInstrumentation instrumentation)
    {
        if (value <= 0)
            return;
        try { counter.Add(value, tags); }
        catch (Exception e) { instrumentation.ReportFailure(e); }
    }

    /// <summary>Ноль — допустимое значение (мгновенное ожидание, пустая задача) и записывается.</summary>
    public static void Record(Histogram<double> histogram, double value, in TagList tags, PgBoltInstrumentation instrumentation)
    {
        try { histogram.Record(value, tags); }
        catch (Exception e) { instrumentation.ReportFailure(e); }
    }
}
