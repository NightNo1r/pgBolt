using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>Records synchronous measurements and reports exceptions thrown by <see cref="Meter"/> listeners as event 8001.</summary>
internal static class GuardedInstruments
{
    /// <summary>Skips zero and negative counts because they represent no events.</summary>
    public static void Add(Counter<long> counter, long value, in TagList tags, PgBoltInstrumentation instrumentation)
    {
        if (value <= 0)
            return;
        try { counter.Add(value, tags); }
        catch (Exception e) { instrumentation.ReportFailure(e); }
    }

    /// <summary>Records zero as a valid measurement for an immediate wait or an empty job.</summary>
    public static void Record(Histogram<double> histogram, double value, in TagList tags, PgBoltInstrumentation instrumentation)
    {
        try { histogram.Record(value, tags); }
        catch (Exception e) { instrumentation.ReportFailure(e); }
    }
}
