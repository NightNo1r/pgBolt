using System.Diagnostics;
using System.Diagnostics.Metrics;
using static PgBolt.Postgres.Diagnostics.PgBoltTelemetry;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>Values for the <c>pgbolt.enqueue.result</c> tag.</summary>
internal static class EnqueueResults
{
    public const string Created = "created";
    public const string Duplicate = "duplicate";
}

/// <summary>Client API metrics. Measurement callback failures are isolated from client calls.</summary>
internal sealed class PgBoltClientMetrics(PgBoltInstrumentation instrumentation)
{
    private readonly Counter<long> _enqueued =
        instrumentation.Meter.CreateCounter<long>(PgBoltDiagnostics.Instruments.JobEnqueued, "{job}");

    /// <summary>Number of rows enqueued for one (queue, type, result) combination. Skips a <paramref name="count"/> of zero.</summary>
    public void Enqueued(string queue, string jobType, string result, int count) =>
        GuardedInstruments.Add(_enqueued, count,
            new TagList { { Attr.Queue, queue }, { Attr.JobType, jobType }, { Attr.EnqueueResult, result } }, instrumentation);
}
