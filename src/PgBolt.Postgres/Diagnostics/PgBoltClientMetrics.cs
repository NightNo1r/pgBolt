using System.Diagnostics;
using System.Diagnostics.Metrics;
using static PgBolt.Postgres.Diagnostics.PgBoltTelemetry;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>Значения тега <c>pgbolt.enqueue.result</c>.</summary>
internal static class EnqueueResults
{
    public const string Created = "created";
    public const string Duplicate = "duplicate";
}

/// <summary>Метрики клиентского API. Запись под защитой: сбой измерительного callback'а не ломает вызов клиента.</summary>
internal sealed class PgBoltClientMetrics(PgBoltInstrumentation instrumentation)
{
    private readonly Counter<long> _enqueued =
        instrumentation.Meter.CreateCounter<long>(PgBoltDiagnostics.Instruments.JobEnqueued, "{job}");

    /// <summary>Число поставленных строк одной тройки (очередь, тип, результат); <paramref name="count"/> = 0 не пишется.</summary>
    public void Enqueued(string queue, string jobType, string result, int count) =>
        GuardedInstruments.Add(_enqueued, count,
            new TagList { { Attr.Queue, queue }, { Attr.JobType, jobType }, { Attr.EnqueueResult, result } }, instrumentation);
}
