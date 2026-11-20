using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Fakes;

/// <summary>Телеметрия для unit-тестов: у каждого экземпляра свой <c>Meter</c>, измерения тестов не смешиваются.</summary>
internal static class TestTelemetry
{
    public static PgBoltInstrumentation Instrumentation(TimeProvider? time = null, ILoggerFactory? logs = null) =>
        new(null, time ?? TimeProvider.System, logs);

    public static PgBoltMetrics Metrics(PgBoltInstrumentation? i = null, ClusterQueueStats? cluster = null,
        ClusterTableStats? tables = null)
    {
        var clock = new MonoClock(TimeProvider.System);
        var options = new PgBoltProcessingOptions();
        return new(i ?? Instrumentation(), cluster ?? new ClusterQueueStats(clock, options), tables ?? new ClusterTableStats(clock, options));
    }

    /// <summary>Сумма значений счётчика по измерениям с тегом <paramref name="tag"/> = <paramref name="value"/>.</summary>
    public static long Sum(MetricCollector<long> collector, string tag, string value) =>
        collector.GetMeasurementSnapshot().Where(m => Equals(m.Tags[tag], value)).Sum(m => m.Value);
}

/// <summary>
/// Непараллельная коллекция: слушатели источника <c>PgBolt</c> и бросающие <c>MeterListener</c> видят и ломают
/// измерения и span'ы соседних тестов, если те идут одновременно.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TelemetryCollection
{
    public const string Name = "telemetry";
}
