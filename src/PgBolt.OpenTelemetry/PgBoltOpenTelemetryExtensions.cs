using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace PgBolt;

/// <summary>Подключение трассировки и метрик PgBolt к OpenTelemetry SDK.</summary>
public static class PgBoltOpenTelemetryExtensions
{
    /// <summary>Подписывает провайдер трассировки на источник span'ов PgBolt.</summary>
    public static TracerProviderBuilder AddPgBoltInstrumentation(this TracerProviderBuilder builder) =>
        builder.AddSource(PgBoltDiagnostics.ActivitySourceName);

    /// <summary>
    /// Подписывает провайдер метрик на <c>Meter</c> PgBolt и задаёт границы бакетов гистограмм View'ами:
    /// SDK, не учитывающие advice instrument'а, получают те же границы.
    /// </summary>
    public static MeterProviderBuilder AddPgBoltInstrumentation(this MeterProviderBuilder builder) =>
        builder
            .AddMeter(PgBoltDiagnostics.MeterName)
            .AddView(PgBoltDiagnostics.Instruments.JobDuration, Buckets(PgBoltDiagnostics.HistogramBuckets.Job))
            .AddView(PgBoltDiagnostics.Instruments.JobWaitTime, Buckets(PgBoltDiagnostics.HistogramBuckets.Job))
            .AddView(PgBoltDiagnostics.Instruments.MaintenanceDuration, Buckets(PgBoltDiagnostics.HistogramBuckets.Maintenance));

    private static ExplicitBucketHistogramConfiguration Buckets(IReadOnlyList<double> boundaries) =>
        new() { Boundaries = [.. boundaries] };
}
