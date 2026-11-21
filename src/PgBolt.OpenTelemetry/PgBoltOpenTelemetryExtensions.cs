using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace PgBolt;

/// <summary>Connects PgBolt traces and metrics to the OpenTelemetry SDK.</summary>
public static class PgBoltOpenTelemetryExtensions
{
    /// <summary>Subscribes the tracer provider to the PgBolt activity source.</summary>
    public static TracerProviderBuilder AddPgBoltInstrumentation(this TracerProviderBuilder builder) =>
        builder.AddSource(PgBoltDiagnostics.ActivitySourceName);

    /// <summary>
    /// Subscribes the meter provider to the PgBolt <c>Meter</c> and configures histogram views so SDKs that ignore instrument advice use the same
    /// bucket boundaries.
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
