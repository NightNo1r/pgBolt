using System.Diagnostics;
using System.Diagnostics.Metrics;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace PgBolt.OpenTelemetry.Tests;

public class InstrumentationTests
{
    [Fact]
    public void Tracer_extension_subscribes_PgBolt_source()
    {
        var exported = new List<Activity>();
        using var provider = Sdk.CreateTracerProviderBuilder()
            .AddPgBoltInstrumentation()
            .AddInMemoryExporter(exported)
            .Build();
        using var source = new ActivitySource(PgBoltDiagnostics.ActivitySourceName);

        source.StartActivity("x")!.Dispose();

        Assert.Contains(exported, a => a.OperationName == "x");
    }

    [Fact]
    public void Meter_extension_applies_histogram_buckets()
    {
        var exported = new List<Metric>();
        using var provider = Sdk.CreateMeterProviderBuilder()
            .AddPgBoltInstrumentation()
            .AddInMemoryExporter(exported)
            .Build();
        using var meter = new Meter(PgBoltDiagnostics.MeterName);
        var expected = new Dictionary<string, IReadOnlyList<double>>
        {
            [PgBoltDiagnostics.Instruments.JobDuration] = PgBoltDiagnostics.HistogramBuckets.Job,
            [PgBoltDiagnostics.Instruments.JobWaitTime] = PgBoltDiagnostics.HistogramBuckets.Job,
            [PgBoltDiagnostics.Instruments.MaintenanceDuration] = PgBoltDiagnostics.HistogramBuckets.Maintenance,
        };
        foreach (var name in expected.Keys)
            meter.CreateHistogram<double>(name).Record(1);

        provider.ForceFlush();

        foreach (var (name, boundaries) in expected)
        {
            var metric = Assert.Single(exported, m => m.Name == name);
            var point = Assert.Single(ToList(metric));
            var actual = new List<double>();
            foreach (var bucket in point.GetHistogramBuckets())
                if (!double.IsPositiveInfinity(bucket.ExplicitBound))
                    actual.Add(bucket.ExplicitBound);
            Assert.Equal(boundaries, actual);
        }
    }

    private static List<MetricPoint> ToList(Metric metric)
    {
        var points = new List<MetricPoint>();
        foreach (var point in metric.GetMetricPoints())
            points.Add(point);
        return points;
    }
}
