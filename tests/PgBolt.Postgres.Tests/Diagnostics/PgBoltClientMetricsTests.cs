using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using PgBolt.Postgres.Diagnostics;

namespace PgBolt.Postgres.Tests.Diagnostics;

public sealed class PgBoltClientMetricsTests
{
    private static ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddMetrics();
        services.AddPgBolt(o => o.ConnectionString = "Host=unused");
        return services.BuildServiceProvider();
    }

    [Fact]
    public void Enqueued_adds_with_tags()
    {
        using var sp = Build();
        using var collector = new MetricCollector<long>(sp.GetRequiredService<IMeterFactory>(), PgBoltDiagnostics.MeterName,
            PgBoltDiagnostics.Instruments.JobEnqueued);

        var metrics = sp.GetRequiredService<PgBoltClientMetrics>();
        metrics.Enqueued("q", "t", EnqueueResults.Created, 2);
        metrics.Enqueued("q", "t", EnqueueResults.Duplicate, 0);

        var measurement = Assert.Single(collector.GetMeasurementSnapshot());
        Assert.Equal(2, measurement.Value);
        Assert.Equal("q", measurement.Tags["pgbolt.queue"]);
        Assert.Equal("t", measurement.Tags["pgbolt.job.type"]);
        Assert.Equal("created", measurement.Tags["pgbolt.enqueue.result"]);
    }
}
