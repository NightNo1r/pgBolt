using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests.Diagnostics;

public sealed class PgBoltInstrumentationTests
{
    private static ServiceProvider Build(bool metrics)
    {
        var services = new ServiceCollection();
        if (metrics)
            services.AddMetrics();
        services.AddPgBolt(o => o.ConnectionString = "Host=unused");
        return services.BuildServiceProvider();
    }

    [Fact]
    public void Meter_comes_from_factory_when_registered()
    {
        using var sp = Build(metrics: true);
        var meter = sp.GetRequiredService<PgBoltInstrumentation>().Meter;

        Assert.Equal("PgBolt", meter.Name);
        Assert.Equal(PgBoltInstrumentation.Version, meter.Version);
        Assert.Same(sp.GetRequiredService<IMeterFactory>(), meter.Scope);
    }

    [Fact]
    public void Two_containers_have_isolated_meters()
    {
        using var sp1 = Build(metrics: true);
        using var sp2 = Build(metrics: true);
        var counter1 = sp1.GetRequiredService<PgBoltInstrumentation>().Meter.CreateCounter<long>("x");
        var counter2 = sp2.GetRequiredService<PgBoltInstrumentation>().Meter.CreateCounter<long>("x");
        using var collector1 = new MetricCollector<long>(sp1.GetRequiredService<IMeterFactory>(), "PgBolt", "x");
        using var collector2 = new MetricCollector<long>(sp2.GetRequiredService<IMeterFactory>(), "PgBolt", "x");

        counter1.Add(1);
        counter2.Add(5);

        Assert.Equal(1, Assert.Single(collector1.GetMeasurementSnapshot()).Value);
        Assert.Equal(5, Assert.Single(collector2.GetMeasurementSnapshot()).Value);
    }

    [Fact]
    public void Own_meter_is_disposed_with_container()
    {
        var sp = Build(metrics: false);
        var meter = sp.GetRequiredService<PgBoltInstrumentation>().Meter;
        var counter = meter.CreateCounter<long>("x");
        var completed = false;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == meter)
                l.EnableMeasurementEvents(instrument);
        };
        listener.MeasurementsCompleted = (instrument, _) =>
        {
            if (instrument == counter)
                completed = true;
        };
        listener.Start();

        sp.Dispose();

        Assert.True(completed);
    }

    private sealed class Fixture : IDisposable
    {
        public FakeTimeProvider Time { get; } = new();
        public ListLogger Logs { get; } = new();
        public PgBoltInstrumentation Instrumentation { get; }
        public Fixture() => Instrumentation = new PgBoltInstrumentation(null, Time, Logs);
        public void Dispose() => Instrumentation.Dispose();
    }

    [Fact]
    public void ReportFailure_logs_8001_once_per_minute()
    {
        using var f = new Fixture();
        var first = new InvalidOperationException("first");

        f.Instrumentation.ReportFailure(first);
        f.Instrumentation.ReportFailure(new InvalidOperationException("second"));
        f.Instrumentation.ReportFailure(new InvalidOperationException("third"));

        var entry = Assert.Single(f.Logs.Entries);
        Assert.Equal(8001, entry.EventId.Id);
        Assert.Equal("TelemetryFailed", entry.EventId.Name);
        Assert.Equal("PgBolt.Telemetry", entry.Category);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Same(first, entry.Exception);

        f.Time.Advance(TimeSpan.FromMinutes(1));
        f.Instrumentation.ReportFailure(new InvalidOperationException("fourth"));
        Assert.Equal(2, f.Logs.Entries.Count);
    }

    [Fact]
    public void ReportCallbackFailure_logs_8002_once_per_minute_with_its_own_limiter()
    {
        using var f = new Fixture();
        var first = new InvalidOperationException("first");

        f.Instrumentation.ReportFailure(new InvalidOperationException("other signal"));
        f.Instrumentation.ReportCallbackFailure(first);
        f.Instrumentation.ReportCallbackFailure(new InvalidOperationException("second"));

        var entry = Assert.Single(f.Logs.Entries, e => e.EventId.Id == 8002);
        Assert.Equal("MetricCallbackFailed", entry.EventId.Name);
        Assert.Equal("PgBolt.Telemetry", entry.Category);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Same(first, entry.Exception);

        f.Time.Advance(TimeSpan.FromMinutes(1));
        f.Instrumentation.ReportCallbackFailure(new InvalidOperationException("third"));
        Assert.Equal(2, f.Logs.Entries.Count(e => e.EventId.Id == 8002));
    }

    [Fact]
    public void ReportFailure_swallows_logger_exception()
    {
        using var f = new Fixture();
        f.Logs.OnEntry = _ => throw new InvalidOperationException("provider failed");

        f.Instrumentation.ReportFailure(new Exception("telemetry"));
        f.Time.Advance(TimeSpan.FromMinutes(1));
        f.Instrumentation.ReportCallbackFailure(new Exception("callback"));

        Assert.Equal(2, f.Logs.Entries.Count);
    }
}
