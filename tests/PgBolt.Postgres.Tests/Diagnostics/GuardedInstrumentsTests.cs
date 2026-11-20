using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests.Diagnostics;

public sealed class GuardedInstrumentsTests : IDisposable
{
    private readonly ListLogger _logs = new();
    private readonly PgBoltInstrumentation _instrumentation;
    private readonly Meter _meter = new("PgBolt.Tests.Guarded");

    public GuardedInstrumentsTests() => _instrumentation = new PgBoltInstrumentation(null, new FakeTimeProvider(), _logs);

    public void Dispose()
    {
        _meter.Dispose();
        _instrumentation.Dispose();
    }

    [Fact]
    public void Add_skips_non_positive_counts()
    {
        var counter = _meter.CreateCounter<long>("c");
        using var collector = new MetricCollector<long>(counter);

        GuardedInstruments.Add(counter, 0, default, _instrumentation);
        GuardedInstruments.Add(counter, -1, default, _instrumentation);
        GuardedInstruments.Add(counter, 3, default, _instrumentation);

        Assert.Equal(3, Assert.Single(collector.GetMeasurementSnapshot()).Value);
    }

    [Fact]
    public void Record_keeps_zero()
    {
        var histogram = _meter.CreateHistogram<double>("h");
        using var collector = new MetricCollector<double>(histogram);

        GuardedInstruments.Record(histogram, 0, default, _instrumentation);

        Assert.Equal(0, Assert.Single(collector.GetMeasurementSnapshot()).Value);
    }

    [Fact]
    public void Throwing_listener_is_reported_not_thrown()
    {
        var counter = _meter.CreateCounter<long>("c");
        var histogram = _meter.CreateHistogram<double>("h");
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == _meter)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, _, _, _) => throw new InvalidOperationException("listener"));
        listener.SetMeasurementEventCallback<double>((_, _, _, _) => throw new InvalidOperationException("listener"));
        listener.Start();

        GuardedInstruments.Add(counter, 1, default, _instrumentation);
        GuardedInstruments.Record(histogram, 1, default, _instrumentation);

        Assert.Single(_logs.Entries, e => e.EventId.Id == 8001);
    }
}
