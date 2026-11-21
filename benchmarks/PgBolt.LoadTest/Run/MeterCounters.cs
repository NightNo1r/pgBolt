using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using PgBolt.LoadTest.Report;
using PgBolt.Postgres.Diagnostics;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// PgBolt <c>Meter</c> counters across all hosts in the process: <c>outcome.dropped</c>, <c>storage.errors</c> by operation, and
/// <c>job.enqueued</c>. Create before host startup to capture all measurements.
/// </summary>
internal sealed class MeterCounters : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly ConcurrentDictionary<string, long> _storageErrors = new();
    private long _dropped;
    private long _enqueued;

    public MeterCounters()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == PgBoltDiagnostics.MeterName
                && instrument.Name is I.JobOutcomeDropped or I.StorageErrors or I.JobEnqueued)
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>(OnMeasurement);
        _listener.Start();
    }

    public PgBoltCounters Snapshot() =>
        new(Interlocked.Read(ref _dropped), new Dictionary<string, long>(_storageErrors), Interlocked.Read(ref _enqueued));

    private void OnMeasurement(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        switch (instrument.Name)
        {
            case I.JobOutcomeDropped:
                Interlocked.Add(ref _dropped, value);
                break;
            case I.JobEnqueued:
                Interlocked.Add(ref _enqueued, value);
                break;
            default:
                var operation = "";
                foreach (var tag in tags)
                    if (tag.Key == PgBoltTelemetry.Attr.StorageOperation) operation = tag.Value?.ToString() ?? "";
                _storageErrors.AddOrUpdate(operation, value, (_, sum) => sum + value);
                break;
        }
    }

    public void Dispose() => _listener.Dispose();
}
