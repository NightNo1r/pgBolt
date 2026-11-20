namespace PgBolt.LoadTest.Measure;

internal sealed record FetchSummary(LatencySummary Duration, long Empty, long Failed, long Sent, LatencySummary QueueLock);

// Два хоста пишут в одно окно. Закрытие окна и последние отсчёты упорядочены одним lock.
internal sealed class FetchRecorder
{
    private readonly Lock _gate = new();
    private readonly LatencyRecorder _fetch = new();
    private readonly LatencyRecorder _queueLock = new();
    private bool _active;
    private long _empty;
    private long _failed;
    private long _sent;

    public void Begin()
    {
        lock (_gate) _active = true;
    }

    public void Fetch(TimeSpan elapsed, int? jobs, int sends)
    {
        lock (_gate)
        {
            if (!_active) { return; }
            _fetch.Record(elapsed.Ticks / TimeSpan.TicksPerMicrosecond);
            _sent += sends;
            if (jobs is null) { _failed++; }
            else if (jobs == 0) { _empty++; }
        }
    }

    public void QueueLock(long micros)
    {
        lock (_gate)
        {
            if (_active) { _queueLock.Record(micros); }
        }
    }

    public FetchSummary End()
    {
        lock (_gate)
        {
            _active = false;
            return new(_fetch.CloseWindow(), _empty, _failed, _sent, _queueLock.CloseWindow());
        }
    }
}
