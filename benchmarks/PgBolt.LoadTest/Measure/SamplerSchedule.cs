namespace PgBolt.LoadTest.Measure;

internal enum TickDecision
{
    /// <summary>Skips the tick while the previous sample is still running.</summary>
    Skip,
    Sample,
    /// <summary>Sample that includes the 10-second count of delayed and leased jobs.</summary>
    SampleWithCounts,
}

/// <summary>
/// Sampling schedule without I/O. Ticks never accumulate. A tick during collection is skipped, and the next sample starts on the first tick
/// after completion. An attempt fails if every query fails. After <paramref name="maxConsecutiveFailures"/> consecutive failures, the harness
/// considers the connection lost. The caller serializes access.
/// </summary>
internal sealed class SamplerSchedule(TimeSpan period, int maxConsecutiveFailures = 30)
{
    private static readonly TimeSpan CountsPeriod = TimeSpan.FromSeconds(10);

    private bool _running;
    private TimeSpan? _lastCounts;
    private int _failureStreak;

    public TimeSpan Period => period;
    public long SkippedTicks { get; private set; }
    public long FailedAttempts { get; private set; }
    public bool Exhausted { get; private set; }

    public TickDecision OnTick(TimeSpan now)
    {
        if (_running)
        {
            SkippedTicks++;
            return TickDecision.Skip;
        }

        _running = true;
        if (_lastCounts is { } last && now - last < CountsPeriod)
            return TickDecision.Sample;
        _lastCounts = now;
        return TickDecision.SampleWithCounts;
    }

    public void OnCompleted(bool anyQueryOk)
    {
        _running = false;
        if (anyQueryOk)
        {
            _failureStreak = 0;
            return;
        }

        FailedAttempts++;
        if (++_failureStreak >= maxConsecutiveFailures)
            Exhausted = true;
    }
}
