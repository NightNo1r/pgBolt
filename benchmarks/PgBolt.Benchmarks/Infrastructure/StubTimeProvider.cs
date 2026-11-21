namespace PgBolt.Benchmarks.Infrastructure;

/// <summary>
/// Stub clock for LeaseKeeper. GetTimestamp returns 0 and frequency is TimeSpan.TicksPerSecond, so MonoClock.Until(d) equals d ticks. The stub
/// timer remembers the most recent deadline without firing on Change.
/// </summary>
internal sealed class StubTimeProvider : TimeProvider
{
    public StubTimer? Timer { get; private set; }

    public override long GetTimestamp() => 0;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
        Timer = new StubTimer(dueTime);
}

/// <summary>Stub timer. Change updates the deadline with Interlocked.Exchange for a full barrier, like the real timer's internal lock.</summary>
internal sealed class StubTimer(TimeSpan dueTime) : ITimer
{
    private long _dueTicks = dueTime.Ticks;

    /// <summary>Deadline in ticks relative to a current time of zero. long.MaxValue means the timer is disabled.</summary>
    public long Target
    {
        get
        {
            var due = Interlocked.Read(ref _dueTicks);
            return due < 0 ? long.MaxValue : due;
        }
    }

    public bool Change(TimeSpan dueTime, TimeSpan period)
    {
        Interlocked.Exchange(ref _dueTicks, dueTime.Ticks);
        return true;
    }

    public void Dispose() => Interlocked.Exchange(ref _dueTicks, -1);

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
