namespace PgBolt.Benchmarks.Infrastructure;

/// <summary>
/// Часы-заглушка для LeaseKeeper: GetTimestamp = 0, частота = TimeSpan.TicksPerSecond, поэтому MonoClock.Until(d) = d тиков,
/// и таймер-заглушка, запоминающий последний рубеж (Change не срабатывает).
/// </summary>
internal sealed class StubTimeProvider : TimeProvider
{
    public StubTimer? Timer { get; private set; }

    public override long GetTimestamp() => 0;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
        Timer = new StubTimer(dueTime);
}

/// <summary>Таймер-заглушка: Change — Interlocked.Exchange рубежа (полный барьер, как lock внутри настоящего таймера).</summary>
internal sealed class StubTimer(TimeSpan dueTime) : ITimer
{
    private long _dueTicks = dueTime.Ticks;

    /// <summary>Рубеж в тиках при нулевом «сейчас»; long.MaxValue — таймер выключен.</summary>
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
