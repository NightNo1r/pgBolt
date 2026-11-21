namespace PgBolt.Server.Runtime;

/// <summary>
/// Limits log entries and periodic checks such as resolver lag to once per <paramref name="period"/> on the monotonic
/// clock.
/// </summary>
internal sealed class LogThrottle(MonoClock clock, TimeSpan period)
{
    private readonly Lock _lock = new();
    private long? _last;

    /// <returns>True on the first call or when the period has elapsed since the last accepted call.</returns>
    public bool TryEnter()
    {
        lock (_lock)
        {
            if (_last is { } last && clock.Elapsed(last) < period)
                return false;
            _last = clock.Now;
            return true;
        }
    }
}
