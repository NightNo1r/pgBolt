namespace PgBolt.Server.Runtime;

/// <summary>Не чаще раза в <paramref name="period"/> по монотонным часам: записи лога, периодические проверки (лаг resolver'а).</summary>
internal sealed class LogThrottle(MonoClock clock, TimeSpan period)
{
    private readonly Lock _lock = new();
    private long? _last;

    /// <returns>true — вызовов ещё не было или с последнего прошёл период.</returns>
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
