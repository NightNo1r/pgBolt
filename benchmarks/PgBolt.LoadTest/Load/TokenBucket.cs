namespace PgBolt.LoadTest.Load;

/// <summary>
/// Accumulates <c>rate</c> tokens per second up to capacity, counting and discarding excess tokens. Thread-safe so <c>ramp</c> stages can change
/// the rate from the scenario thread while the generator consumes tokens.
/// </summary>
internal sealed class TokenBucket
{
    private readonly object _gate = new();
    private readonly TimeProvider _time;
    private double _rate;
    private double _ceiling;
    private double _tokens;
    private double _dropped;
    private long _accruedAt;

    public TokenBucket(double ratePerSecond, double ceiling, TimeProvider time)
    {
        _time = time;
        _rate = ratePerSecond;
        _ceiling = ceiling;
        _accruedAt = time.GetTimestamp();
    }

    /// <summary>Tokens discarded above capacity, including accrual up to the time of reading.</summary>
    public long Dropped
    {
        get
        {
            lock (_gate)
            {
                Accrue();
                return (long)_dropped;
            }
        }
    }

    /// <summary>Whole tokens available now.</summary>
    public int Available()
    {
        lock (_gate)
        {
            Accrue();
            return (int)_tokens;
        }
    }

    /// <summary>Consumes tokens.</summary>
    public void Consume(int count)
    {
        lock (_gate) _tokens -= count;
    }

    /// <summary>
    /// Consumes one token if available. Checks and consumes under one <c>lock</c> so a rate decrease cannot reissue excess tokens already counted as
    /// dropped.
    /// </summary>
    public bool TryConsume()
    {
        lock (_gate)
        {
            if (_tokens < 1) return false;
            _tokens--;
            return true;
        }
    }

    /// <summary>Accrues tokens up to now at the old rate, then switches rates. The new capacity equals one second of the new rate.</summary>
    public void SetRate(double ratePerSecond)
    {
        lock (_gate)
        {
            Accrue();
            _rate = ratePerSecond;
            _ceiling = ratePerSecond;
        }
    }

    /// <summary>Accrues tokens since the last update. Call while holding <c>_gate</c>.</summary>
    private void Accrue()
    {
        var now = _time.GetTimestamp();
        _tokens += (now - _accruedAt) * _rate / _time.TimestampFrequency;
        _accruedAt = now;
        if (_tokens > _ceiling)
        {
            _dropped += _tokens - _ceiling;
            _tokens = _ceiling;
        }
    }
}
