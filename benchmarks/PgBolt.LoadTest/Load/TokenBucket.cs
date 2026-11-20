namespace PgBolt.LoadTest.Load;

/// <summary>
/// Token bucket: копит <c>rate</c> токенов в секунду не выше потолка; излишек сверх потолка отбрасывает и считает.
/// Потокобезопасен: ступени <c>ramp</c> меняют поток из потока сценария, а цикл генератора берёт токены.
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

    /// <summary>Сколько токенов отброшено сверх потолка; учитывает время до момента чтения.</summary>
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

    /// <summary>Целое число токенов, доступных сейчас.</summary>
    public int Available()
    {
        lock (_gate)
        {
            Accrue();
            return (int)_tokens;
        }
    }

    /// <summary>Списывает токены.</summary>
    public void Consume(int count)
    {
        lock (_gate) _tokens -= count;
    }

    /// <summary>
    /// Списывает один токен, если он есть сейчас. Проверка и списание под одним <c>lock</c>: после снижения
    /// потока токены, сверх нового потолка уже учтённые как отброшенные, повторно не выдаются.
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

    /// <summary>
    /// Меняет поток: накопленное до вызова начислено по старому потоку, дальше идёт новый.
    /// Потолок — секунда нового потока.
    /// </summary>
    public void SetRate(double ratePerSecond)
    {
        lock (_gate)
        {
            Accrue();
            _rate = ratePerSecond;
            _ceiling = ratePerSecond;
        }
    }

    /// <summary>Начисляет токены за время с прошлого начисления. Вызывать под <c>_gate</c>.</summary>
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
