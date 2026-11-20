namespace PgBolt.LoadTest.Measure;

internal enum TickDecision
{
    /// <summary>Предыдущий срез ещё идёт: тик пропускается.</summary>
    Skip,
    Sample,
    /// <summary>Срез вместе с 10-секундным счётом отложенных задач и задач в lease.</summary>
    SampleWithCounts,
}

/// <summary>
/// Расписание среза без ввода-вывода. Тики не накапливаются: тик во время идущего среза пропускается,
/// следующий срез — на первом тике после завершения. Попытка, в которой не выполнился ни один запрос,
/// неудачна; <paramref name="maxConsecutiveFailures"/> неудач подряд означают потерю соединения стенда.
/// Не потокобезопасно: вызывающий сериализует вызовы.
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
