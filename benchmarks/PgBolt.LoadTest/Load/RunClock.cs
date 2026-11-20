using System.Diagnostics;

namespace PgBolt.LoadTest.Load;

/// <summary>
/// Общие часы прогона: микросекунды от его начала. Хосты и продюсер живут в одном процессе,
/// поэтому метки времени одних сравнимы с метками других.
/// </summary>
internal static class RunClock
{
    private static readonly Stopwatch Watch = Stopwatch.StartNew();

    public static long NowMicros => Watch.Elapsed.Ticks / TimeSpan.TicksPerMicrosecond;

    /// <summary>Время от начала прогона.</summary>
    public static TimeSpan Now => TimeSpan.FromMicroseconds(NowMicros);

    /// <summary>Ждёт момента <paramref name="at"/> по часам прогона; прошедший момент — без ожидания.</summary>
    public static Task DelayUntilAsync(TimeSpan at, CancellationToken ct)
    {
        var wait = at - Now;
        return Task.Delay(wait > TimeSpan.Zero ? wait : TimeSpan.Zero, ct);
    }

    /// <summary>Начинает отсчёт заново; вызывается в начале прогона.</summary>
    public static void Reset() => Watch.Restart();
}
