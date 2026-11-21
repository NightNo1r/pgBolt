using System.Diagnostics;

namespace PgBolt.LoadTest.Load;

/// <summary>Shared run clock in microseconds since start. Hosts and producer run in the same process, so their timestamps are
/// comparable.</summary>
internal static class RunClock
{
    private static readonly Stopwatch Watch = Stopwatch.StartNew();

    public static long NowMicros => Watch.Elapsed.Ticks / TimeSpan.TicksPerMicrosecond;

    /// <summary>Elapsed time since the run started.</summary>
    public static TimeSpan Now => TimeSpan.FromMicroseconds(NowMicros);

    /// <summary>Waits until <paramref name="at"/> on the run clock. Returns immediately if the time has passed.</summary>
    public static Task DelayUntilAsync(TimeSpan at, CancellationToken ct)
    {
        var wait = at - Now;
        return Task.Delay(wait > TimeSpan.Zero ? wait : TimeSpan.Zero, ct);
    }

    /// <summary>Restarts the clock at the beginning of a run.</summary>
    public static void Reset() => Watch.Restart();
}
