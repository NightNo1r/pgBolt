namespace PgBolt.Server.Runtime;

/// <summary>
/// Monotonic runtime clock built on <see cref="TimeProvider.GetTimestamp"/>. Timestamps use
/// <see cref="TimeProvider.TimestampFrequency"/> units, and intervals use <see cref="TimeSpan"/>. Independent of
/// wall-clock time.
/// </summary>
internal sealed class MonoClock(TimeProvider time)
{
    public TimeProvider Time { get; } = time ?? throw new ArgumentNullException(nameof(time));

    public long Now => Time.GetTimestamp();

    /// <summary>Adds <paramref name="duration"/> to <paramref name="timestamp"/>. The duration can be negative.</summary>
    public long Add(long timestamp, TimeSpan duration) =>
        timestamp + (long)((Int128)duration.Ticks * Time.TimestampFrequency / TimeSpan.TicksPerSecond);

    /// <summary>
    /// Time remaining until <paramref name="timestamp"/>, at least zero. Rounds up so the wait cannot finish early.
    /// </summary>
    public TimeSpan Until(long timestamp)
    {
        var delta = timestamp - Now;
        if (delta <= 0)
            return TimeSpan.Zero;
        var frequency = Time.TimestampFrequency;
        return TimeSpan.FromTicks((long)(((Int128)delta * TimeSpan.TicksPerSecond + frequency - 1) / frequency));
    }

    /// <summary>Time elapsed since <paramref name="since"/>, at least zero, rounded down.</summary>
    public TimeSpan Elapsed(long since)
    {
        var delta = Now - since;
        return delta <= 0
            ? TimeSpan.Zero
            : TimeSpan.FromTicks((long)((Int128)delta * TimeSpan.TicksPerSecond / Time.TimestampFrequency));
    }
}
