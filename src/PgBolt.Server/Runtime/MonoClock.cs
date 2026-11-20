namespace PgBolt.Server.Runtime;

/// <summary>
/// Монотонное время runtime поверх <see cref="TimeProvider.GetTimestamp"/>: моменты — в единицах
/// <see cref="TimeProvider.TimestampFrequency"/>, интервалы — <see cref="TimeSpan"/>. Wall clock не используется.
/// </summary>
internal sealed class MonoClock(TimeProvider time)
{
    public TimeProvider Time { get; } = time ?? throw new ArgumentNullException(nameof(time));

    public long Now => Time.GetTimestamp();

    /// <summary><paramref name="timestamp"/> + <paramref name="duration"/> (duration может быть отрицательной).</summary>
    public long Add(long timestamp, TimeSpan duration) =>
        timestamp + (long)((Int128)duration.Ticks * Time.TimestampFrequency / TimeSpan.TicksPerSecond);

    /// <summary>Сколько осталось до <paramref name="timestamp"/>; ≥ 0, округление вверх — ожидание не заканчивается раньше момента.</summary>
    public TimeSpan Until(long timestamp)
    {
        var delta = timestamp - Now;
        if (delta <= 0)
            return TimeSpan.Zero;
        var frequency = Time.TimestampFrequency;
        return TimeSpan.FromTicks((long)(((Int128)delta * TimeSpan.TicksPerSecond + frequency - 1) / frequency));
    }

    /// <summary>Сколько прошло с <paramref name="since"/>; ≥ 0, округление вниз.</summary>
    public TimeSpan Elapsed(long since)
    {
        var delta = Now - since;
        return delta <= 0
            ? TimeSpan.Zero
            : TimeSpan.FromTicks((long)((Int128)delta * TimeSpan.TicksPerSecond / Time.TimestampFrequency));
    }
}
