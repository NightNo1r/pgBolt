namespace PgBolt.Postgres.Tests;

public sealed class UuidV7Tests
{
    private static byte[] Be(Guid g) => g.ToByteArray(bigEndian: true);

    [Fact]
    public void GetTimestamp_roundtrips_CreateVersion7()
    {
        var ts = new DateTimeOffset(2026, 9, 27, 13, 45, 12, 345, TimeSpan.Zero);
        Assert.Equal(ts, UuidV7.GetTimestamp(Guid.CreateVersion7(ts)));
    }

    [Fact]
    public void LowerBound_brackets_ids_of_the_day_in_postgres_byte_order()
    {
        var day = new DateOnly(2026, 9, 27);
        var lo = Be(UuidV7.LowerBound(day));
        var hi = Be(UuidV7.LowerBound(day.AddDays(1)));
        foreach (var hour in new[] { 0, 12, 23 })
        {
            var id = Be(Guid.CreateVersion7(new DateTimeOffset(2026, 9, 27, hour, 59, 59, 999, TimeSpan.Zero)));
            Assert.True(lo.AsSpan().SequenceCompareTo(id) <= 0);
            Assert.True(id.AsSpan().SequenceCompareTo(hi) < 0);
        }
    }

    [Fact]
    public void LowerBound_is_timestamp_prefix_with_zero_tail()
    {
        var g = UuidV7.LowerBound(new DateOnly(1970, 1, 2));
        Assert.Equal("00000526-5c00-0000-0000-000000000000", g.ToString());
    }

    [Fact]
    public void Lower_bound_of_timestamp_precedes_ids_of_that_millisecond()
    {
        var t = new DateTimeOffset(2026, 9, 27, 13, 45, 12, 345, TimeSpan.Zero).AddTicks(6789);
        var lower = UuidV7.LowerBound(t);

        Assert.True(Be(lower).AsSpan().SequenceCompareTo(Be(Guid.CreateVersion7(t))) <= 0);
        Assert.Equal(t.ToUnixTimeMilliseconds(), UuidV7.GetTimestamp(lower).ToUnixTimeMilliseconds());
    }

    [Fact]
    public void EventsLowerBound_is_start_of_previous_utc_day()
    {
        var id = Guid.CreateVersion7(new DateTimeOffset(2026, 9, 27, 0, 30, 0, TimeSpan.Zero));
        Assert.Equal(new DateTimeOffset(2026, 9, 26, 0, 0, 0, TimeSpan.Zero), UuidV7.EventsLowerBound(id));
    }
}
