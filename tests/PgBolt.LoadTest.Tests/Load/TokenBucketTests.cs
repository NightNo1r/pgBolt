using Microsoft.Extensions.Time.Testing;
using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Tests.Load;

public sealed class TokenBucketTests
{
    [Fact]
    public void Issues_rate_times_ten_seconds()
    {
        var time = new FakeTimeProvider();
        var bucket = new TokenBucket(10_000, 10_000, time);
        long issued = 0;

        for (var i = 0; i < 10_000; i++)
        {
            time.Advance(TimeSpan.FromMilliseconds(1));
            var count = bucket.Available();
            bucket.Consume(count);
            issued += count;
        }

        Assert.InRange(issued, 100_000 * 0.995, 100_000 * 1.005);
    }

    [Fact]
    public void Accumulates_up_to_ceiling_and_counts_dropped()
    {
        var time = new FakeTimeProvider();
        var bucket = new TokenBucket(1_000, 1_000, time);

        time.Advance(TimeSpan.FromSeconds(3));

        Assert.Equal(1_000, bucket.Available());
        Assert.InRange(bucket.Dropped, 2_000 - 1, 2_000 + 1);
    }

    [Fact]
    public void Rate_change_applies_from_change_moment_and_moves_ceiling()
    {
        var time = new FakeTimeProvider();
        var bucket = new TokenBucket(1_000, 1_000, time);

        time.Advance(TimeSpan.FromSeconds(0.5));
        bucket.SetRate(4_000);
        Assert.Equal(500, bucket.Available());

        time.Advance(TimeSpan.FromSeconds(0.5));
        Assert.Equal(2_500, bucket.Available());

        time.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(4_000, bucket.Available());
        Assert.InRange(bucket.Dropped, 6_500 - 1, 6_500 + 1);
    }
}
