using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Tests.Load;

public sealed class ProfileBTests
{
    [Fact]
    public void LimitedPreservesJobMix()
    {
        var standard = new ProfileB(new Random(42));
        var limited = new ProfileB(new Random(42), QueueProfile.Limited);
        for (var i = 0; i < 1000; i++)
        {
            Assert.Equal(standard.Next(i) with { Queue = "default" }, limited.Next(i));
        }
    }

    [Fact]
    public void Distribution_matches_profile()
    {
        const int count = 1_000_000;
        var profile = new ProfileB(new Random(42));
        long critical = 0, delayed = 0, failing = 0;

        for (long seq = 0; seq < count; seq++)
        {
            var job = profile.Next(seq);
            Assert.Equal(seq, job.Seq);
            if (job.Queue == "critical") critical++;
            else Assert.Equal("default", job.Queue);
            if (job.Delayed) delayed++;
            if (job.FailFirstAttempt) failing++;
        }

        Assert.InRange(critical / (double)count, 0.197, 0.203);
        Assert.InRange(delayed / (double)count, 0.097, 0.103);
        Assert.InRange(failing / (double)count, 0.009, 0.011);
    }

    [Fact]
    public void PickDelaySeconds_covers_1_to_30_uniformly()
    {
        const int count = 300_000;
        var rng = new Random(42);
        var hits = new int[31];

        for (var i = 0; i < count; i++) hits[ProfileB.PickDelaySeconds(rng)]++;

        Assert.Equal(0, hits[0]);
        for (var delay = 1; delay <= 30; delay++)
            Assert.InRange(hits[delay] / (double)count, 1.0 / 30 - 0.005, 1.0 / 30 + 0.005);
    }
}
