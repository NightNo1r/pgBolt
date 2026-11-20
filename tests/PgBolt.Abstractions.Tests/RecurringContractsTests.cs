using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;

namespace PgBolt.Abstractions.Tests;

public sealed class RecurringContractsTests
{
    [Fact]
    public void Options_have_spec_defaults()
    {
        var o = new RecurringJobOptions();
        Assert.Null(o.TimeZone);
        Assert.Equal(MisfirePolicy.FireOnce, o.Misfire);
        Assert.Equal(TimeSpan.FromDays(1), o.CatchUpWindow);
        Assert.Equal(OverlapPolicy.Skip, o.Overlap);
        Assert.Null(o.Queue);
        Assert.Null(o.Priority);
        Assert.Null(o.MaxAttempts);
        Assert.Null(o.RetryDelays);
    }

    [Fact]
    public void Policy_codes_match_stored_values()
    {
        Assert.Equal(0, (int)MisfirePolicy.FireOnce);
        Assert.Equal(1, (int)MisfirePolicy.Skip);
        Assert.Equal(2, (int)MisfirePolicy.CatchUp);
        Assert.Equal(0, (int)OverlapPolicy.Skip);
        Assert.Equal(1, (int)OverlapPolicy.Allow);
    }

    [Fact]
    public void Job_context_carries_schedule_fields()
    {
        var at = new DateTimeOffset(2026, 9, 30, 3, 0, 0, TimeSpan.Zero);
        var plain = new JobContext(JobId.New(), "n", 1, 3, null, null, NullLogger.Instance);
        var recurring = new JobContext(JobId.New(), "n", 1, 3, null, null, NullLogger.Instance, at, "daily");

        Assert.Null(plain.ScheduledAt);
        Assert.Null(plain.RecurringJobId);
        Assert.Equal(at, recurring.ScheduledAt);
        Assert.Equal("daily", recurring.RecurringJobId);
    }

    [Fact]
    public void Job_info_schedule_fields_default_to_null()
    {
        using var args = JsonDocument.Parse("{}");
        var info = new JobInfo(JobId.New(), "n", "q", 0, JobState.Enqueued, 0, 3, DateTimeOffset.UtcNow, null, null, null,
            null, null, false, args.RootElement.Clone(), []);

        Assert.Null(info.RecurringJobId);
        Assert.Null(info.ScheduledAt);
    }
}
