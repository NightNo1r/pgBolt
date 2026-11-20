namespace PgBolt.Dashboard.Tests.Support;

/// <summary>GetUtcNow — системное; GetTimestamp — системное плюс (число вызовов × Step): бюджет обхода в интеграционных тестах.</summary>
public sealed class StepTimeProvider : TimeProvider
{
    private long _calls;

    public TimeSpan Step { get; set; }

    public override long GetTimestamp()
    {
        var calls = Interlocked.Increment(ref _calls);
        return System.GetTimestamp() + calls * (long)(Step.TotalSeconds * TimestampFrequency);
    }
}
