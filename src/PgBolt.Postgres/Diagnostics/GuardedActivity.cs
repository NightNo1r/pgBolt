using System.Diagnostics;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>Stops an <see cref="Activity"/> safely when an <see cref="ActivitySource"/> listener throws from <c>ActivityStopped</c>.</summary>
internal static class GuardedActivity
{
    /// <summary>
    /// Stops the span and resets <see cref="Activity.Current"/> to <paramref name="restore"/> before reporting a stop failure, so the logger and its
    /// listeners see the outer span.
    /// </summary>
    public static void Stop(Activity span, Activity? restore, PgBoltInstrumentation instrumentation)
    {
        Exception? failure = null;
        try { span.Stop(); }
        catch (Exception e) { failure = e; }
        Activity.Current = restore;
        if (failure is not null)
            instrumentation.ReportFailure(failure);
    }
}
