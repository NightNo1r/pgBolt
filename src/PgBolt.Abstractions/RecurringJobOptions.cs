namespace PgBolt;

/// <summary>Handles occurrences missed during downtime or a pause (Part 4 §5.3). Values are stored in recurring_jobs.misfire.</summary>
public enum MisfirePolicy
{
    /// <summary>Fires once for all missed occurrences. ScheduledAt is the latest missed occurrence.</summary>
    FireOnce = 0,

    /// <summary>Fires only if the latest missed occurrence is no older than MisfireThreshold.</summary>
    Skip = 1,

    /// <summary>Fires for each missed occurrence within CatchUpWindow.</summary>
    CatchUp = 2,
}

/// <summary>Handles an occurrence while the previous run is still active (Part 4 §5.3). Values are stored in recurring_jobs.overlap.</summary>
public enum OverlapPolicy
{
    /// <summary>Skips enqueue while the previous run is active (key pgbolt:recurring:{id}). CatchUp waits for that run.</summary>
    Skip = 0,

    /// <summary>Enqueues every occurrence.</summary>
    Allow = 1,
}

public sealed record RecurringJobOptions
{
    /// <summary>Cron time zone, stored as an IANA ID. Null selects UTC.</summary>
    /// <remarks>
    /// <para>
    /// Daylight saving transitions follow Cronos semantics (Part 4 §5.4). A time in the spring gap fires at the transition: <c>30 2 * * *</c> fires
    /// at 03:00 EDT in America/New_York on 2026-03-08. In the fall, a fixed hour and minute fire once. An interval expression such as <c>*/15</c> in
    /// the hour or minute field fires in both passes through the repeated hour.
    /// </para>
    /// <para>
    /// Historical changes to a zone's base offset, such as Europe/Moscow in 2011 and 2014 or Pacific/Apia in 2011, behave differently. A repeated
    /// hour fires once, and a time in a gap can fire before the transition. Occurrences are never duplicated. If a Cronos error at such a transition
    /// cannot be corrected, the scheduler tick records it in last_error.
    /// </para>
    /// </remarks>
    public TimeZoneInfo? TimeZone { get; init; }

    public MisfirePolicy Misfire { get; init; } = MisfirePolicy.FireOnce;

    /// <summary>CatchUp runs missed occurrences within this window. The window must be positive and at most 366 days.</summary>
    public TimeSpan CatchUpWindow { get; init; } = TimeSpan.FromDays(1);

    public OverlapPolicy Overlap { get; init; } = OverlapPolicy.Skip;

    /// <summary>When null, uses the job_types default at the time of firing. The same applies to the following properties.</summary>
    public string? Queue { get; init; }

    public short? Priority { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
}
