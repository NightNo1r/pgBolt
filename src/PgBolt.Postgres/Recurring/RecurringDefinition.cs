namespace PgBolt.Postgres.Recurring;

/// <summary>Normalized schedule definition stored in recurring_jobs, excluding the cursor (Part 4 §3, §4.2).</summary>
internal sealed record RecurringDefinition(
    string Id,
    string Type,
    string ArgsJson,
    string Cron,
    string TimeZone,
    MisfirePolicy Misfire,
    TimeSpan CatchUpWindow,
    OverlapPolicy Overlap,
    string? Queue,
    short? Priority,
    int? MaxAttempts,
    int[]? RetryDelaysSeconds)
{
    public const int MaxIdLength = 200;
    private static readonly TimeSpan MaxCatchUpWindow = TimeSpan.FromDays(366);
    private static readonly RecurringJobOptions Defaults = new();

    /// <summary>Validates everything except the job type and arguments (§4.2). Code-defined schedules validate these on first
    /// synchronization.</summary>
    public static void Validate(string id, string cron, RecurringJobOptions? options) => _ = Normalize(id, cron, options);

    public static RecurringDefinition Create(string id, string type, string argsJson, string cron, RecurringJobOptions? options)
    {
        var n = Normalize(id, cron, options);
        ArgumentException.ThrowIfNullOrEmpty(type);
        ArgumentNullException.ThrowIfNull(argsJson);
        return new RecurringDefinition(id, type, argsJson, cron, n.TimeZone, n.Misfire, n.CatchUpWindow, n.Overlap,
            n.Queue, n.Priority, n.MaxAttempts, n.RetryDelaysSeconds);
    }

    public CronSchedule Schedule() => CronSchedule.Parse(Cron, TimeZone);

    private static Normalized Normalize(string id, string cron, RecurringJobOptions? options)
    {
        options ??= Defaults;
        if (string.IsNullOrEmpty(id) || id.Length > MaxIdLength)
            throw new ArgumentException($"Recurring job id must be 1..{MaxIdLength} characters.", nameof(id));
        if (string.IsNullOrWhiteSpace(cron))
            throw new ArgumentException("Cron expression must not be empty.", nameof(cron));

        var timeZone = options.TimeZone is null ? "UTC" : CronSchedule.IanaId(options.TimeZone);
        CronSchedule schedule;
        try
        {
            schedule = CronSchedule.Parse(cron, timeZone);
        }
        catch (Exception e) when (e is FormatException or TimeZoneNotFoundException or InvalidTimeZoneException)
        {
            throw new ArgumentException($"Invalid cron '{cron}' in time zone '{timeZone}': {e.Message}", nameof(cron), e);
        }
        if (schedule.After(DateTime.UtcNow) is null)
            throw new ArgumentException($"Cron '{cron}' has no future occurrences.", nameof(cron));

        var window = TimeSpan.FromTicks(options.CatchUpWindow.Ticks / TimeSpan.TicksPerMicrosecond * TimeSpan.TicksPerMicrosecond);
        if (window <= TimeSpan.Zero || window > MaxCatchUpWindow)
            throw new ArgumentException("CatchUpWindow must be in (0, 366 days].", nameof(options));
        if (!Enum.IsDefined(options.Misfire))
            throw new ArgumentException($"Unknown misfire policy {options.Misfire}.", nameof(options));
        if (!Enum.IsDefined(options.Overlap))
            throw new ArgumentException($"Unknown overlap policy {options.Overlap}.", nameof(options));
        if (options.Queue is { Length: 0 })
            throw new ArgumentException("Queue must not be empty.", nameof(options));
        if (options.MaxAttempts < 1)
            throw new ArgumentException("MaxAttempts must be >= 1.", nameof(options));

        var retry = options.RetryDelays?.Select(d => d < TimeSpan.Zero
            ? throw new ArgumentException("Retry delays must be non-negative.", nameof(options))
            : checked((int)Math.Ceiling(d.TotalSeconds))).ToArray();
        return new Normalized(timeZone, options.Misfire, window, options.Overlap, options.Queue, options.Priority, options.MaxAttempts, retry);
    }

    private readonly record struct Normalized(string TimeZone, MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap,
        string? Queue, short? Priority, int? MaxAttempts, int[]? RetryDelaysSeconds);

    /// <summary>Compares all fields by value, including RetryDelaysSeconds, because record array members otherwise compare by reference.</summary>
    public bool Equals(RecurringDefinition? other) =>
        other is not null && Id == other.Id && Type == other.Type && ArgsJson == other.ArgsJson && Cron == other.Cron
        && TimeZone == other.TimeZone && Misfire == other.Misfire && CatchUpWindow == other.CatchUpWindow && Overlap == other.Overlap
        && Queue == other.Queue && Priority == other.Priority && MaxAttempts == other.MaxAttempts
        && SameDelays(RetryDelaysSeconds, other.RetryDelaysSeconds);

    public override int GetHashCode() => HashCode.Combine(Id, Type, Cron, TimeZone);

    internal static bool SameDelays(int[]? a, int[]? b) =>
        (a is null) == (b is null) && (a ?? []).AsSpan().SequenceEqual(b ?? []);
}
