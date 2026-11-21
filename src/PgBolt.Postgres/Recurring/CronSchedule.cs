using Cronos;

namespace PgBolt.Postgres.Recurring;

/// <summary>
/// Cron expression in a time zone (Part 4 §2, §5.4). The API, synchronization, and scheduler tick infer the same format: Standard for @ macros
/// or five fields, IncludeSeconds for six fields. Macros parse identically in either format.
/// </summary>
internal sealed class CronSchedule
{
    private readonly CronExpression _expression;

    private CronSchedule(CronExpression expression, TimeZoneInfo zone)
    {
        _expression = expression;
        Zone = zone;
    }

    public TimeZoneInfo Zone { get; }

    /// <summary>
    /// FormatException (including CronFormatException) indicates invalid cron syntax. TimeZoneNotFoundException means the zone is unavailable on
    /// this pod.
    /// </summary>
    public static CronSchedule Parse(string cron, string timeZoneId)
    {
        ArgumentNullException.ThrowIfNull(cron);
        return new CronSchedule(CronExpression.Parse(cron.Trim(), FormatOf(cron)), TimeZoneInfo.FindSystemTimeZoneById(timeZoneId));
    }

    public static CronFormat FormatOf(string cron)
    {
        var trimmed = cron.Trim();
        if (trimmed.StartsWith('@'))
            return CronFormat.Standard;
        return trimmed.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length switch
        {
            5 => CronFormat.Standard,
            6 => CronFormat.IncludeSeconds,
            var n => throw new FormatException($"Cron expression '{cron}' has {n} fields; expected 5, 6 or a @macro."),
        };
    }

    /// <summary>IANA time zone ID used in storage (§4.2).</summary>
    public static string IanaId(TimeZoneInfo zone)
    {
        if (zone.HasIanaId)
            return zone.Id;
        return TimeZoneInfo.TryConvertWindowsIdToIanaId(zone.Id, out var iana)
            ? iana
            : throw new ArgumentException($"Time zone '{zone.Id}' has no IANA id.");
    }

    /// <summary>
    /// Returns the first occurrence strictly after t, or null if none remain (§5.3). The result is &gt; t and nondecreasing in t (see
    /// <see cref="Repair"/>). If Cronos cannot produce a valid answer, InvalidOperationException becomes a scheduler tick row error (§5.2).
    /// </summary>
    public DateTime? After(DateTime utc)
    {
        utc = AsUtc(utc);
        var next = _expression.GetNextOccurrence(utc, Zone, inclusive: false);
        return next is { } n && n <= utc ? Repair(utc) : next;
    }

    /// <summary>
    /// Enumerates occurrences in [fromUtc, toUtc], including both bounds (§5.4). Uses successive <see cref="After"/> calls because Cronos
    /// GetOccurrences can repeat an occurrence indefinitely on a faulty transition. Occurrences fall on whole seconds, so the first occurrence ≥
    /// from is after(from − 1 tick).
    /// </summary>
    public IEnumerable<DateTime> Between(DateTime fromUtc, DateTime toUtc)
    {
        var to = AsUtc(toUtc);
        for (var next = After(AsUtc(fromUtc).AddTicks(-1)); next is { } at && at <= to; next = After(at))
            yield return at;
    }

    /// <summary>
    /// Cronos 0.13 can return after(t) ≤ t in the hour before a change to the standard offset, e.g. Europe/Moscow on 2011-03-27 from 02:00 MSK+3 to
    /// 03:00 MSK+4. For <c>0 * * * *</c>, it returns 22:00Z throughout [22:00Z, 23:00Z), which could enqueue one occurrence about 3600 times.
    /// Occurrences fall on whole seconds, so the first occurrence &gt; t is the first ≥ the next whole second s. A probe is valid if Cronos returns
    /// null or an occurrence at or after the probe. Starting at s, double the step until a valid probe is found, then binary-search for the first
    /// valid probe after the last invalid one. Return its occurrence. Only faulty ranges require these extra calls, at most about 2 ×
    /// <see cref="MaxRepairDoublings"/>.
    /// </summary>
    private DateTime? Repair(DateTime utc)
    {
        var start = new DateTime(utc.Ticks - utc.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc).AddSeconds(1);
        if (FirstAtOrAfter(start) is var first && Sane(start, first))
            return first;

        var lo = start;                                                    // неисправна
        for (var doubling = 0; doubling < MaxRepairDoublings; doubling++)
        {
            var hi = start.AddSeconds(2L << doubling);
            if (!Sane(hi, FirstAtOrAfter(hi)))
            {
                lo = hi;
                continue;
            }
            while (hi - lo > OneSecond)                                     // lo неисправна, hi исправна, обе — целые секунды
            {
                var mid = lo.AddSeconds((long)(hi - lo).TotalSeconds / 2);
                if (Sane(mid, FirstAtOrAfter(mid)))
                    hi = mid;
                else
                    lo = mid;
            }
            return FirstAtOrAfter(hi);
        }
        throw new InvalidOperationException(
            $"Cronos returned a non-increasing occurrence for cron '{_expression}' in time zone '{Zone.Id}' after {utc:O}.");
    }

    private const int MaxRepairDoublings = 20;                             // 2^20 с ≈ 12 сут от t
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    private DateTime? FirstAtOrAfter(DateTime utc) => _expression.GetNextOccurrence(utc, Zone, inclusive: true);

    private static bool Sane(DateTime from, DateTime? occurrence) => occurrence is not { } o || o >= from;

    /// <summary>Npgsql returns timestamptz with Kind = Utc, which Cronos requires explicitly.</summary>
    private static DateTime AsUtc(DateTime value) =>
        value.Kind == DateTimeKind.Utc ? value : DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
