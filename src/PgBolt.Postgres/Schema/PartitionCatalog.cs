using System.Globalization;
using System.Text.RegularExpressions;
using Npgsql;

namespace PgBolt.Postgres.Schema;

internal enum CatalogKind { JobsPartition, EventsPartition, DetachedJobs, DetachedEvents, CompactionRemnant }

/// <param name="Oid">Table OID, used to identify the table throughout the run (§5.2).</param>
/// <param name="Name">Table name in the PgBolt schema.</param>
/// <param name="Kind">Partition or remnant classification.</param>
/// <param name="Day">UTC date encoded in the table name.</param>
internal sealed record CatalogTable(long Oid, string Name, CatalogKind Kind, DateOnly Day)
{
    public bool IsJobs => Kind is CatalogKind.JobsPartition or CatalogKind.DetachedJobs or CatalogKind.CompactionRemnant;

    public string Table => IsJobs ? "jobs" : "job_events";
}

/// <param name="Today">Current UTC date on the database server (§3).</param>
/// <param name="Tables">Tables in the schema with PgBolt partition names.</param>
internal sealed record PartitionListing(DateOnly Today, List<CatalogTable> Tables);

/// <summary>Tables created by PgBolt and UTC days used by retention (Part 2 §5.2, §3).</summary>
internal static partial class PartitionCatalog
{
    [GeneratedRegex("^jobs_([0-9]{8})(?:_r([0-9]{1,10}))?$", RegexOptions.CultureInvariant)]
    private static partial Regex JobsName();

    [GeneratedRegex("^job_events_([0-9]{8})$", RegexOptions.CultureInvariant)]
    private static partial Regex EventsName();

    /// <summary>
    /// Classifies partition names according to §5.2. Returns null for a different parent, an attached remnant, or an unrecognized name.
    /// </summary>
    public static CatalogTable? Classify(long oid, string name, string? parent)
    {
        var jobs = JobsName().Match(name);
        if (jobs.Success)
        {
            if (!TryDay(jobs.Groups[1].Value, out var day))
                return null;
            if (jobs.Groups[2].Success)
                return parent is null ? new CatalogTable(oid, name, CatalogKind.CompactionRemnant, day) : null;
            return parent switch
            {
                "jobs" => new CatalogTable(oid, name, CatalogKind.JobsPartition, day),
                null => new CatalogTable(oid, name, CatalogKind.DetachedJobs, day),
                _ => null,
            };
        }

        var events = EventsName().Match(name);
        if (!events.Success || !TryDay(events.Groups[1].Value, out var eventsDay))
            return null;
        return parent switch
        {
            "job_events" => new CatalogTable(oid, name, CatalogKind.EventsPartition, eventsDay),
            null => new CatalogTable(oid, name, CatalogKind.DetachedEvents, eventsDay),
            _ => null,
        };
    }

    /// <summary>Rd = ceil(retention / 1 day), with a minimum of 1.</summary>
    public static int RetentionDays(TimeSpan retention) =>
        checked((int)((retention.Ticks + TimeSpan.TicksPerDay - 1) / TimeSpan.TicksPerDay));

    /// <summary>The last eligible day is today − 1 − Rd (§5.2). Null disables retention.</summary>
    public static DateOnly? Horizon(DateOnly today, TimeSpan? retention) =>
        retention is { } r ? today.AddDays(-1 - RetentionDays(r)) : null;

    public static int Overdue(DateOnly horizon, DateOnly day) => horizon.DayNumber - day.DayNumber;

    /// <summary>
    /// Reads the database server's current UTC day (§3) and all tables with PgBolt partition names, attached or detached, in one query (§5.2).
    /// Identifies parents by OID. A regclass::text name can omit the schema when visible in search_path, allowing <c>other.jobs</c> to match 'jobs'.
    /// </summary>
    public static async Task<PartitionListing> ListAsync(NpgsqlDataSource dataSource, string schema, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        DateOnly today;
        await using (var cmd = new NpgsqlCommand("SELECT (now() AT TIME ZONE 'UTC')::date", conn))
            today = (DateOnly)(await cmd.ExecuteScalarAsync(ct))!;

        var tables = new List<CatalogTable>();
        await using (var cmd = new NpgsqlCommand($$"""
            SELECT c.oid::bigint, c.relname::text,
                   CASE i.inhparent WHEN '{{schema}}.jobs'::regclass       THEN 'jobs'
                                    WHEN '{{schema}}.job_events'::regclass THEN 'job_events'
                                    ELSE i.inhparent::oid::text END
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
            WHERE n.nspname = $1 AND c.relkind = 'r'
              AND c.relname ~ '^(jobs_[0-9]{8}(_r[0-9]+)?|job_events_[0-9]{8})$'
            """, conn))
        {
            cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schema });
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                if (Classify(reader.GetInt64(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)) is { } table)
                    tables.Add(table);
        }
        return new PartitionListing(today, tables);
    }

    private static bool TryDay(string text, out DateOnly day) =>
        DateOnly.TryParseExact(text, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out day);
}
