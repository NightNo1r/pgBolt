namespace PgBolt.LoadTest.Measure;

/// <summary>
/// Classifies <c>pg_stat_statements</c> rows using SQL fragments from <c>PgBoltStore</c>. Fragments contain only identifiers and keywords
/// because <c>pg_stat_statements</c> replaces parameters and constants with <c>$n</c>.
/// </summary>
internal static class StatementClasses
{
    public const string Dequeue = "dequeue";
    public const string DequeueLimited = "dequeue_limited";
    public const string ReadJobs = "read_jobs";
    public const string QueueLock = "queue_lock";
    public const string Ack = "ack";
    public const string Outcome = "outcome";
    public const string Heartbeat = "heartbeat";
    public const string Enqueue = "enqueue";
    public const string Promoter = "promoter";

    /// <summary>Statement classes executed by the load profile in every measurement window.</summary>
    public static readonly IReadOnlyList<string> Required = [Dequeue, Ack, Outcome, Heartbeat, Enqueue, Promoter];

    private static readonly (string Class, string Fragment)[] Fragments =
    [
        (Dequeue, "FROM c WHERE q.job_id = c.job_id"),
        (DequeueLimited, "AS room"),
        (ReadJobs, ".jobs AS leased_jobs WHERE id = ANY("),
        (QueueLock, "pg_advisory_xact_lock(hashtextextended("),
        (Ack, "AS acked"),
        (Outcome, "AS signals FROM ev"),                      // повтор и окончательный отказ
        (Heartbeat, "AS server_present"),
        (Enqueue, "AS existed"),                              // все варианты вставки задач
        (Promoter, "FROM p WHERE q.job_id = p.job_id"),
    ];

    /// <summary>Returns the query class, or <c>null</c> when the query is outside PgBolt or matches multiple classes.</summary>
    public static string? Classify(string query)
    {
        var matches = Fragments.Where(f => query.Contains(f.Fragment, StringComparison.Ordinal)).Take(2).ToList();
        return matches.Count == 1 ? matches[0].Class : null;
    }
}
