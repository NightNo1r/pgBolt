namespace PgBolt.Postgres.Schema;

/// <param name="Created">Number of jobs and job_events partitions created.</param>
/// <param name="TimedOut">The lock was unavailable within lock_timeout. Retry later (§4.4).</param>
/// <param name="TodayPresent">Both jobs and job_events partitions exist for the database server's current UTC day.</param>
public sealed record PartitionEnsureResult(int Created, bool TimedOut, bool TodayPresent);
