using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string TryClaimMaintenanceTemplate = """
        INSERT INTO {schema}.maintenance_tasks AS t (name, next_run_at, last_run_at)
        VALUES (@name, now() + @interval, now())
        ON CONFLICT (name) DO UPDATE
            SET next_run_at = EXCLUDED.next_run_at, last_run_at = EXCLUDED.last_run_at
            WHERE t.next_run_at <= now()
        RETURNING t.last_run_at;
        """;

    private const string CompleteMaintenanceTemplate = """
        UPDATE {schema}.maintenance_tasks
        SET last_error = @error,
            last_finished_at = now(),
            next_run_at = CASE WHEN @retry_delay::interval IS NULL THEN next_run_at
                               ELSE LEAST(next_run_at, now() + @retry_delay::interval) END
        WHERE name = @name AND last_run_at = @claimed_at;
        """;

    private const string RemoveDeadServersTemplate = """
        DELETE FROM {schema}.servers WHERE heartbeat_at < now() - @older_than;
        """;

    private string TryClaimMaintenanceSql => field ??= Render(TryClaimMaintenanceTemplate);
    private string CompleteMaintenanceSql => field ??= Render(CompleteMaintenanceTemplate);
    private string RemoveDeadServersSql => field ??= Render(RemoveDeadServersTemplate);

    /// <summary>
    /// Claims a due periodic task by upserting its row (Part 2 §5.7). Returns the claim's UTC last_run_at as the ownership token for
    /// <see cref="CompleteMaintenanceAsync"/>, or null when another pod holds the task.
    /// </summary>
    public Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return QueryAsync<DateTime?>(TryClaimMaintenanceSql, ps =>
        {
            P.Add(ps, "name", name);
            P.Add(ps, "interval", interval);
        }, async (r, c) => await r.ReadAsync(c) ? r.GetDateTime(0) : null, ct);
    }

    /// <summary>
    /// Records the result if the claim still belongs to this pod (last_run_at = claimedAt, §5.7). A non-null retryDelay shortens next_run_at to
    /// now() + retryDelay. Returns false if another pod has reclaimed it.
    /// </summary>
    public async Task<bool> CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return await NonQueryAsync(CompleteMaintenanceSql, ps =>
        {
            P.Add(ps, "name", name);
            P.Add(ps, "claimed_at", claimedAt, NpgsqlDbType.TimestampTz);
            P.AddNullable(ps, "error", error, NpgsqlDbType.Text);
            ps.Add(new NpgsqlParameter("retry_delay", NpgsqlDbType.Interval) { Value = retryDelay is { } d ? d : DBNull.Value });
        }, ct) == 1;
    }

    /// <summary>Removes stale server rows only when heartbeat_at &lt; now() − olderThan (§5.6).</summary>
    public Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(olderThan, TimeSpan.Zero);
        return NonQueryAsync(RemoveDeadServersSql, ps => P.Add(ps, "older_than", olderThan), ct);
    }
}
