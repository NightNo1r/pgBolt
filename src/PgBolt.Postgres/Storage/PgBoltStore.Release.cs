using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string ReleaseTemplate = """
        WITH l AS MATERIALIZED (
            SELECT job_id, cancel_requested FROM {schema}.queue
            WHERE job_id = @id AND lease_id = @lease_id
            FOR UPDATE
        ),
        r AS (
            UPDATE {schema}.queue q
            SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL,
                attempt = q.attempt - 1, ready = true
            FROM l WHERE q.job_id = l.job_id AND NOT l.cancel_requested
            RETURNING q.job_id
        ),
        c AS (
            DELETE FROM {schema}.queue q USING l
            WHERE q.job_id = l.job_id AND l.cancel_requested
            RETURNING q.job_id, q.attempt - CASE WHEN @ran THEN 0 ELSE 1 END AS attempt, q.leased_at, q.notify
        ),
        cj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = c.attempt,
                                       started_at = CASE WHEN @ran THEN c.leased_at ELSE j.started_at END,
                                       finished_at = now(), server_id = @server, notify = j.notify OR c.notify
            FROM c WHERE j.id = c.job_id AND j.id = @id
            RETURNING j.id, j.batch_id, c.notify
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, 4, batch_id FROM cj WHERE notify
            RETURNING 1
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message)
            SELECT job_id, now(), @kind_cancelled, @server, attempt, @message FROM c
        )
        SELECT (SELECT count(*) FROM r) AS released, (SELECT count(*) FROM c) AS cancelled, (SELECT count(*) FROM s) AS signals;
        """;

    private string ReleaseSql => field ??= Render(ReleaseTemplate);

    /// <summary>
    /// Returns a job to the queue without consuming an attempt (attempt − 1, ready = true). If cancellation was requested, finalizes as Cancelled
    /// with the last attempt that actually started (Part 3 §7.1). Fences updates by lease_id.
    /// </summary>
    /// <param name="jobId">Job being released.</param>
    /// <param name="leaseId">Lease token for this execution.</param>
    /// <param name="serverId">Server that holds the lease.</param>
    /// <param name="ran">True if shutdown interrupted execution. False if the job never started.</param>
    /// <param name="message">Optional message for the cancellation event.</param>
    /// <param name="ct">Token for cancelling the operation.</param>
    public Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct) =>
        QueryAndSignalAsync<ReleaseOutcome>(ReleaseSql, ps =>
        {
            P.Add(ps, "id", jobId);
            P.Add(ps, "lease_id", leaseId);
            P.Add(ps, "server", serverId);
            P.Add(ps, "ran", ran);
            P.Add(ps, "kind_cancelled", (short)JobEventKind.Cancelled);
            P.AddNullable(ps, "message", message, NpgsqlDbType.Text);
        }, async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                throw new InvalidOperationException("Release statement returned no row.");
            var released = r.GetInt64(0);
            var cancelled = r.GetInt64(1);
            var outcome = cancelled > 0 ? ReleaseOutcome.Cancelled
                : released > 0 ? ReleaseOutcome.Released
                : ReleaseOutcome.LeaseLost;
            return (outcome, r.GetInt64(2));
        }, ct);
}
