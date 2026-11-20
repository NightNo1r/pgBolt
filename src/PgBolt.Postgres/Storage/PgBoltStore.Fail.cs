using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string RetryTemplate = """
        WITH l AS MATERIALIZED (
            SELECT job_id, cancel_requested FROM {schema}.queue
            WHERE job_id = @id AND lease_id = @lease_id
            FOR UPDATE
        ),
        r AS (
            UPDATE {schema}.queue q
            SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL,
                run_at = now() + @delay, ready = (@delay <= interval '0')
            FROM l WHERE q.job_id = l.job_id AND NOT l.cancel_requested
            RETURNING q.job_id, q.attempt
        ),
        c AS (
            DELETE FROM {schema}.queue q USING l
            WHERE q.job_id = l.job_id AND l.cancel_requested
            RETURNING q.job_id, q.attempt, q.leased_at, q.notify
        ),
        cj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = c.attempt, started_at = c.leased_at,
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
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message, exception)
            SELECT job_id, now(), @kind, @server, attempt, @message, @exception FROM r
            UNION ALL
            SELECT job_id, now(), @kind_cancelled, @server, attempt, @message, @exception FROM c
            RETURNING kind
        )
        SELECT kind, (SELECT count(*) FROM s) AS signals FROM ev
        """;

    private const string FinalFailTemplate = """
        WITH d AS (
            DELETE FROM {schema}.queue WHERE job_id = @id AND lease_id = @lease_id
            RETURNING job_id, attempt, leased_at, cancel_requested, notify
        ),
        j AS (
            UPDATE {schema}.jobs
            SET state = CASE WHEN d.cancel_requested THEN 4 ELSE @state END,
                attempt = d.attempt, started_at = d.leased_at, finished_at = now(), server_id = @server,
                notify = jobs.notify OR d.notify
            FROM d WHERE jobs.id = d.job_id AND jobs.id = @id
            RETURNING jobs.id, jobs.batch_id, jobs.state, d.notify
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, state, batch_id FROM j WHERE notify
            RETURNING 1
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message, exception)
            SELECT job_id, now(), CASE WHEN cancel_requested THEN @kind_cancelled ELSE @kind END,
                   @server, attempt, @message, @exception
            FROM d
            RETURNING kind
        )
        SELECT kind, (SELECT count(*) FROM s) AS signals FROM ev
        """;

    internal string RetrySql => field ??= Render(RetryTemplate);
    internal string FinalFailSql => field ??= Render(FinalFailTemplate);

    public Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind,
        string? message, string? exception, CancellationToken ct)
    {
        if (kind is not (JobEventKind.Retry or JobEventKind.NoExecutor))
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "Retry accepts Retry or NoExecutor.");
        return FailStatementAsync(RetrySql, jobId, leaseId, serverId, kind, message, exception, ps =>
        {
            P.Add(ps, "delay", delay);
        }, FailOutcome.Retried, ct);
    }

    public Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind,
        string? message, string? exception, CancellationToken ct)
    {
        if (kind is not (JobEventKind.Failed or JobEventKind.NoExecutor))
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "Fail accepts Failed or NoExecutor.");
        return FailStatementAsync(FinalFailSql, jobId, leaseId, serverId, kind, message, exception,
            ps => P.Add(ps, "state", (short)3), FailOutcome.Failed, ct);
    }

    public Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct) =>
        FailStatementAsync(FinalFailSql, jobId, leaseId, serverId, JobEventKind.Cancelled, message, null,
            ps => P.Add(ps, "state", (short)4), FailOutcome.Cancelled, ct);

    private Task<FailOutcome> FailStatementAsync(string sql, Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind,
        string? message, string? exception, Action<Npgsql.NpgsqlParameterCollection> bindExtra, FailOutcome normalOutcome,
        CancellationToken ct) =>
        QueryAndSignalAsync<FailOutcome>(sql, ps =>
        {
            P.Add(ps, "id", jobId);
            P.Add(ps, "lease_id", leaseId);
            P.Add(ps, "server", serverId);
            P.Add(ps, "kind", (short)kind);
            P.Add(ps, "kind_cancelled", (short)JobEventKind.Cancelled);
            P.AddNullable(ps, "message", message, NpgsqlDbType.Text);
            P.AddNullable(ps, "exception", exception, NpgsqlDbType.Text);
            bindExtra(ps);
        }, async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return (FailOutcome.LeaseLost, 0L);
            var outcome = r.GetInt16(0) == (short)JobEventKind.Cancelled && kind != JobEventKind.Cancelled
                ? FailOutcome.Cancelled
                : normalOutcome;
            return (outcome, r.GetInt64(1));
        }, ct);
}
