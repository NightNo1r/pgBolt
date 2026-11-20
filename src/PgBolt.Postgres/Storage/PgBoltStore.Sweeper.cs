namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string SweepTemplate = """
        WITH e AS (
            SELECT job_id, attempt, max_attempts, lease_owner, cancel_requested FROM {schema}.queue
            WHERE lease_owner IS NOT NULL AND lease_until < now()
            LIMIT @batch
            FOR UPDATE SKIP LOCKED
        ),
        reset AS (
            UPDATE {schema}.queue q SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL
            FROM e WHERE q.job_id = e.job_id AND e.attempt < e.max_attempts AND NOT e.cancel_requested
        ),
        fin AS (
            DELETE FROM {schema}.queue q USING e
            WHERE q.job_id = e.job_id AND (e.attempt >= e.max_attempts OR e.cancel_requested)
            RETURNING q.job_id, q.attempt, q.cancel_requested, q.notify
        ),
        f AS (
            UPDATE {schema}.jobs j
            SET state = CASE WHEN fin.cancel_requested THEN 4 ELSE 3 END, attempt = fin.attempt, finished_at = now(),
                notify = j.notify OR fin.notify
            FROM fin
            WHERE j.id = fin.job_id
            RETURNING j.id, j.batch_id, j.state, fin.notify
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, state, batch_id FROM f WHERE notify
            RETURNING 1
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt)
            SELECT job_id, now(), @kind_lease_expired, lease_owner, attempt FROM e
            UNION ALL
            SELECT job_id, now(), @kind_cancelled, lease_owner, attempt FROM e WHERE cancel_requested
            RETURNING kind
        )
        SELECT (SELECT count(*) FROM ev WHERE kind = @kind_lease_expired) AS expired, (SELECT count(*) FROM s) AS signals
        """;

    private string SweepSql => field ??= Render(SweepTemplate);

    /// <summary>Сбрасывает истёкшие lease (§5.6). Можно запускать на всех серверах одновременно.</summary>
    public Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        return QueryAndSignalAsync<int>(SweepSql, ps =>
        {
            P.Add(ps, "batch", batchSize);
            P.Add(ps, "kind_lease_expired", (short)JobEventKind.LeaseExpired);
            P.Add(ps, "kind_cancelled", (short)JobEventKind.Cancelled);
        }, async (r, c) =>
        {
            await r.ReadAsync(c);
            return ((int)r.GetInt64(0), r.GetInt64(1));
        }, ct);
    }
}
