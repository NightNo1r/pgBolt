namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string RequeueTemplate = """
        WITH RECURSIVE s AS MATERIALIZED (
            SELECT id, state, queue, priority, max_attempts, unique_key, notify, batch_id
            FROM {schema}.jobs WHERE id = @id
            FOR UPDATE
        ),
        q AS (
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key, notify, batch_id)
            SELECT id, queue, priority, now(), true, max_attempts, unique_key, notify OR batch_id IS NOT NULL, batch_id
            FROM s WHERE state IN (3, 4)
            ON CONFLICT DO NOTHING
            RETURNING job_id
        ),
        j AS (
            UPDATE {schema}.jobs SET state = 0, finished_at = NULL
            WHERE id = @id AND id IN (SELECT job_id FROM q)
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind) SELECT job_id, now(), @kind_requeued FROM q
        ),
        chain(id, depth) AS (
            SELECT s.batch_id, 0 FROM s WHERE s.batch_id IS NOT NULL AND EXISTS (SELECT 1 FROM q)
            UNION ALL
            SELECT b.parent_batch_id, chain.depth + 1
            FROM chain JOIN {schema}.batches b ON b.id = chain.id
            WHERE b.parent_batch_id IS NOT NULL
        ),
        lk AS MATERIALIZED (
            SELECT b.id, b.state, chain.depth
            FROM {schema}.batches b JOIN chain ON chain.id = b.id
            ORDER BY b.id
            FOR NO KEY UPDATE OF b
        ),
        lv AS (
            SELECT id, depth,
                   lag(state) OVER w AS below_state,
                   bool_and(state IN (2, 3, 4)) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS below_terminal,
                   row_number() OVER w - 1 = depth AS contiguous
            FROM lk
            WINDOW w AS (ORDER BY depth)
        ),
        u AS (
            UPDATE {schema}.batches b
            SET failed = b.failed - (k.k = 3)::int, cancelled = b.cancelled - (k.k = 4)::int,
                state = CASE WHEN b.state IN (2, 3, 4) THEN 0 ELSE b.state END,
                cancel_requested = b.cancel_requested AND b.state NOT IN (2, 3, 4),
                finished_at = NULL
            FROM (SELECT lv.id, CASE WHEN lv.depth = 0 THEN (SELECT state FROM s) ELSE lv.below_state END AS k
                  FROM lv
                  WHERE lv.contiguous AND (lv.depth = 0 OR lv.below_terminal)) k
            WHERE b.id = k.id
        )
        SELECT (SELECT state FROM s) AS state, EXISTS (SELECT 1 FROM q) AS requeued, (SELECT queue FROM s) AS queue
        """;

    private string RequeueSql => field ??= Render(RequeueTemplate);

    /// <summary>
    /// Failed/Cancelled → Pending (core §5.9). Часть 5 §6.6: queue.notify = jobs.notify OR член batch, queue.batch_id = jobs.batch_id;
    /// у члена batch в той же транзакции счётчики batch и терминальных внешних уменьшаются синхронно (цепочка блокируется
    /// ORDER BY id FOR NO KEY UPDATE, подъём снизу вверх до первого нетерминального или отсутствующего). 40P01/40001 — повтор целиком.
    /// </summary>
    public Task<(RequeueResult Result, string? Queue)> RequeueAsync(Guid jobId, CancellationToken ct) =>
        QueryAsync(RequeueSql, ps =>
        {
            P.Add(ps, "id", jobId);
            P.Add(ps, "kind_requeued", (short)JobEventKind.Requeued);
        }, async (r, c) =>
        {
            await r.ReadAsync(c);
            var state = r.GetNullableValue<short>(0);
            var requeued = r.GetBoolean(1);
            var queue = r.GetNullable<string>(2);
            var result = requeued ? RequeueResult.Requeued
                : state is null ? RequeueResult.NotFound
                : state is not (3 or 4) ? RequeueResult.NotTerminal
                : RequeueResult.Conflict;
            return (result, queue);
        }, ct);
}
