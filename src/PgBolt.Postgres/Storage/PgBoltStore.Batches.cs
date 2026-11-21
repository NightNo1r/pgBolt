using Npgsql;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Cancels a batch (Part 5 §6.5). CancelBatchAsync uses one statement. CancelMembers processes signals individually in short transactions with a
/// 1-second lock_timeout. Stage 0 cancels member jobs using jobs_batch and set-based core §5.7 semantics. Stage 1 processes nested batches. Each
/// portion rereads its node's cancel_requested flag. If requeue cleared it (§6.6), the portion changes nothing and the signal is removed.
/// </summary>
public sealed partial class PgBoltStore
{
    private const string CancelBatchTemplate = """
        WITH b AS MATERIALIZED (
            SELECT id, state, cancel_requested FROM {schema}.batches WHERE id = @id FOR NO KEY UPDATE
        ),
        u AS (
            UPDATE {schema}.batches t SET cancel_requested = true FROM b
            WHERE t.id = b.id AND b.state IN (0, 1) AND NOT b.cancel_requested
            RETURNING t.id
        ),
        s AS (INSERT INTO {schema}.completions (node_id, kind) SELECT id, 6 FROM u RETURNING 1)
        SELECT b.state, EXISTS (SELECT 1 FROM s) AS signalled FROM b
        """;

    private const string GetBatchTemplate = """
        SELECT id, parent_batch_id, parent_id, condition, state, total,
               greatest(0, succeeded), greatest(0, failed), greatest(0, cancelled), cancel_requested, created_at, finished_at
        FROM {schema}.batches WHERE id = @id
        """;

    private const string CancelCaptureTemplate = $$"""
        SELECT id, node_id, stage, cursor FROM {schema}.completions
        WHERE {{CancelReadyPredicate}}
        ORDER BY id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        """;

    /// <summary>
    /// Stage 0 (§6.5) finds members via jobs_batch without locking jobs, then locks their queue rows with ORDER BY job_id FOR UPDATE without SKIP
    /// LOCKED.
    /// </summary>
    private const string CancelJobsTemplate = """
        WITH b AS (
            SELECT cancel_requested FROM {schema}.batches WHERE id = @node
        ),
        m AS MATERIALIZED (
            SELECT id FROM {schema}.jobs
            WHERE batch_id = @node AND batch_id IS NOT NULL AND id > @cursor AND (SELECT cancel_requested FROM b)
            ORDER BY id
            LIMIT @lim
        ),
        l AS MATERIALIZED (
            SELECT q.job_id, q.lease_owner FROM {schema}.queue q
            WHERE q.job_id IN (SELECT id FROM m)
            ORDER BY q.job_id
            FOR UPDATE
        ),
        mark AS (
            UPDATE {schema}.queue q SET cancel_requested = true
            FROM l WHERE q.job_id = l.job_id AND l.lease_owner IS NOT NULL
        ),
        x AS (
            DELETE FROM {schema}.queue q USING l
            WHERE q.job_id = l.job_id AND l.lease_owner IS NULL
            RETURNING q.job_id, q.attempt, q.notify
        ),
        xj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = x.attempt, finished_at = now(), notify = j.notify OR x.notify
            FROM x WHERE j.id = x.job_id
            RETURNING j.id, j.batch_id, x.notify
        ),
        xe AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
            SELECT job_id, now(), @kind_cancelled, attempt, 'batch ' || @node || ' cancelled' FROM x
        ),
        xs AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, 4, batch_id FROM xj WHERE notify
        )
        SELECT COALESCE((SELECT cancel_requested FROM b), false) AS requested,
               (SELECT count(*) FROM m)::int AS taken,
               (SELECT id FROM m ORDER BY id DESC LIMIT 1) AS last_id
        """;

    /// <summary>
    /// Stage 1 (§6.5) visits nested batches (batches_nested) in ID order. Active or Awaiting batches without a flag get cancel_requested and their
    /// own CancelMembers signal. A nested batch must be open both in the statement snapshot (c.open, which also reads the node flag b) and in the
    /// latest version under FOR NO KEY UPDATE (lk). A batch reactivated by a requeue committed after the snapshot counts as already visited: it was
    /// terminal in the snapshot and fails c.open (§6.5, §6.6). Requeue clears the node flag only on a terminal node. The upward traversal stops at a
    /// nonterminal node with cancel_requested, preserving that flag (§6.6, step 2). The enclosing flag therefore still cancels waiting branch
    /// members (§6.4 T2.3). Closed batches in the snapshot are left unlocked. The cursor and portion size include all nested batches (c).
    /// </summary>
    private const string CancelNestedTemplate = """
        WITH b AS (
            SELECT cancel_requested FROM {schema}.batches WHERE id = @node
        ),
        c AS MATERIALIZED (
            SELECT id, state IN (0, 1) AND NOT cancel_requested AS open FROM {schema}.batches
            WHERE parent_batch_id = @node AND parent_batch_id IS NOT NULL AND id > @cursor AND (SELECT cancel_requested FROM b)
            ORDER BY id
            LIMIT @lim
        ),
        lk AS MATERIALIZED (
            SELECT t.id, t.state, t.cancel_requested FROM {schema}.batches t
            WHERE t.id IN (SELECT id FROM c WHERE open)
            ORDER BY t.id
            FOR NO KEY UPDATE
        ),
        u AS (
            UPDATE {schema}.batches t SET cancel_requested = true
            FROM lk WHERE t.id = lk.id AND lk.state IN (0, 1) AND NOT lk.cancel_requested
            RETURNING t.id
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind) SELECT id, 6 FROM u
        )
        SELECT COALESCE((SELECT cancel_requested FROM b), false) AS requested,
               (SELECT count(*) FROM c)::int AS taken,
               (SELECT id FROM c ORDER BY id DESC LIMIT 1) AS last_id
        """;

    private const string CancelAdvanceTemplate =
        "UPDATE {schema}.completions SET stage = @stage, cursor = @cursor, retry_after = NULL WHERE id = @id";

    private const string CancelDoneTemplate = "DELETE FROM {schema}.completions WHERE id = @id";

    private const string CancelDeferTemplate = "UPDATE {schema}.completions SET retry_after = now() + @delay WHERE id = @id";

    private string CancelBatchSql => field ??= Render(CancelBatchTemplate);
    private string GetBatchSql => field ??= Render(GetBatchTemplate);
    private string CancelCaptureSql => field ??= Render(CancelCaptureTemplate);
    private string CancelJobsSql => field ??= Render(CancelJobsTemplate);
    private string CancelNestedSql => field ??= Render(CancelNestedTemplate);
    private string CancelAdvanceSql => field ??= Render(CancelAdvanceTemplate);
    private string CancelDoneSql => field ??= Render(CancelDoneTemplate);
    private string CancelDeferSql => field ??= Render(CancelDeferTemplate);

    /// <summary>
    /// Returns NotFound for a missing batch, AlreadyFinished for a terminal batch, and CancellationRequested otherwise. A repeated request sends no
    /// new signal (§6.5).
    /// </summary>
    internal Task<BatchCancelResult> CancelBatchAsync(Guid batchId, CancellationToken ct) =>
        QueryAndSignalAsync<BatchCancelResult>(CancelBatchSql, ps => P.Add(ps, "id", batchId), async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return (BatchCancelResult.NotFound, 0L);
            var state = r.GetInt16(0);
            var result = state is 2 or 3 or 4 ? BatchCancelResult.AlreadyFinished : BatchCancelResult.CancellationRequested;
            return (result, r.GetBoolean(1) ? 1L : 0L);   // сигнал — после завершения statement'а (коммит на Sync)
        }, ct);

    /// <summary>
    /// Counters have a floor of zero. They can temporarily undercount after a member is requeued and before its previous signal is processed (§10,
    /// §6.6).
    /// </summary>
    internal Task<BatchInfo?> GetBatchAsync(Guid batchId, CancellationToken ct) =>
        QueryAsync<BatchInfo?>(GetBatchSql, ps => P.Add(ps, "id", batchId), async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return null;
            var parent = r.GetNullableValue<Guid>(2);
            var condition = r.GetNullableValue<short>(3);
            return new BatchInfo(new BatchId(r.GetGuid(0)), (BatchState)r.GetInt16(4), r.GetInt32(5), r.GetInt32(6), r.GetInt32(7),
                r.GetInt32(8), r.GetNullableValue<Guid>(1) is { } outer ? new BatchId(outer) : null,
                WorkflowCodes.ParentJob(condition, parent), WorkflowCodes.ParentBatch(condition, parent), WorkflowCodes.Condition(condition),
                r.GetBoolean(9), r.GetFieldValue<DateTimeOffset>(10), r.GetNullableValue<DateTimeOffset>(11));
        }, ct);

    private enum PortionKind { Empty, Done, LockTimeout }

    private readonly record struct CancelPortion(PortionKind Kind, long SignalId, int Rows);

    private readonly record struct CancelStage(bool Requested, int Taken, Guid? Last);

    /// <summary>
    /// Processes ready signals in consecutive transactions until rowBudget or maxTransactions is reached (§6.5). On lock_timeout, rolls back the
    /// portion, sets retry_after for its signal separately, and continues.
    /// </summary>
    internal async Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay,
        CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(portion);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowBudget);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxTransactions);
        int portions = 0, rows = 0, timedOut = 0;
        for (var transactions = 0; transactions < maxTransactions && rows < rowBudget; transactions++)
        {
            var lim = Math.Min(portion, rowBudget - rows);
            var step = await RunAsync(c => CancelMembersOnceAsync(lim, c), ct);
            if (step.Kind == PortionKind.Empty)
                break;
            if (step.Kind == PortionKind.LockTimeout)
            {
                timedOut++;
                await NonQueryAsync(CancelDeferSql, ps =>
                {
                    P.Add(ps, "id", step.SignalId);
                    P.Add(ps, "delay", retryDelay);
                }, ct);
                continue;
            }
            portions++;
            rows += step.Rows;
        }
        return new CancelMembersResult(portions, rows, timedOut);
    }

    private async Task<CancelPortion> CancelMembersOnceAsync(int lim, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // SET LOCAL и захват — одним NpgsqlBatch (BEGIN уходит с ним же): statement'ы выполняются по порядку, lock_timeout
        // действует на захват и statement'ы порции.
        (long Id, Guid Node, short Stage, Guid? Cursor)? captured = null;
        await using (var capture = new NpgsqlBatch(conn, tx)
                     {
                         BatchCommands = { new NpgsqlBatchCommand("SET LOCAL lock_timeout = '1s'"), new NpgsqlBatchCommand(CancelCaptureSql) },
                     })
        {
            await using var r = await capture.ExecuteReaderAsync(ct);
            if (await r.ReadAsync(ct))
                captured = (r.GetInt64(0), r.GetGuid(1), r.GetInt16(2), r.GetNullableValue<Guid>(3));
        }
        if (captured is not { } signal)
        {
            await tx.CommitAsync(ct);
            return new CancelPortion(PortionKind.Empty, 0, 0);
        }

        try
        {
            var (stage, cursor) = (signal.Stage, signal.Cursor);
            var rows = 0;
            var done = false;
            if (stage == 0)
            {
                var jobs = await CancelStageAsync(conn, tx, CancelJobsSql, signal.Node, cursor, lim, jobsStage: true, ct);
                rows += jobs.Taken;
                if (!jobs.Requested)
                    done = true;                       // флаг сброшен requeue'ом или строки нет: устаревший сигнал
                else if (jobs.Taken == lim)
                    cursor = jobs.Last;                // порция исчерпана
                else
                    (stage, cursor) = ((short)1, null);
            }
            if (!done && stage == 1 && rows < lim)
            {
                var left = lim - rows;
                var nested = await CancelStageAsync(conn, tx, CancelNestedSql, signal.Node, cursor, left, jobsStage: false, ct);
                rows += nested.Taken;
                if (!nested.Requested || nested.Taken < left)
                    done = true;
                else
                    cursor = nested.Last;
            }

            await using (var finish = new NpgsqlCommand(done ? CancelDoneSql : CancelAdvanceSql, conn, tx))
            {
                P.Add(finish.Parameters, "id", signal.Id);
                if (!done)
                {
                    P.Add(finish.Parameters, "stage", stage);
                    P.AddValue(finish.Parameters, "cursor", cursor, NpgsqlTypes.NpgsqlDbType.Uuid);
                }
                await finish.ExecuteNonQueryAsync(ct);
            }
            await tx.CommitAsync(ct);
            return new CancelPortion(PortionKind.Done, signal.Id, rows);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            return new CancelPortion(PortionKind.LockTimeout, signal.Id, 0);   // dispose транзакции — откат порции
        }
    }

    private static async Task<CancelStage> CancelStageAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, Guid node, Guid? cursor,
        int lim, bool jobsStage, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        P.Add(cmd.Parameters, "node", node);
        P.Add(cmd.Parameters, "cursor", cursor ?? Guid.Empty);
        P.Add(cmd.Parameters, "lim", lim);
        if (jobsStage)
            P.Add(cmd.Parameters, "kind_cancelled", (short)JobEventKind.Cancelled);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        return new CancelStage(r.GetBoolean(0), r.GetInt32(1), r.GetNullableValue<Guid>(2));
    }
}
