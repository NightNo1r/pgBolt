using Npgsql;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Отмена batch (часть 5 §6.5): CancelBatchAsync — один statement; CancelMembers — короткие транзакции с lock_timeout 1 с, сигнал
/// за сигналом: этап 0 — члены-задачи (jobs_batch, семантика cancel core §5.7 набором), этап 1 — вложенные batches. Каждый
/// statement порции перечитывает cancel_requested своего узла: сброшен requeue'ом (§6.6) — порция ничего не меняет, сигнал удаляется.
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

    /// <summary>§6.5 stage = 0: члены по jobs_batch без блокировки jobs; их строки queue — ORDER BY job_id FOR UPDATE (без SKIP LOCKED).</summary>
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
    /// §6.5 stage = 1: вложенные batches (batches_nested) в порядке id; Active/Awaiting без флага — флаг и свой CancelMembers.
    /// Помечается вложенный, открытый и в снимке statement'а (c.open; в том же снимке прочитан cancel_requested узла, b), и в
    /// последней версии под FOR NO KEY UPDATE (lk). Ставший Active requeue'ом, закоммиченным после снимка, не помечается по правилу
    /// снимка (§6.5, §6.6 «Requeue при идущей отмене»): в снимке он терминален, фильтр c.open не проходит — для порции такой batch
    /// пройден обходом. Флаг узла этот requeue сбрасывает, только если узел был терминальным; на нетерминальном узле с
    /// cancel_requested подъём останавливается и флаг остаётся (§6.6 п. 2) — ждущие члены ветки отменяются проверкой по флагу
    /// внешнего (§6.4 Т2.3). Закрытые в снимке не блокируются; курсор и размер порции — по всем вложенным (c).
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

    /// <summary>§6.5: нет строки — NotFound; терминальный — AlreadyFinished; иначе CancellationRequested (повтор — без нового сигнала).</summary>
    internal Task<BatchCancelResult> CancelBatchAsync(Guid batchId, CancellationToken ct) =>
        QueryAndSignalAsync<BatchCancelResult>(CancelBatchSql, ps => P.Add(ps, "id", batchId), async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return (BatchCancelResult.NotFound, 0L);
            var state = r.GetInt16(0);
            var result = state is 2 or 3 or 4 ? BatchCancelResult.AlreadyFinished : BatchCancelResult.CancellationRequested;
            return (result, r.GetBoolean(1) ? 1L : 0L);   // сигнал — после завершения statement'а (коммит на Sync)
        }, ct);

    /// <summary>§10: счётчики ограничены снизу нулём — между requeue члена и учётом его прежнего сигнала они бывают ниже факта (§6.6).</summary>
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
    /// §6.5: транзакции подряд, сигнал за сигналом, пока есть готовые сигналы, строк обхода меньше rowBudget и транзакций меньше
    /// maxTransactions. lock_timeout — откат порции и retry_after этого сигнала отдельным statement'ом, итерация продолжается.
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
