using Npgsql;

namespace PgBolt.Postgres.Schema;

/// <summary>
/// Часть 5 §7.1: источники живых строк партиции jobs [lo, hi) — (а) родители-задачи с ждущими детьми (loose index scan по
/// queue_await_jobs / batches_await_jobs, не больше T + 1 спусков), (б) Failed- и Cancelled-члены существующих незавершённых
/// успехом batches (только в предпроверке, по партиции); удаление строк batches по JobsRetention — окно по batches_finished,
/// блокировка кандидатов окна, затем проверка новым снимком.
/// </summary>
internal sealed partial class RetentionRunner
{
    internal const int BatchDeleteChunk = 1000;

    /// <summary>(а): различные parent_id ожиданий родителей-задач (коды 1, 2) в [lo, hi); lo/hi/limit — текст параметров ($N).</summary>
    internal string AwaitingParentsSql(bool batches, string lo, string hi, string limit)
    {
        var (table, predicate) = batches
            ? ("batches", "state = 1 AND condition IN (1, 2)")
            : ("queue", "await_condition IN (1, 2)");
        return $"""
            WITH RECURSIVE p(id) AS (
                (SELECT parent_id FROM {_schema}.{table}
                 WHERE {predicate} AND parent_id >= {lo} AND parent_id < {hi} ORDER BY parent_id LIMIT 1)
                UNION ALL
                SELECT (SELECT parent_id FROM {_schema}.{table}
                        WHERE {predicate} AND parent_id > p.id AND parent_id < {hi} ORDER BY parent_id LIMIT 1)
                FROM p WHERE p.id IS NOT NULL
            )
            SELECT id FROM p WHERE id IS NOT NULL LIMIT {limit}
            """;
    }

    /// <summary>
    /// (б): у Succeeded-batch Failed/Cancelled-членов нет (§6.6), поэтому обходятся batches_open. Читается партиция кандидата
    /// по имени (её строки — ровно [lo, hi), jobs_batch на ней есть), а не родитель jobs; $1/$2 — диапазон, $3 — лимит.
    /// </summary>
    private string BatchMembersSql(string partition) => $"""
        SELECT j.id FROM {_schema}.batches b
        CROSS JOIN LATERAL (SELECT id FROM {Qualified(partition)}
                            WHERE batch_id = b.id AND batch_id IS NOT NULL AND id >= $1 AND id < $2 AND state IN (3, 4)) j
        WHERE b.state <> 2
        LIMIT $3
        """;

    /// <summary>
    /// Предпроверка: живые queue в [$1, $2) и (б) одним statement'ом — в одном снимке (§7.1); каждый источник — LIMIT $3.
    /// </summary>
    internal string QueueAndBatchMembersSql(string partition) => $"""
        (SELECT job_id FROM {_schema}.queue WHERE job_id >= $1 AND job_id < $2 LIMIT $3)
        UNION ALL
        ({BatchMembersSql(partition)})
        """;

    /// <summary>
    /// §7.1, окно statement'а 1: следующие строки в порядке (finished_at, id) — чистый range scan по batches_finished, без
    /// блокировок и anti-join, поэтому каждая строка читается запуском один раз. $1 — порог, $2/$3 — курсор (последняя
    /// строка предыдущего окна, в пределах запуска).
    /// </summary>
    internal string BatchCandidatesSql(bool withCursor) => $"""
        SELECT id, finished_at FROM {_schema}.batches
        WHERE finished_at < $1 {(withCursor ? "AND (finished_at, id) > ($2, $3)" : "")}
        ORDER BY finished_at, id
        LIMIT {BatchDeleteChunk}
        """;

    /// <summary>
    /// §7.1, statement 1: блокировка кандидатов окна $1 с проверками statement'а 2 (только отсев заведомо живых),
    /// FOR UPDATE SKIP LOCKED в порядке id. Порог $2 повторяется здесь, а не только в окне без блокировок: batch, который
    /// между окном и блокировкой прошёл requeue и завершился снова, моложе JobsRetention; строку, изменённую во время
    /// ожидания блокировки, условие перепроверяет на новой версии (EvalPlanQual).
    /// </summary>
    private string BatchLockSql => $"""
        SELECT b.id FROM {_schema}.batches b
        WHERE b.id = ANY($1)
          AND b.finished_at < $2
          AND b.state IN (2, 3, 4)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.queue q WHERE q.parent_id = b.id AND q.await_condition IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches c WHERE c.parent_id = b.id AND c.parent_id IS NOT NULL AND c.state = 1)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches o WHERE o.id = b.parent_batch_id)
        ORDER BY b.id
        FOR UPDATE OF b SKIP LOCKED
        """;

    /// <summary>§7.1, statement 2: решение — новым снимком после блокировок (подписка, закоммиченная до блокировки, видна).</summary>
    private string BatchDeleteSql => $"""
        DELETE FROM {_schema}.batches b
        WHERE b.id = ANY($1)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.queue WHERE parent_id = b.id AND await_condition IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches WHERE parent_id = b.id AND state = 1)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches o WHERE o.id = b.parent_batch_id)
        RETURNING b.id
        """;

    /// <summary>
    /// §7.1: окна по 1000 строк; порог now() − JobsRetention фиксируется на запуск; курсор — последняя строка окна;
    /// окно короче 1000 завершает запуск. Окно и блокировка — два round-trip'а (блокировка зависит от окна).
    /// </summary>
    private async Task<int> DeleteBatchesAsync(TimeSpan retention, CancellationToken ct)
    {
        DateTime threshold;
        await using (var now = _dataSource.CreateCommand("SELECT now() - $1"))
        {
            now.Parameters.Add(Arg(retention));
            threshold = (DateTime)(await now.ExecuteScalarAsync(ct))!;
        }

        var deleted = 0;
        (DateTime At, Guid Id)? cursor = null;
        while (true)
        {
            await using var conn = await _dataSource.OpenConnectionAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);
            var window = new List<(Guid Id, DateTime At)>(BatchDeleteChunk);
            await using (var select = new NpgsqlCommand(BatchCandidatesSql(cursor is not null), conn, tx))
            {
                select.Parameters.Add(Arg(threshold));
                if (cursor is { } c)
                {
                    select.Parameters.Add(Arg(c.At));
                    select.Parameters.Add(Arg(c.Id));
                }
                await using var r = await select.ExecuteReaderAsync(ct);
                while (await r.ReadAsync(ct))
                    window.Add((r.GetGuid(0), r.GetDateTime(1)));
            }
            await HookAsync(RetentionStep.BatchesWindowed, "batches", [], ct);
            var locked = new List<Guid>(window.Count);
            if (window.Count > 0)
            {
                await using var lockCmd = new NpgsqlCommand(BatchLockSql, conn, tx);
                lockCmd.Parameters.Add(Arg(window.Select(w => w.Id).ToArray()));
                lockCmd.Parameters.Add(Arg(threshold));
                await using var r = await lockCmd.ExecuteReaderAsync(ct);
                while (await r.ReadAsync(ct))
                    locked.Add(r.GetGuid(0));
            }
            await HookAsync(RetentionStep.BatchesLocked, "batches", [], ct);
            if (locked.Count > 0)
            {
                await using var delete = new NpgsqlCommand(BatchDeleteSql, conn, tx);
                delete.Parameters.Add(Arg(locked.ToArray()));
                await using var r = await delete.ExecuteReaderAsync(ct);
                while (await r.ReadAsync(ct))
                    deleted++;
            }
            await tx.CommitAsync(ct);
            if (window.Count < BatchDeleteChunk)
                return deleted;
            cursor = (window[^1].At, window[^1].Id);
        }
    }
}
