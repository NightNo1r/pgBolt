using Npgsql;

namespace PgBolt.Postgres.Schema;

/// <summary>
/// Live rows in jobs partition [lo, hi) include (a) parent jobs with waiting children, found by loose index scans on queue_await_jobs and
/// batches_await_jobs with at most T + 1 descents, and (b) Failed or Cancelled members of existing batches that have not succeeded, checked only
/// during the partition precheck. To expire batches older than JobsRetention, scan windows on batches_finished, lock candidates, then recheck in
/// a fresh snapshot (Part 5 §7.1).
/// </summary>
internal sealed partial class RetentionRunner
{
    internal const int BatchDeleteChunk = 1000;

    /// <summary>
    /// Source (a): distinct parent_id values in [lo, hi) for jobs waiting on parent jobs (codes 1 and 2). lo, hi, and limit are parameter
    /// placeholders ($N).
    /// </summary>
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
    /// Source (b): a Succeeded batch has no Failed or Cancelled members (§6.6), so scan batches_open. Read the named candidate partition, whose rows
    /// cover exactly [lo, hi) and have a jobs_batch index. $1 and $2 bound the range, and $3 limits rows.
    /// </summary>
    private string BatchMembersSql(string partition) => $"""
        SELECT j.id FROM {_schema}.batches b
        CROSS JOIN LATERAL (SELECT id FROM {Qualified(partition)}
                            WHERE batch_id = b.id AND batch_id IS NOT NULL AND id >= $1 AND id < $2 AND state IN (3, 4)) j
        WHERE b.state <> 2
        LIMIT $3
        """;

    /// <summary>Reads live queue rows in [$1, $2) and source (b) in one statement and snapshot (§7.1). Each source uses LIMIT $3.</summary>
    internal string QueueAndBatchMembersSql(string partition) => $"""
        (SELECT job_id FROM {_schema}.queue WHERE job_id >= $1 AND job_id < $2 LIMIT $3)
        UNION ALL
        ({BatchMembersSql(partition)})
        """;

    /// <summary>
    /// Window for statement 1 (§7.1). Scan the next rows in (finished_at, id) order via batches_finished, without locks or an anti-join. Each row is
    /// read once per run. $1 is the cutoff, and $2/$3 identify the previous window's last row.
    /// </summary>
    internal string BatchCandidatesSql(bool withCursor) => $"""
        SELECT id, finished_at FROM {_schema}.batches
        WHERE finished_at < $1 {(withCursor ? "AND (finished_at, id) > ($2, $3)" : "")}
        ORDER BY finished_at, id
        LIMIT {BatchDeleteChunk}
        """;

    /// <summary>
    /// Statement 1 (§7.1) locks candidates in window $1 with FOR UPDATE SKIP LOCKED in ID order. Statement 2's checks filter out obviously live
    /// rows. Recheck cutoff $2 because a batch requeued and finished again since the unlocked scan can be younger than JobsRetention. EvalPlanQual
    /// checks the new row version if it changed while awaiting a lock.
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

    /// <summary>Statement 2 (§7.1) decides from a fresh snapshot after locking, so subscriptions committed before the lock are visible.</summary>
    private string BatchDeleteSql => $"""
        DELETE FROM {_schema}.batches b
        WHERE b.id = ANY($1)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.queue WHERE parent_id = b.id AND await_condition IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches WHERE parent_id = b.id AND state = 1)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches o WHERE o.id = b.parent_batch_id)
        RETURNING b.id
        """;

    /// <summary>
    /// Reads windows of 1000 rows, fixing the cutoff at now() − JobsRetention for the run (§7.1). The last row supplies the cursor. A short window
    /// ends the run. Reading and locking require separate round trips because the lock depends on the window.
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
