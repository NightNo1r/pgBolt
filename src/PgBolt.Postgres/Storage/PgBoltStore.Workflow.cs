using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Writes a workflow graph in one transaction (Part 5 §6.1). Inserts batches with one unnest, then jobs with workflow enqueue columns, sending
/// all commands in one NpgsqlBatch. Resolves conflicts, subscribes to external parents in ID order (§6.2), and writes synthetic signals.
/// </summary>
public sealed partial class PgBoltStore
{
    private const string WorkflowSavepoint = "pgbolt_workflow";
    private const string AfterSavepoint = "pgbolt_after";

    private const string InsertBatchesTemplate = """
        INSERT INTO {schema}.batches (id, parent_batch_id, parent_id, condition, state, total, created_at)
        SELECT id, parent_batch_id, parent_id, condition, state, total, now()
        FROM unnest(@ids::uuid[], @outer_ids::uuid[], @parent_ids::uuid[], @conditions::smallint[], @states::smallint[], @totals::int[])
             AS t(id, parent_batch_id, parent_id, condition, state, total)
        """;

    private const string SubscribeQueueTemplate = "UPDATE {schema}.queue SET notify = true WHERE job_id = @id RETURNING 1";

    private const string SubscribeTerminalTemplate =
        "UPDATE {schema}.jobs SET notify = true WHERE id = @id AND state IN (2, 3, 4) RETURNING state";

    private const string SubscribeBatchTemplate = "SELECT 1 FROM {schema}.batches WHERE id = @id FOR KEY SHARE";

    private const string SyntheticSignalsTemplate = """
        INSERT INTO {schema}.completions (node_id, kind)
        SELECT node_id, kind FROM unnest(@nodes::uuid[], @kinds::smallint[]) AS t(node_id, kind)
        """;

    private const string GraphWrittenTemplate = """
        SELECT EXISTS (SELECT 1 FROM {schema}.batches WHERE id = @id) OR EXISTS (SELECT 1 FROM {schema}.jobs WHERE id = @id)
        """;

    private string InsertBatchesSql => field ??= Render(InsertBatchesTemplate);
    private string SubscribeQueueSql => field ??= Render(SubscribeQueueTemplate);
    private string SubscribeTerminalSql => field ??= Render(SubscribeTerminalTemplate);
    private string SubscribeBatchSql => field ??= Render(SubscribeBatchTemplate);
    private string SyntheticSignalsSql => field ??= Render(SyntheticSignalsTemplate);
    private string GraphWrittenSql => field ??= Render(GraphWrittenTemplate);

    /// <summary>
    /// In a caller-provided transaction, creates SAVEPOINT pgbolt_workflow, rolls back to it on any exception, and rethrows without retries or
    /// wakeups (§6.1). With a null transaction, owns the transaction. After 40P01, 40001, a transient error, or evidence of a previous attempt
    /// (23505 on the batches/jobs primary key or exists_by_id), checks the graph's first row in a fresh transaction. An existing row means success,
    /// otherwise retry.
    /// </summary>
    internal async Task<WorkflowWriteResult> SubmitWorkflowAsync(WorkflowPlan plan, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(plan);
        if (plan.Batches.Count == 0 && plan.Jobs.Count == 0)
            throw new ArgumentException("The workflow plan is empty.", nameof(plan));
        foreach (var jobs in plan.Jobs)
        {
            jobs.Validate();
            if (jobs.ParentIds is null)
                throw new ArgumentException("Workflow job groups must carry workflow arrays.", nameof(plan));
        }

        if (transaction is not null)
        {
            var conn = transaction.Connection ?? throw new InvalidOperationException("The transaction is already completed.");
            await transaction.SaveAsync(WorkflowSavepoint, ct);
            try
            {
                var written = await WriteWorkflowAsync(conn, transaction, plan, ct);
                await transaction.ReleaseAsync(WorkflowSavepoint, ct);
                return written;
            }
            catch (WorkflowAlreadyWrittenException e)
            {
                // Без повтора внутренний признак не наружу: строки с id графа уже есть в видимых этой транзакции данных.
                await RollbackToSavepointAsync(transaction, WorkflowSavepoint);
                throw new InvalidOperationException(
                    "Rows with the workflow's job or batch ids already exist: the workflow was already submitted "
                    + "(possibly earlier in this transaction).", e);
            }
            catch
            {
                await RollbackToSavepointAsync(transaction, WorkflowSavepoint);
                throw;
            }
        }

        var transientRetried = false;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var conn = await _dataSource.OpenConnectionAsync(ct);
                await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
                var written = await WriteWorkflowAsync(conn, tx, plan, ct);
                await tx.CommitAsync(ct);
                if (written.SignalResolver)
                    Completions?.Signal();
                return written;
            }
            catch (Exception e) when (IsPreviousAttempt(e))
            {
                // Предыдущая попытка закоммичена (возможно, пока эта ждала её строк): транзакция откатана, проверка новой.
                if (await GraphWrittenAsync(plan.FirstNodeId, ct))
                    return Recovered(plan);
                if (attempt >= MaxStatementAttempts)
                {
                    // Внутренний признак повтора не уходит пользователю; 23505 по PK — пробрасывается как есть.
                    if (e is WorkflowAlreadyWrittenException)
                        throw new InvalidOperationException(
                            "Rows with the workflow's job or batch ids already exist, but the workflow's first row was not found "
                            + $"after {attempt} attempts: the ids are used by another submission.", e);
                    throw;
                }
                continue;
            }
            catch (PostgresException e) when (attempt < MaxStatementAttempts && _retryOnConflict && DbRetry.IsRetryable(e))
            {
            }
            catch (NpgsqlException e) when (e is not PostgresException && e.IsTransient && !transientRetried)
            {
                transientRetried = true;
            }

            // Перед повтором: ответ на COMMIT мог потеряться после фиксации (§6.1 «Повтор»).
            if (await GraphWrittenAsync(plan.FirstNodeId, ct))
                return Recovered(plan);
        }
    }

    /// <summary>A previous attempt wrote the graph. Any synthetic signals for external parents are already in completions.</summary>
    private WorkflowWriteResult Recovered(WorkflowPlan plan)
    {
        var result = new WorkflowWriteResult(plan.External.Count > 0);
        if (result.SignalResolver)
            Completions?.Signal();
        return result;
    }

    private static bool IsPreviousAttempt(Exception e) =>
        e is WorkflowAlreadyWrittenException
        || e is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation, TableName: "batches" }
        || (e is PostgresException p && IsJobsPrimaryKeyViolation(p));

    private async Task<bool> GraphWrittenAsync(Guid firstNodeId, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(GraphWrittenSql, conn);
        P.Add(cmd.Parameters, "id", firstNodeId);
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>Runs steps 1–5 of §6.1 in transaction tx.</summary>
    private async Task<WorkflowWriteResult> WriteWorkflowAsync(NpgsqlConnection conn, NpgsqlTransaction tx, WorkflowPlan plan,
        CancellationToken ct)
    {
        // 1–2. Batches и задачи — одним NpgsqlBatch.
        var rows = new Dictionary<Guid, (bool Created, bool Existed)>(plan.Jobs.Sum(j => j.Ids.Length));
        await using (var batch = new NpgsqlBatch(conn, tx))
        {
            if (plan.Batches.Count > 0)
                batch.BatchCommands.Add(InsertBatchesCommand(plan.Batches));
            foreach (var jobs in plan.Jobs)
                batch.BatchCommands.Add(CreateEnqueueCommand(jobs));
            await using var r = await batch.ExecuteReaderAsync(ct);
            do
            {
                while (await r.ReadAsync(ct))
                    rows[r.GetGuid(0)] = (r.GetBoolean(1), r.GetBoolean(2));
            }
            while (await r.NextResultAsync(ct));
        }

        // 3. Конфликты: existed — строку уже записала предыдущая попытка; иначе запрос разрешения core §5.1.
        if (rows.Values.Any(v => v.Existed))
            throw new WorkflowAlreadyWrittenException();
        var unresolved = plan.Jobs
            .SelectMany(b => b.Ids.Select((id, i) => (Id: id, Key: b.UniqueKeys[i])))
            .Where(x => !rows[x.Id].Created)
            .ToList();
        if (unresolved.Count > 0)
            await ThrowWorkflowConflictAsync(conn, tx, unresolved, ct);

        // 4. Подписка на внешних родителей (§6.2) — последняя запись, в порядке id.
        var nodes = new List<Guid>();
        var kinds = new List<short>();
        foreach (var parent in plan.External)
        {
            var kind = parent.IsBatch
                ? await SubscribeBatchAsync(conn, tx, parent.Id, ct)
                : await SubscribeJobAsync(conn, tx, parent.Id, ct);
            if (kind is { } k)
            {
                nodes.Add(parent.Id);
                kinds.Add(k);
            }
        }

        // 5. Синтетические сигналы.
        if (nodes.Count > 0)
        {
            await using var signals = new NpgsqlCommand(SyntheticSignalsSql, conn, tx);
            P.Add(signals.Parameters, "nodes", nodes.ToArray());
            P.Add(signals.Parameters, "kinds", kinds.ToArray());
            await signals.ExecuteNonQueryAsync(ct);
        }
        return new WorkflowWriteResult(nodes.Count > 0);
    }

    private NpgsqlBatchCommand InsertBatchesCommand(IReadOnlyList<WorkflowBatchRow> batches)
    {
        var cmd = new NpgsqlBatchCommand(InsertBatchesSql);
        var ps = cmd.Parameters;
        P.Add(ps, "ids", batches.Select(b => b.Id).ToArray());
        P.Add(ps, "outer_ids", batches.Select(b => b.OuterBatchId).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "parent_ids", batches.Select(b => b.ParentId).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "conditions", batches.Select(b => b.Condition).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Smallint);
        P.Add(ps, "states", batches.Select(b => b.State).ToArray());
        P.Add(ps, "totals", batches.Select(b => b.Total).ToArray());
        return cmd;
    }

    /// <summary>exists_by_id indicates an earlier attempt. Otherwise a unique_key conflict raises UniqueKeyConflictException (§6.1, step
    /// 3).</summary>
    private async Task ThrowWorkflowConflictAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<(Guid Id, string? Key)> unresolved,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(ResolveSql, conn, tx);
        P.Add(cmd.Parameters, "ids", unresolved.Select(u => u.Id).ToArray());
        P.Add(cmd.Parameters, "unique_keys", unresolved.Select(u => u.Key).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Text);
        var keys = unresolved.ToDictionary(u => u.Id, u => u.Key);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            if (r.GetBoolean(1) || keys[r.GetGuid(0)] is not { } key)
                throw new WorkflowAlreadyWrittenException();
            var existing = r.GetNullableValue<Guid>(2);
            throw new UniqueKeyConflictException(key, existing is { } e ? new JobId(e) : null);
        }
        throw new WorkflowAlreadyWrittenException();
    }

    /// <summary>
    /// Subscribes to a parent job (§6.2, steps 1–3). Returns the synthetic signal kind for a terminal parent or null after subscribing to a Pending,
    /// Awaiting, or running parent's queue row. A missing job raises ParentNotFoundException.
    /// </summary>
    private async Task<short?> SubscribeJobAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, CancellationToken ct)
    {
        while (true)
        {
            await using (var queue = new NpgsqlCommand(SubscribeQueueSql, conn, tx))
            {
                P.Add(queue.Parameters, "id", id);
                if (await queue.ExecuteScalarAsync(ct) is not null)
                    return null;
            }

            await tx.SaveAsync(AfterSavepoint, ct);
            await using (var terminal = new NpgsqlCommand(SubscribeTerminalSql, conn, tx))
            {
                P.Add(terminal.Parameters, "id", id);
                if (await terminal.ExecuteScalarAsync(ct) is short state)
                {
                    await tx.ReleaseAsync(AfterSavepoint, ct);
                    return state;
                }
            }
            // Ноль строк: UPDATE мог дождаться requeue и заблокировать новую версию строки jobs — снять блокировку (§6.2).
            await tx.RollbackAsync(AfterSavepoint, ct);

            await using var exists = new NpgsqlCommand(JobStateSql, conn, tx);
            P.Add(exists.Parameters, "id", id);
            if (await exists.ExecuteScalarAsync(ct) is null)
                throw new ParentNotFoundException(id);
            // Строка есть: requeue после снимка п. 1 (state 0) или финализация после п. 2 — повтор с п. 1.
        }
    }

    /// <summary>
    /// Takes KEY SHARE on the batch row. Emits Subscribed for an existing batch or throws ParentNotFoundException if missing (§6.2, step 4).
    /// </summary>
    private async Task<short?> SubscribeBatchAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(SubscribeBatchSql, conn, tx);
        P.Add(cmd.Parameters, "id", id);
        return await cmd.ExecuteScalarAsync(ct) is null ? throw new ParentNotFoundException(id) : WorkflowCodes.Subscribed;
    }

    /// <summary>Rolls back to the savepoint after any failure, preserving the original exception if rollback fails on a broken connection.</summary>
    private static async Task RollbackToSavepointAsync(NpgsqlTransaction tx, string savepoint)
    {
        try
        {
            await tx.RollbackAsync(savepoint, CancellationToken.None);
        }
        catch (Exception e) when (e is NpgsqlException or InvalidOperationException)
        {
            // Соединение разорвано: транзакцию откатит сервер, исходное исключение важнее.
        }
    }
}
