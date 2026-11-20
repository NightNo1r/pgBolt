namespace PgBolt.Postgres.Storage;

/// <summary>
/// Строка batches графа (§6.1 п. 1). State — WorkflowCodes.BatchAwaiting у batch-продолжений и вложенных в них на любой глубине,
/// иначе BatchActive; Condition — код 1–4 при ParentId; Total — число прямых членов (задачи + вложенные batches).
/// </summary>
internal sealed record WorkflowBatchRow(Guid Id, Guid? OuterBatchId, Guid? ParentId, short? Condition, short State, int Total);

/// <summary>Существующий родитель After (§6.2): задача или batch.</summary>
internal readonly record struct WorkflowParent(Guid Id, bool IsBatch);

/// <summary>Граф одного SubmitAsync: batches, группы задач (EnqueueBatch с массивами графа), внешние родители в порядке id.</summary>
internal sealed record WorkflowPlan(IReadOnlyList<WorkflowBatchRow> Batches, IReadOnlyList<EnqueueBatch> Jobs, IReadOnlyList<WorkflowParent> External)
{
    /// <summary>§6.1 «Повтор»: первая строка графа — первый batch, иначе первая задача.</summary>
    public Guid FirstNodeId => Batches.Count > 0 ? Batches[0].Id : Jobs[0].Ids[0];
}

/// <summary>Итог записи графа: SignalResolver — у графа есть синтетические сигналы (§6.1 п. 5).</summary>
internal readonly record struct WorkflowWriteResult(bool SignalResolver);

/// <summary>
/// Строки графа уже записаны (или пишутся) предыдущей попыткой того же SubmitAsync (§6.1 «Повтор»). Только внутренний сигнал
/// повтора: наружу SubmitWorkflowAsync его не пропускает, а оборачивает в InvalidOperationException.
/// </summary>
internal sealed class WorkflowAlreadyWrittenException()
    : Exception("The workflow graph has already been written by a previous attempt of the same SubmitAsync.");

/// <summary>
/// Итог Т1 (§6.4): Captured — захвачено сигналов (= batchSize — пакет полный); Finished — batches перешли в терминальное
/// состояние (новые сигналы); Walking — сигналы с детьми переданы Т2; MissingBatches — дельты для удалённых строк batches (Debug);
/// WalkReady / CancelReady — в снимке statement'а есть сигнал, который взял бы захват Т2 / CancelMembers (без переданных этим Т1).
/// </summary>
internal sealed record CompletionCountResult(int Captured, int Finished, int Walking, IReadOnlyList<Guid> MissingBatches,
    bool WalkReady, bool CancelReady)
{
    public static readonly CompletionCountResult Empty = new(0, 0, 0, [], false, false);
}

/// <summary>
/// Итог Т2 (§6.4): Rows — строк детей в порциях; Statements — команд транзакции (не зависит от числа узлов); MaxPortion — наибольшая
/// порция узла; BudgetExhausted — остались узлы без остатка из-за бюджета; NewSignals — Activated / CancelMembers / Cancelled-детей;
/// Deferred — сигналы с retry_after (остаток заблокирован другими); Queues — очереди запущенных детей.
/// </summary>
internal sealed record ResolveResult(int Captured, int Rows, int Statements, int MaxPortion, bool BudgetExhausted, int NewSignals,
    int Deferred, IReadOnlyCollection<string> Queues)
{
    public static readonly ResolveResult Empty = new(0, 0, 1, 0, false, 0, 0, []);
}

/// <summary>Точки тестового хука resolver'а: перед COMMIT транзакции Т2.</summary>
internal enum ResolverStep { Walked }

/// <summary>
/// Итог CancelMembers за итерацию (§6.5): Portions — выполненные транзакции-порции (≥ 1 — итерация повторяется сразу);
/// Rows — строк обхода; TimedOut — порции, откатанные по lock_timeout.
/// </summary>
internal sealed record CancelMembersResult(int Portions, int Rows, int TimedOut)
{
    public static readonly CancelMembersResult Empty = new(0, 0, 0);
}
