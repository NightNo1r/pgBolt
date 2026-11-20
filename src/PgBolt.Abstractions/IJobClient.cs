using System.Text.Json;

namespace PgBolt;

public interface IJobClient
{
    ValueTask<EnqueueResult> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, CancellationToken ct = default);

    ValueTask<IReadOnlyList<EnqueueResult>> EnqueueManyAsync<TArgs>(
        IReadOnlyList<TArgs> args, EnqueueOptions? options = null,
        Func<TArgs, string?>? uniqueKeySelector = null, CancellationToken ct = default);

    ValueTask<EnqueueResult> EnqueueAsync(string jobName, JsonElement args, EnqueueOptions? options = null, CancellationToken ct = default);

    ValueTask<CancelResult> CancelAsync(JobId id, string? reason = null, CancellationToken ct = default);
    ValueTask<bool> TriggerAsync(JobId id, CancellationToken ct = default);
    /// <summary>
    /// Failed/Cancelled → Pending (core §5.9). Член batch (часть 5 §6.6): batch и терминальные внешние синхронно возвращаются в
    /// Active, исход вычисляется заново при следующем завершении; уже запущенные или отменённые продолжения не пересматриваются.
    /// Requeue Cancelled-члена при идущей отмене batch: флаг отмены не сбрасывается, член выполняется или отменяется снова в
    /// зависимости от позиции обхода отмены, ждущие члены его ветки не запускаются, а отменяются; детерминированно — requeue после
    /// того, как batch стал терминальным. Requeue отменённого продолжения ставит его сразу в Pending.
    /// </summary>
    ValueTask<RequeueResult> RequeueAsync(JobId id, CancellationToken ct = default);
    ValueTask<JobInfo?> GetAsync(JobId id, CancellationToken ct = default);

    /// <summary>Новый граф продолжений и batches (часть 5 §5); пишется SubmitAsync одной транзакцией.</summary>
    IWorkflow Workflow(WorkflowOptions? options = null);

    /// <summary>
    /// Отмена batch (часть 5 §6.5): невзятые члены отменяются сразу, выполняющимся отмена доставляется heartbeat'ом, вложенные
    /// batches — рекурсивно; член, успевший завершиться успехом, учитывается как Succeeded. Ждущие члены ветки не запускаются.
    /// Повторная отмена — CancellationRequested без нового сигнала. Требует resolver (сервер с RunMaintenance = true).
    /// </summary>
    ValueTask<BatchCancelResult> CancelBatchAsync(BatchId id, CancellationToken ct = default);

    /// <summary>Строка batch; счётчики ≥ 0 и временно могут быть ниже факта после requeue члена (§6.6).</summary>
    ValueTask<BatchInfo?> GetBatchAsync(BatchId id, CancellationToken ct = default);
}
