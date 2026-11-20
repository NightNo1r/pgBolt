using System.Data.Common;
using System.Text.Json;

namespace PgBolt;

/// <summary>Условие продолжения (часть 5 §2.2): по исходу родителя — задачи или batch.</summary>
public enum ContinuationCondition
{
    /// <summary>Запуск после Succeeded; Failed — ждать requeue родителя; Cancelled — каскадная отмена.</summary>
    OnSuccess,

    /// <summary>Запуск после любого терминального исхода.</summary>
    OnAnyFinished,
}

public sealed record WorkflowOptions
{
    /// <summary>
    /// Запись графа в транзакции пользователя: только NpgsqlTransaction с уровнем READ COMMITTED (core §5.1). Любое исключение
    /// SubmitAsync откатывает записи графа до точки сохранения pgbolt_workflow — транзакция остаётся рабочей и не содержит строк
    /// графа. Коммитить сразу после SubmitAsync: After на задачу в queue держит блокировку её строки, на терминальную — строки
    /// jobs до коммита: до него Pending-родитель не берётся в работу, heartbeat, ack и fail сервера, выполняющего родителя, ждут
    /// (ожидание дольше LeaseDuration − HeartbeatInterval — «heartbeat потерян» и отмена всех выполнений сервера), RequeueAsync
    /// родителя ждёт, а отмена batch родителя стоит на его строке (часть 5 §6.2, §6.5).
    /// </summary>
    public DbTransaction? Transaction { get; init; }
}

/// <summary>EnqueueOptions без Transaction (часть 5 §5.1). Значения [Job] (или job_types) — умолчания.</summary>
public sealed record StepOptions
{
    public string? Queue { get; init; }
    public short? Priority { get; init; }

    /// <summary>Только у корневых задач и членов batch без родителя; взаимоисключимо с Delay.</summary>
    public DateTimeOffset? RunAt { get; init; }

    /// <summary>Только у корневых задач и членов batch без родителя; отсчитывается от now() сервера БД.</summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>Не у членов batch; префикс pgbolt: зарезервирован; ключ уникален в пределах графа.</summary>
    public string? UniqueKey { get; init; }

    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
}

/// <summary>
/// Общий набор методов workflow и тела batch (часть 5 §5.1). Шаги описывают граф синхронно; запись — SubmitAsync. Построитель
/// не потокобезопасный (§5.2): workflow, его шаги и тела batch вызываются из одного потока.
/// </summary>
public interface IWorkflowScope
{
    /// <remarks>Метаданные TArgs генератор регистрирует по месту вызова, как для EnqueueAsync.</remarks>
    IJobStep Enqueue<TArgs>(TArgs args, StepOptions? options = null);

    IJobStep Enqueue(string jobName, JsonElement args, StepOptions? options = null);

    /// <summary>
    /// Batch в этой области; его члены добавляются только внутри <paramref name="build"/>. Исключение <paramref name="build"/>
    /// (включая ArgumentException пустого batch) делает workflow непригодным: недостроенный batch уже в графе, поэтому
    /// дальнейшие вызовы методов workflow и его шагов, включая SubmitAsync, — InvalidOperationException; описывать граф заново.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="build"/> — null (workflow остаётся пригодным).</exception>
    /// <exception cref="ArgumentException">Batch без членов (в том числе вложенный); workflow непригоден.</exception>
    /// <exception cref="InvalidOperationException">
    /// Область закрыта (вызов у закрытого batch), workflow уже отправлен или непригоден после ошибки построения batch.
    /// </exception>
    IBatchStep StartBatch(Action<IBatchBuilder> build);
}

/// <summary>Граф продолжений и batches (часть 5 §5.1); не потокобезопасный (§5.2), одноразовый: после SubmitAsync не используется.</summary>
public interface IWorkflow : IWorkflowScope
{
    /// <summary>
    /// Продолжения существующей задачи. Задачи нет (или удалена retention'ом, или это id batch) — ParentNotFoundException при
    /// SubmitAsync. В пользовательской транзакции блокирует строку родителя до её коммита (см. WorkflowOptions.Transaction);
    /// если родитель — член batch, CancelMembers этого batch стоит на ней, остальные члены отменяются после освобождения.
    /// </summary>
    INodeStep After(JobId existing);

    /// <summary>
    /// Продолжения существующего batch. OnSuccess к Failed-batch ждёт requeue его Failed-членов: они хранятся, пока существует
    /// строка batch (часть 5 §7.1). Нет batch (или это id задачи) — ParentNotFoundException при SubmitAsync.
    /// </summary>
    INodeStep After(BatchId existing);

    /// <summary>
    /// Пишет граф одной транзакцией. После вызова — успешного или нет — повторный вызов и любые методы workflow и его шагов —
    /// InvalidOperationException. Исключения приходят через возвращённый ValueTask; при любом исключении записи графа откатаны
    /// (в пользовательской транзакции — до точки сохранения, см. WorkflowOptions.Transaction).
    /// </summary>
    /// <exception cref="ArgumentException">Workflow без шагов или After(id) без продолжений.</exception>
    /// <exception cref="UnknownJobException">Динамический шаг (Enqueue / ContinueWith по имени) с задачей, не опубликованной в job_types.</exception>
    /// <exception cref="ParentNotFoundException">Родителя After нет: не создавался, другого вида или удалён retention'ом.</exception>
    /// <exception cref="UniqueKeyConflictException">UniqueKey шага занят активной задачей.</exception>
    /// <exception cref="NotSupportedException">Транзакция WorkflowOptions.Transaction не READ COMMITTED.</exception>
    /// <exception cref="InvalidOperationException">
    /// Повторный вызов; вызов внутри лямбды batch; workflow непригоден после исключения лямбды StartBatch / ContinueWithBatch.
    /// ParentNotFoundException и UniqueKeyConflictException — наследники InvalidOperationException: перехватывать их раньше.
    /// </exception>
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default);
}

public interface IBatchBuilder : IWorkflowScope
{
    /// <summary>
    /// Члены batch по одному на элемент, в порядке списка. Атомарен: исключение (сериализации любого элемента, проверки
    /// <paramref name="options"/>) не добавляет ни одного члена.
    /// </summary>
    IReadOnlyList<IJobStep> EnqueueMany<TArgs>(IReadOnlyList<TArgs> args, StepOptions? options = null);
}

/// <summary>Узел графа: задача, batch или существующий узел (After). Продолжения добавляются в область этого узла.</summary>
public interface INodeStep
{
    IJobStep ContinueWith<TArgs>(TArgs args, ContinuationCondition condition = ContinuationCondition.OnSuccess,
        StepOptions? options = null);

    IJobStep ContinueWith(string jobName, JsonElement args,
        ContinuationCondition condition = ContinuationCondition.OnSuccess, StepOptions? options = null);

    /// <summary>
    /// Batch-продолжение этого узла в его области; члены добавляются только внутри <paramref name="build"/>. Исключение
    /// <paramref name="build"/> (включая ArgumentException пустого batch) делает workflow непригодным: недостроенный batch уже
    /// в графе, поэтому дальнейшие вызовы методов workflow и его шагов, включая SubmitAsync, — InvalidOperationException.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="build"/> — null (workflow остаётся пригодным).</exception>
    /// <exception cref="ArgumentOutOfRangeException">Неизвестное значение <paramref name="condition"/> (workflow остаётся пригодным).</exception>
    /// <exception cref="ArgumentException">Batch без членов (в том числе вложенный); workflow непригоден.</exception>
    /// <exception cref="InvalidOperationException">
    /// Область узла закрыта (член закрытого batch), workflow уже отправлен или непригоден после ошибки построения batch.
    /// </exception>
    IBatchStep ContinueWithBatch(Action<IBatchBuilder> build,
        ContinuationCondition condition = ContinuationCondition.OnSuccess);

    /// <summary>Делегирует workflow: <see cref="IWorkflow.SubmitAsync"/>, те же исключения.</summary>
    /// <inheritdoc cref="IWorkflow.SubmitAsync" path="/exception"/>
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default);
}

public interface IJobStep : INodeStep
{
    JobId Id { get; }
}

public interface IBatchStep : INodeStep
{
    BatchId Id { get; }
}

/// <summary>Id всех задач и batch графа в порядке создания шагов.</summary>
public sealed record WorkflowResult(IReadOnlyList<JobId> Jobs, IReadOnlyList<BatchId> Batches);
