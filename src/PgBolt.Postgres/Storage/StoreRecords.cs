namespace PgBolt.Postgres.Storage;

/// <summary>Пакет задач одного типа (§5.1): поэлементные массивы + общие скаляры.</summary>
public sealed record EnqueueBatch(
    string Type,
    string Queue,
    short Priority,
    int MaxAttempts,
    int[]? RetryDelaysSeconds,
    string? TraceParent,
    TimeSpan Delay,
    Guid[] Ids,
    string[] ArgsJson,
    DateTime?[] RunAtUtc,
    string?[] UniqueKeys)
{
    /// <summary>Часть 4: момент срабатывания по строкам (только вместе с RecurringIds); null — now() БД.</summary>
    public DateTime?[]? ScheduledAtUtc { get; init; }

    /// <summary>
    /// Часть 4: id расписания по строкам. null — задачи не из расписания (прежний SQL клиента); иначе — внутренний вариант
    /// enqueue с колонками recurring_id / scheduled_at (тик, Trigger; §8).
    /// </summary>
    public string[]? RecurringIds { get; init; }

    // Поля графа (часть 5) задаёт только планировщик workflow (internal init): Awaiting-строка без подписки, notify и учёта
    // total нарушила бы инварианты §4.

    /// <summary>Часть 5 §6.1 п. 2: родитель (задача или batch; у кода 5 — свой batch) по строкам. Только вместе с остальными массивами графа.</summary>
    public Guid?[]? ParentIds { get; internal init; }

    /// <summary>Часть 5: jobs.continuation = queue.await_condition (1–5) по строкам; не NULL — строка пишется Awaiting.</summary>
    public short?[]? Continuations { get; internal init; }

    /// <summary>Часть 5: queue.notify = jobs.notify по строкам (член batch или есть дети в графе).</summary>
    public bool[]? Notify { get; internal init; }

    /// <summary>Часть 5: batch члена по строкам (в queue и jobs).</summary>
    public Guid?[]? BatchIds { get; internal init; }

    internal void Validate()
    {
        if (Ids.Length == 0)
            throw new ArgumentException("Batch is empty.");
        if (ArgsJson.Length != Ids.Length || RunAtUtc.Length != Ids.Length || UniqueKeys.Length != Ids.Length)
            throw new ArgumentException("Batch arrays must have equal length.");
        if (Delay < TimeSpan.Zero)
            throw new ArgumentException("Delay must be non-negative.");
        if (MaxAttempts < 1)
            throw new ArgumentException("MaxAttempts must be >= 1.");
        foreach (var runAt in RunAtUtc)
            if (runAt is { Kind: not DateTimeKind.Utc })
                throw new ArgumentException("RunAtUtc values must have DateTimeKind.Utc.");
        if (ScheduledAtUtc is { } scheduled && scheduled.Length != Ids.Length || RecurringIds is { } recurring && recurring.Length != Ids.Length)
            throw new ArgumentException("Batch arrays must have equal length.");
        if (ScheduledAtUtc is not null && RecurringIds is null)
            throw new ArgumentException("ScheduledAtUtc requires RecurringIds.");
        if (RecurringIds is not null)
            foreach (var id in RecurringIds)
                if (string.IsNullOrEmpty(id))
                    throw new ArgumentException("RecurringIds values must be non-empty.");
        if (ScheduledAtUtc is not null)
            foreach (var at in ScheduledAtUtc)
                if (at is { Kind: not DateTimeKind.Utc })
                    throw new ArgumentException("ScheduledAtUtc values must have DateTimeKind.Utc.");
        if (ParentIds is not null || Continuations is not null || Notify is not null || BatchIds is not null)
        {
            if (ParentIds is null || Continuations is null || Notify is null || BatchIds is null)
                throw new ArgumentException("Workflow arrays (ParentIds, Continuations, Notify, BatchIds) must be set together.");
            if (ParentIds.Length != Ids.Length || Continuations.Length != Ids.Length || Notify.Length != Ids.Length
                || BatchIds.Length != Ids.Length)
                throw new ArgumentException("Batch arrays must have equal length.");
            if (RecurringIds is not null)
                throw new ArgumentException("Workflow and recurring columns are mutually exclusive.");
        }
    }
}

public readonly record struct EnqueueRowResult(Guid? Id, bool Created);

public sealed record LeasedJob(
    Guid Id,
    Guid LeaseId,
    string Type,
    string ArgsJson,
    int Attempt,
    int MaxAttempts,
    int[]? RetryDelaysSeconds,
    Guid? ParentId,
    Guid? BatchId,
    string? TraceParent,
    string? RecurringId = null,
    DateTime? ScheduledAtUtc = null,
    short? Continuation = null,
    double WaitSeconds = 0);

/// <summary>Результат dequeue очереди с кластерным лимитом: LimitReached — мест в лимите после него не осталось.</summary>
public sealed record LimitedDequeueResult(IReadOnlyList<LeasedJob> Jobs, bool LimitReached);

public readonly record struct LeaseRef(Guid JobId, Guid LeaseId);

public readonly record struct HeartbeatLease(Guid JobId, Guid LeaseId, bool CancelRequested);

/// <summary>Ответ heartbeat (часть 2 §9.2): обновлена ли строка servers и живые lease из списка.</summary>
public sealed record HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases);

public enum FailOutcome { LeaseLost, Retried, Cancelled, Failed }

/// <summary>Исход release (часть 3 §7.1): возврат без траты попытки, финализация отмены или потерянный lease.</summary>
public enum ReleaseOutcome { LeaseLost, Released, Cancelled }

public sealed record PromoteResult(int Promoted, IReadOnlyCollection<string> Queues);

public sealed record JobTypeRecord(
    string Name,
    string ArgsSchemaJson,
    string DefaultQueue,
    int DefaultMaxAttempts,
    short DefaultPriority,
    int[]? DefaultRetryDelaysSeconds);

/// <summary>Строка сброса счётчиков исходов (часть 6 §4.1): бакет — начало минуты UTC.</summary>
internal readonly record struct OutcomeStatsRow(DateTime BucketUtc, Guid ServerId, string Queue, int Succeeded, int Failed, int Retried);
