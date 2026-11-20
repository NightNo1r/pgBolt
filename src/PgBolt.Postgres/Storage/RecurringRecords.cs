namespace PgBolt.Postgres.Storage;

internal enum RecurringUpsertResult { Inserted, Updated, OwnedByCodeFirst }

internal enum RecurringRemoveResult { NotFound, Removed, OwnedByCodeFirst }

/// <summary>Строка расписания с умолчаниями job_types для Trigger (часть 4 §4.2); UnknownType — типа нет в job_types.</summary>
internal sealed record RecurringTriggerRow(
    string Type,
    string ArgsJson,
    string? Queue,
    short? Priority,
    int? MaxAttempts,
    int[]? RetryDelaysSeconds,
    OverlapPolicy Overlap,
    bool UnknownType);

/// <summary>Ошибка строки (часть 4 §5.2 п. 2): строка не поставлена, курсор стоит, wake_at = db_now + 1 мин.</summary>
internal sealed record RecurringRowError(string Id, string Error);

/// <summary>Срабатывания, отброшенные misfire-политикой при сдвиге курсора (Warning §7): CatchUp — [FromUtc, ToUtc), Skip — [FromUtc, ToUtc] (как MissedRange).</summary>
internal sealed record RecurringMissed(string Id, DateTime FromUtc, DateTime ToUtc);

/// <summary>
/// Итог транзакции тика. Full — захвачено batchSize строк или исчерпан лимит срабатываний: цикл повторяет тик сразу.
/// OverlapSkipped — FireOnce/Skip пропущены из-за живого запуска; Waiting — CatchUp + Skip ждут его завершения;
/// Exhausted — у cron больше нет срабатываний, строка больше не сработает.
/// </summary>
internal sealed record RecurringTickResult(
    int Claimed,
    int Fired,
    bool Full,
    IReadOnlyCollection<string> Queues,
    IReadOnlyList<RecurringRowError> Errors,
    IReadOnlyList<RecurringMissed> Missed,
    IReadOnlyList<string> OverlapSkipped,
    IReadOnlyList<string> Waiting,
    IReadOnlyList<string> Exhausted)
{
    public static readonly RecurringTickResult Empty = new(0, 0, false, [], [], [], [], [], []);
}

internal sealed record RecurringFieldChange(string Id, IReadOnlyList<string> Fields);

/// <summary>Записи синхронизации владельца (часть 4 §6.2); Collisions — id занят другим владельцем или API, строка не тронута.</summary>
internal sealed record RecurringSyncResult(
    IReadOnlyList<string> Inserted,
    IReadOnlyList<RecurringFieldChange> Updated,
    IReadOnlyList<string> Deleted,
    IReadOnlyList<string> Collisions)
{
    public int Writes => Inserted.Count + Updated.Count + Deleted.Count;
}
