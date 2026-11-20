namespace PgBolt.Postgres.Schema;

/// <summary>Результат запуска retention (часть 2 §5.1). Core не пишет логов: предупреждения строит вызывающий по этому результату.</summary>
/// <param name="DroppedJobsPartitions">Сутки удалённых целиком партиций jobs, включая отсоединённые прошлыми запусками.</param>
/// <param name="DroppedEventsPartitions">Сутки удалённых партиций job_events, включая отсоединённые прошлыми запусками.</param>
/// <param name="DroppedCompactionRemnants">Удалённые остатки компактирования прошлых запусков (без остатков этого запуска).</param>
/// <param name="Pinned">Партиции jobs, закреплённые живыми задачами (не больше RetentionCompactionMaxLiveJobs).</param>
/// <param name="Deferred">Отложенные партиции и таблицы.</param>
/// <param name="AttachedJobsPartitions">Присоединённых партиций jobs после запуска (§5.3, «Число партиций»).</param>
/// <param name="DeletedBatches">Удалено строк batches старше JobsRetention (часть 5 §7.1).</param>
public sealed record RetentionResult(
    IReadOnlyList<DateOnly> DroppedJobsPartitions,
    IReadOnlyList<DateOnly> DroppedEventsPartitions,
    int DroppedCompactionRemnants,
    IReadOnlyList<PinnedPartition> Pinned,
    IReadOnlyList<DeferredPartition> Deferred,
    int AttachedJobsPartitions,
    int DeletedBatches = 0);

/// <param name="OverdueDays">На сколько суток кандидат просрочен (0 — стал кандидатом в текущие сутки).</param>
/// <param name="LiveJobs">Число живых задач по предпроверке.</param>
/// <param name="Compacted">true — сжата в этом запуске; false — терминальных строк меньше max(LiveJobs, 100).</param>
public sealed record PinnedPartition(DateOnly Day, int OverdueDays, int LiveJobs, bool Compacted);

/// <param name="Table">"jobs" или "job_events".</param>
/// <param name="LiveJobs">Только для LiveJobs и Replicated: живых задач по предпроверке с LIMIT T + 1 (T + 1 — «больше T»).</param>
/// <param name="Error">Текст исключения — только для DropFailed.</param>
public sealed record DeferredPartition(string Table, DateOnly Day, DeferReason Reason, int OverdueDays, int? LiveJobs,
    string? Error = null);

/// <summary>Причины отложения (часть 2 §5.1).</summary>
public enum DeferReason
{
    /// <summary>Блокировка не получена за RetentionLockTimeout; партиция не изменилась.</summary>
    LockTimeout,
    /// <summary>Под блокировкой родителя сработал statement_timeout или idle_in_transaction_session_timeout; откат.</summary>
    HoldTimeout,
    /// <summary>Таблица отсоединена, DROP не получил блокировку; удалится следующим запуском как остаток.</summary>
    DropLockTimeout,
    /// <summary>DROP отсоединённой таблицы упал с другой ошибкой; удалится следующим запуском как остаток.</summary>
    DropFailed,
    /// <summary>Живых задач больше RetentionCompactionMaxLiveJobs (или компактирование выключено).</summary>
    LiveJobs,
    /// <summary>jobs входит в публикацию логической репликации; компактирование пропущено.</summary>
    Replicated,
}
