using System.Text.Json;

namespace PgBolt;

/// <summary>
/// Runtime API расписаний (часть 4 §4.2). Строки code-first владельцев (Owner != null) через API только
/// приостанавливаются, возобновляются, запускаются и читаются; AddOrUpdate и Remove для них — InvalidOperationException.
/// </summary>
public interface IRecurringJobManager
{
    /// <remarks>Метаданные TArgs (JobMetadata&lt;TArgs&gt;) генератор регистрирует по месту этого вызова, как для EnqueueAsync.</remarks>
    ValueTask AddOrUpdateAsync<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null, CancellationToken ct = default);

    ValueTask AddOrUpdateAsync(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null,
        CancellationToken ct = default);

    /// <returns>false — расписания нет.</returns>
    ValueTask<bool> RemoveAsync(string id, CancellationToken ct = default);

    ValueTask<bool> PauseAsync(string id, CancellationToken ct = default);

    /// <summary>Пропущенное за паузу обрабатывается misfire-политикой расписания.</summary>
    ValueTask<bool> ResumeAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Ставит задачу сейчас, курсор не сдвигает; null — расписания нет. Тип, которого нет в job_types, — UnknownJobException;
    /// retry_delays с NULL-элементами (строка записана SQL'ем в обход API) — InvalidOperationException.
    /// </summary>
    ValueTask<EnqueueResult?> TriggerAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Строка целиком; null — расписания нет. Строка, записанная SQL'ем в обход API со значением, которое не представимо в
    /// RecurringJobInfo (catch_up_window с месяцами или вне TimeSpan, NULL-элементы retry_delays), —
    /// InvalidOperationException с id, полем и способом исправления.
    /// </summary>
    ValueTask<RecurringJobInfo?> GetAsync(string id, CancellationToken ct = default);
}
