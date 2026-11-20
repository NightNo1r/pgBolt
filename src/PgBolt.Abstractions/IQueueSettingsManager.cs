namespace PgBolt;

/// <summary>
/// Runtime API переопределений кластерного лимита конкурентности и веса очередей. Переопределение лимита и переопределение веса
/// независимы: запись и сброс одного не трогают другое. Строки очередей не удаляются.
/// </summary>
public interface IQueueSettingsManager
{
    ValueTask<IReadOnlyList<QueueSettings>> ListAsync(CancellationToken ct = default);

    /// <returns>null — у очереди нет ни значений из кода, ни переопределений.</returns>
    ValueTask<QueueSettings?> GetAsync(string queue, CancellationToken ct = default);

    /// <summary>
    /// Для очереди без записи создаёт её. Число и пауза для уже лимитированной очереди действуют со следующего dequeue;
    /// лимит, впервые заданный очереди, серверы других процессов начинают соблюдать в пределах
    /// <c>QueueSettingsRefreshInterval</c> (до этого их dequeue лимит не соблюдает), процесс, выполнивший запись, — сразу.
    /// Пауза затирает прежнее переопределение лимита: возобновление его число не восстанавливает.
    /// </summary>
    /// <param name="maxConcurrency">null — без лимита, 0 — пауза, ≥ 1 — лимит.</param>
    ValueTask SetConcurrencyOverrideAsync(string queue, int? maxConcurrency, string? updatedBy = null, CancellationToken ct = default);

    /// <summary>Снять переопределение лимита: действует значение из кода.</summary>
    ValueTask ResetConcurrencyOverrideAsync(string queue, CancellationToken ct = default);

    /// <summary>
    /// Для очереди без записи создаёт её. Новый вес серверы начинают учитывать в пределах <c>QueueSettingsRefreshInterval</c>
    /// (процесс, выполнивший запись, — сразу), с ближайшей границы плана слотов.
    /// </summary>
    /// <param name="weight">null — без веса, иначе 1…1000.</param>
    ValueTask SetWeightOverrideAsync(string queue, int? weight, string? updatedBy = null, CancellationToken ct = default);

    /// <summary>Снять переопределение веса: действует значение из кода.</summary>
    ValueTask ResetWeightOverrideAsync(string queue, CancellationToken ct = default);

    /// <summary>Сбросить кэш этого процесса; без кэша — ничего не делает.</summary>
    void InvalidateCache();
}
