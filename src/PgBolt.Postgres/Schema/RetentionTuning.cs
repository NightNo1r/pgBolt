namespace PgBolt.Postgres.Schema;

/// <summary>
/// Точки тестовых хуков retention (§12): после списка, предпроверки, pg_locks, прогрева, пакетов А/Б и COMMIT транзакции 1
/// и после выбора и блокировки кандидатов удаления строк batches (часть 5 §7.1); PrecheckOidChecked — между первым
/// (сверка oid, (а)) и вторым (queue и (б) одним statement'ом) пакетами предпроверки партиции jobs; BatchesWindowed — между
/// окном кандидатов удаления строк batches и их блокировкой.
/// </summary>
internal enum RetentionStep
{
    Listed, Prechecked, LocksChecked, WarmedUp, BatchA, BatchB, Committed, BatchesLocked, PrecheckOidChecked, BatchesWindowed,
}

/// <param name="Parent">"jobs", "job_events" или "" (Listed).</param>
/// <param name="Days">Сутки партиций шага (группы, компактируемой партиции или всех предпроверенных).</param>
internal sealed record RetentionHookContext(RetentionStep Step, string Parent, IReadOnlyList<DateOnly> Days);

/// <summary>
/// Внутренние параметры retention (§5.3): K — партиций в группе отсоединения, потолки удержания блокировки родителя.
/// Тесты создают свой экземпляр — глобального изменяемого состояния нет.
/// </summary>
internal sealed record RetentionTuning(int PartitionsPerTransaction, TimeSpan HoldStatementTimeout, TimeSpan HoldIdleTimeout)
{
    public static RetentionTuning Default { get; } = new(10, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));

    /// <summary>Только для тестов: вызывается в точках <see cref="RetentionStep"/>; исключение прерывает запуск.</summary>
    public Func<RetentionHookContext, CancellationToken, Task>? Hook { get; init; }

    /// <summary>Только для тестов: команда, добавляемая первой в пакет Б (например, pg_sleep для statement_timeout).</summary>
    public string? BatchBTestPrefix { get; init; }
}
