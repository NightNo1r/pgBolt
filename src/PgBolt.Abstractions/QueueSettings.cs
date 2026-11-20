namespace PgBolt;

/// <summary>
/// Настройки очереди: значения из кода и переопределения лимита и веса. Строки в БД нет — эффективных лимита и веса тоже нет.
/// </summary>
public sealed record QueueSettings(
    string Queue,
    int? DefaultMaxConcurrency,
    int? DefaultWeight,
    string? DefaultPublishedBy,
    bool OverrideConcurrencySet,
    int? OverrideMaxConcurrency,
    string? OverrideConcurrencyUpdatedBy,
    DateTimeOffset? OverrideConcurrencyUpdatedAt,
    bool OverrideWeightSet,
    int? OverrideWeight,
    string? OverrideWeightUpdatedBy,
    DateTimeOffset? OverrideWeightUpdatedAt)
{
    /// <summary>Переопределение, если оно задано (в том числе «без лимита»), иначе значение из кода; null — без лимита.</summary>
    public int? EffectiveMaxConcurrency => OverrideConcurrencySet ? OverrideMaxConcurrency : DefaultMaxConcurrency;

    /// <summary>Эффективный лимит равен 0.</summary>
    public bool Paused => EffectiveMaxConcurrency == 0;

    /// <summary>Переопределение, если оно задано (в том числе «без веса»), иначе значение из кода; null — без веса.</summary>
    public int? EffectiveWeight => OverrideWeightSet ? OverrideWeight : DefaultWeight;
}
