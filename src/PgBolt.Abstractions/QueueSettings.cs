namespace PgBolt;

/// <summary>
/// Code-defined settings and overrides for a queue's concurrency limit and weight. A queue without a database record has neither an effective
/// limit nor a weight.
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
    /// <summary>
    /// The concurrency override if set, including an explicit unlimited override. Otherwise uses the code-defined limit. Null means unlimited.
    /// </summary>
    public int? EffectiveMaxConcurrency => OverrideConcurrencySet ? OverrideMaxConcurrency : DefaultMaxConcurrency;

    /// <summary>True when the effective concurrency limit is zero.</summary>
    public bool Paused => EffectiveMaxConcurrency == 0;

    /// <summary>
    /// The weight override if set, including an explicit override that removes the weight. Otherwise uses the code-defined weight. Null means no
    /// weight.
    /// </summary>
    public int? EffectiveWeight => OverrideWeightSet ? OverrideWeight : DefaultWeight;
}
