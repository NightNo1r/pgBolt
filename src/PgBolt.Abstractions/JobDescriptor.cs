namespace PgBolt;

/// <summary>Умолчания задачи из [Job] (или из job_types для динамического enqueue).</summary>
public sealed record JobDescriptor(string Name, string Queue, int MaxAttempts, short Priority, int[]? RetryDelaysSeconds)
{
    /// <summary>Таймаут выполнения в секундах из [Job]; 0 — нет. Init-свойство, чтобы не ломать позиционный конструктор.</summary>
    public int TimeoutSeconds { get; init; }

    /// <summary>Сравнение по значению, включая содержимое RetryDelaysSeconds и TimeoutSeconds.</summary>
    public bool SameAs(JobDescriptor other) =>
        Name == other.Name && Queue == other.Queue && MaxAttempts == other.MaxAttempts && Priority == other.Priority
        && TimeoutSeconds == other.TimeoutSeconds
        && (RetryDelaysSeconds ?? []).AsSpan().SequenceEqual(other.RetryDelaysSeconds ?? [])
        && (RetryDelaysSeconds is null) == (other.RetryDelaysSeconds is null);
}
