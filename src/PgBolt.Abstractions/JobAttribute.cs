namespace PgBolt;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class JobAttribute(string name) : Attribute
{
    public string Name { get; } = name;
    public string Queue { get; init; } = "default";
    public int MaxAttempts { get; init; } = 10;
    public short Priority { get; init; }
    public int[]? RetryDelaysSeconds { get; init; }

    /// <summary>
    /// Таймаут одного выполнения в секундах; 0 — нет таймаута (по умолчанию). Применяется исполняющим сервером
    /// по своему registry, в job_types не публикуется. Отрицательное значение — ошибка генератора BOLT007; значение
    /// больше 4294967 (предел таймеров .NET, 2³² − 2 мс ≈ 49,7 сут) — предупреждение BOLT008, сервер урезает его до предела.
    /// </summary>
    public int TimeoutSeconds { get; init; }
}
