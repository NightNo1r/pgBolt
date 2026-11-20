namespace PgBolt.Server.Runtime;

/// <summary>Исход выполнения, который отправляет CompletionSink (§5.2, §6.1).</summary>
internal abstract record JobOutcome
{
    /// <summary>Успех — пакетный ack (core §5.3).</summary>
    public sealed record Ack : JobOutcome;

    /// <summary>Повтор через <paramref name="Delay"/> (core §5.4); Kind — Retry или NoExecutor.</summary>
    public sealed record Retry(TimeSpan Delay, JobEventKind Kind, string? Message, string? Exception) : JobOutcome;

    /// <summary>Окончательный fail (core §5.4); Kind — Failed или NoExecutor.</summary>
    public sealed record Fail(JobEventKind Kind, string? Message, string? Exception) : JobOutcome;

    /// <summary>Подтверждение запрошенной отмены (core §5.4, state = 4).</summary>
    public sealed record ConfirmCancel(string? Message) : JobOutcome;

    /// <summary>Возврат без траты попытки (§7.1).</summary>
    public sealed record Release(bool Ran) : JobOutcome;
}
