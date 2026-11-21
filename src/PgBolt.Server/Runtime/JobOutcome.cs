namespace PgBolt.Server.Runtime;

/// <summary>Execution outcome sent by CompletionSink (§5.2, §6.1).</summary>
internal abstract record JobOutcome
{
    /// <summary>Successful execution acknowledged in a batch (core §5.3).</summary>
    public sealed record Ack : JobOutcome;

    /// <summary>Retry after <paramref name="Delay"/> (core §5.4). Kind is Retry or NoExecutor.</summary>
    public sealed record Retry(TimeSpan Delay, JobEventKind Kind, string? Message, string? Exception) : JobOutcome;

    /// <summary>Terminal failure (core §5.4). Kind is Failed or NoExecutor.</summary>
    public sealed record Fail(JobEventKind Kind, string? Message, string? Exception) : JobOutcome;

    /// <summary>Acknowledges a cancellation request (core §5.4, state = 4).</summary>
    public sealed record ConfirmCancel(string? Message) : JobOutcome;

    /// <summary>Returns the job without consuming an attempt (§7.1).</summary>
    public sealed record Release(bool Ran) : JobOutcome;
}
