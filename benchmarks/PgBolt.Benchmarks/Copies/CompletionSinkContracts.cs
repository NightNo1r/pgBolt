namespace PgBolt.Benchmarks.Copies;

/// <summary>Shared acknowledgment contract for CompletionSink variants used by the benchmarks and verifier.</summary>
internal interface ICompletionSink
{
    void Start();

    bool Complete(Execution execution, JobOutcome outcome);

    Task StopAsync(CancellationToken ct);
}

/// <summary>
/// Stub for SendAckBatchAsync, SendSingleAsync, and IServerStore. Transitions OutcomePending to OutcomeInFlight to Removed and removes the entry
/// as a successful ack would, without a database. Counts sends for verification.
/// </summary>
internal sealed class AckSendStub(ExecutionTable table)
{
    private long _sent;
    private long _batches;

    public long Sent => Interlocked.Read(ref _sent);

    public long Batches => Interlocked.Read(ref _batches);

    /// <summary>Called for every sent entry. Verification requires exactly one call per entry before StopAsync completes.</summary>
    public Action<Execution>? OnSent { get; set; }

    public Task SendAckBatchAsync(List<Execution> batch)
    {
        var sent = 0;
        foreach (var execution in batch)
        {
            if (!execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight))
                continue;                                   // как BeginAttempt: запись уже снята
            if (execution.TryTransition(ExecutionState.OutcomeInFlight, ExecutionState.Removed))
                table.Remove(execution);
            OnSent?.Invoke(execution);
            sent++;
        }
        Interlocked.Add(ref _sent, sent);
        Interlocked.Increment(ref _batches);
        return Task.CompletedTask;
    }

    public Task SendSingleAsync(Execution execution) => SendAckBatchAsync([execution]);
}
