namespace PgBolt.Benchmarks.Copies;

/// <summary>Общий контракт вариантов CompletionSink (ack-путь), чтобы бенчмарк и проверка работали с любым.</summary>
internal interface ICompletionSink
{
    void Start();

    bool Complete(Execution execution, JobOutcome outcome);

    Task StopAsync(CancellationToken ct);
}

/// <summary>
/// Заглушка отправки (вместо SendAckBatchAsync/SendSingleAsync и IServerStore): переходы OutcomePending → OutcomeInFlight →
/// Removed и удаление из таблицы, как при успешном ack, без БД. Считает отправленные записи для проверки.
/// </summary>
internal sealed class AckSendStub(ExecutionTable table)
{
    private long _sent;
    private long _batches;

    public long Sent => Interlocked.Read(ref _sent);

    public long Batches => Interlocked.Read(ref _batches);

    /// <summary>Вызывается для каждой отправленной записи (проверка: ровно один раз, до конца StopAsync).</summary>
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
