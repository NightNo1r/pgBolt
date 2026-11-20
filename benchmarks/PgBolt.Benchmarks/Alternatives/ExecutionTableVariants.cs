namespace PgBolt.Benchmarks.Copies;

/// <summary>Альтернативы <see cref="ExecutionTable.Snapshot"/> для сравнения (продуктовый Snapshot — в копии без изменений).</summary>
internal sealed partial class ExecutionTable
{
    /// <summary>
    /// Как <see cref="Snapshot"/>, но без <c>new List(_byLease.Count)</c>: Count у ConcurrentDictionary захватывает все
    /// полосы (lock'и) словаря, а перечисление — нет. Цена — рост List удвоением.
    /// </summary>
    public IReadOnlyList<Execution> SnapshotWithoutCount()
    {
        var result = new List<Execution>();
        foreach (var (_, execution) in _byLease)
            if (execution.State != ExecutionState.Removed)
                result.Add(execution);
        return result;
    }

    /// <summary>
    /// Ближайший рубеж <c>validUntil − interval</c> среди отслеживаемых (Running без причины отмены) прямо по перечислению,
    /// без List — замена <c>Snapshot()</c> + цикла в LeaseKeeper.RearmDeadlineTimer. long.MaxValue — следить не за кем.
    /// </summary>
    public long MinWatchedDeadline(MonoClock clock, TimeSpan interval)
    {
        var next = long.MaxValue;
        foreach (var (_, execution) in _byLease)
            if (execution.State == ExecutionState.Running && execution.FirstReason == CancelReason.None)
                next = Math.Min(next, clock.Add(execution.ValidUntil, -interval));
        return next;
    }
}
