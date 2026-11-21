namespace PgBolt.Benchmarks.Copies;

/// <summary>Alternatives to <see cref="ExecutionTable.Snapshot"/> for comparison with the unchanged copy of the production
/// implementation.</summary>
internal sealed partial class ExecutionTable
{
    /// <summary>
    /// Like <see cref="Snapshot"/>, without <c>new List(_byLease.Count)</c>. ConcurrentDictionary.Count acquires all stripe locks, while enumeration
    /// does not. The trade-off is list growth by doubling.
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
    /// Finds the earliest <c>validUntil − interval</c> among tracked executions (Running with no cancellation reason) by enumeration without a List.
    /// Replaces <c>Snapshot()</c> and the loop in LeaseKeeper.RearmDeadlineTimer. long.MaxValue means nothing is tracked.
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
