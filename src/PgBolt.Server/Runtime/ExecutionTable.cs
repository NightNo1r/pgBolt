using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace PgBolt.Server.Runtime;

/// <summary>Server execution table keyed by lease_id (§6.2). Reclaiming the same job creates a separate entry.</summary>
internal sealed class ExecutionTable
{
    private readonly ConcurrentDictionary<Guid, Execution> _byLease = new();

    public int Count => _byLease.Count;

    /// <exception cref="InvalidOperationException">An entry with the same lease_id already exists.</exception>
    public void Add(Execution execution)
    {
        ArgumentNullException.ThrowIfNull(execution);
        if (!_byLease.TryAdd(execution.Lease.LeaseId, execution))
            throw new InvalidOperationException($"Lease {execution.Lease.LeaseId} is already registered.");
    }

    /// <summary>Removes this specific object, preserving any other entry with the same lease_id.</summary>
    public bool Remove(Execution execution)
    {
        ArgumentNullException.ThrowIfNull(execution);
        return _byLease.TryRemove(new KeyValuePair<Guid, Execution>(execution.Lease.LeaseId, execution));
    }

    public bool TryGet(Guid leaseId, [MaybeNullWhen(false)] out Execution execution) =>
        _byLease.TryGetValue(leaseId, out execution);

    /// <summary>
    /// Enumerates all entries, including <see cref="ExecutionState.Removed"/>, without copying or locking.
    /// ConcurrentDictionary enumeration avoids the all-stripe locks taken by Count and Values, which block Add and
    /// Remove. Enumeration sees entries present for its entire duration and may see concurrent additions and removals.
    /// </summary>
    public IEnumerable<Execution> Entries()
    {
        foreach (var (_, execution) in _byLease)
            yield return execution;
    }

    /// <summary>Copies all entries except those already marked <see cref="ExecutionState.Removed"/>.</summary>
    public IReadOnlyList<Execution> Snapshot()
    {
        var result = new List<Execution>(_byLease.Count);
        foreach (var (_, execution) in _byLease)
            if (execution.State != ExecutionState.Removed)
                result.Add(execution);
        return result;
    }
}
