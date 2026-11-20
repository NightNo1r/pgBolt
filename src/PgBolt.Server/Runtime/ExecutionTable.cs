using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace PgBolt.Server.Runtime;

/// <summary>Таблица выполнений сервера по lease_id (§6.2): одна задача, взятая повторно, — две разные записи.</summary>
internal sealed class ExecutionTable
{
    private readonly ConcurrentDictionary<Guid, Execution> _byLease = new();

    public int Count => _byLease.Count;

    /// <exception cref="InvalidOperationException">Запись с таким lease_id уже есть.</exception>
    public void Add(Execution execution)
    {
        ArgumentNullException.ThrowIfNull(execution);
        if (!_byLease.TryAdd(execution.Lease.LeaseId, execution))
            throw new InvalidOperationException($"Lease {execution.Lease.LeaseId} is already registered.");
    }

    /// <summary>Удаляет именно этот объект (не другую запись с тем же lease_id).</summary>
    public bool Remove(Execution execution)
    {
        ArgumentNullException.ThrowIfNull(execution);
        return _byLease.TryRemove(new KeyValuePair<Guid, Execution>(execution.Lease.LeaseId, execution));
    }

    public bool TryGet(Guid leaseId, [MaybeNullWhen(false)] out Execution execution) =>
        _byLease.TryGetValue(leaseId, out execution);

    /// <summary>
    /// Все записи (в том числе <see cref="ExecutionState.Removed"/>) без копии и без блокировок: перечисление
    /// ConcurrentDictionary их не берёт, в отличие от Count/Values, захватывающих все полосы словаря и
    /// останавливающих Add/Remove. Видит записи, существовавшие всё время перечисления; одновременные Add/Remove — может.
    /// </summary>
    public IEnumerable<Execution> Entries()
    {
        foreach (var (_, execution) in _byLease)
            yield return execution;
    }

    /// <summary>Копия записей, кроме уже <see cref="ExecutionState.Removed"/>.</summary>
    public IReadOnlyList<Execution> Snapshot()
    {
        var result = new List<Execution>(_byLease.Count);
        foreach (var (_, execution) in _byLease)
            if (execution.State != ExecutionState.Removed)
                result.Add(execution);
        return result;
    }
}
