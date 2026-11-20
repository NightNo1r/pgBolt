// Копия: src/PgBolt.Server/Runtime/ExecutionTable.cs @ 52b4a8f (текущая).
// Изменены только пространство имён и имя типа и добавлен модификатор partial (варианты Snapshot — в Alternatives/ExecutionTableVariants.cs); код синхронизации — дословно.

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace PgBolt.Benchmarks.Copies;

/// <summary>Таблица выполнений сервера по lease_id (§6.2): одна задача, взятая повторно, — две разные записи.</summary>
internal sealed partial class ExecutionTable
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
