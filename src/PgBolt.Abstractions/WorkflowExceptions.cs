namespace PgBolt;

/// <summary>Родителя After нет: не создавался, другого вида (задача ↔ batch) или удалён retention'ом. Записи графа откатаны.</summary>
public sealed class ParentNotFoundException(Guid parentId)
    : InvalidOperationException($"Workflow parent {parentId} does not exist: it was never created, is of the other kind (job vs batch) " +
                                "or was removed by retention. Nothing of the workflow was written.")
{
    public Guid ParentId { get; } = parentId;
}

/// <summary>Ключ шага workflow занят активной задачей (ExistingId; null — она успела завершиться). Записи графа откатаны.</summary>
public sealed class UniqueKeyConflictException(string uniqueKey, JobId? existingId)
    : InvalidOperationException($"Unique key '{uniqueKey}' is held by active job {existingId?.ToString() ?? "(already finished)"}; " +
                                "nothing of the workflow was written.")
{
    public string UniqueKey { get; } = uniqueKey;
    public JobId? ExistingId { get; } = existingId;
}
