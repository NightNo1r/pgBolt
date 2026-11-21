namespace PgBolt;

/// <summary>
/// The After parent never existed, has the wrong kind (job or batch), or was removed by retention. Graph writes have been rolled back.
/// </summary>
public sealed class ParentNotFoundException(Guid parentId)
    : InvalidOperationException($"Workflow parent {parentId} does not exist: it was never created, is of the other kind (job vs batch) " +
                                "or was removed by retention. Nothing of the workflow was written.")
{
    public Guid ParentId { get; } = parentId;
}

/// <summary>
/// An active job holds a workflow step's unique key. ExistingId identifies it unless it has already finished. Graph writes have been rolled
/// back.
/// </summary>
public sealed class UniqueKeyConflictException(string uniqueKey, JobId? existingId)
    : InvalidOperationException($"Unique key '{uniqueKey}' is held by active job {existingId?.ToString() ?? "(already finished)"}; " +
                                "nothing of the workflow was written.")
{
    public string UniqueKey { get; } = uniqueKey;
    public JobId? ExistingId { get; } = existingId;
}
