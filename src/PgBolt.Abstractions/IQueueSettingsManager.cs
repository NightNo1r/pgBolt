namespace PgBolt;

/// <summary>
/// Manages cluster-wide overrides for queue concurrency limits and weights. Setting or resetting one leaves the other in effect. Queue records
/// are retained.
/// </summary>
public interface IQueueSettingsManager
{
    ValueTask<IReadOnlyList<QueueSettings>> ListAsync(CancellationToken ct = default);

    /// <returns>null if the queue has neither code-defined settings nor overrides.</returns>
    ValueTask<QueueSettings?> GetAsync(string queue, CancellationToken ct = default);

    /// <summary>
    /// Creates a queue record if needed. For an already limited queue, the next dequeue honors the new limit or pause. When a queue first gets a
    /// limit, the writing process enforces it immediately. Other processes can dequeue without the limit until they refresh within
    /// <c>QueueSettingsRefreshInterval</c>. Pausing overwrites the previous limit override. Resuming does not restore its value.
    /// </summary>
    /// <param name="queue">Name of the queue to update.</param>
    /// <param name="maxConcurrency">null for unlimited concurrency, 0 to pause, or a positive limit.</param>
    /// <param name="updatedBy">Optional identifier of the caller making the change.</param>
    /// <param name="ct">Token for cancelling the operation.</param>
    ValueTask SetConcurrencyOverrideAsync(string queue, int? maxConcurrency, string? updatedBy = null, CancellationToken ct = default);

    /// <summary>Removes the concurrency override and restores the code-defined limit.</summary>
    ValueTask ResetConcurrencyOverrideAsync(string queue, CancellationToken ct = default);

    /// <summary>
    /// Creates a queue record if needed. Servers pick up the new weight within <c>QueueSettingsRefreshInterval</c>, and the writing process picks it
    /// up immediately. Each applies the weight at its next slot plan boundary.
    /// </summary>
    /// <param name="queue">Name of the queue to update.</param>
    /// <param name="weight">null for no weight, or a weight from 1 through 1000.</param>
    /// <param name="updatedBy">Optional identifier of the caller making the change.</param>
    /// <param name="ct">Token for cancelling the operation.</param>
    ValueTask SetWeightOverrideAsync(string queue, int? weight, string? updatedBy = null, CancellationToken ct = default);

    /// <summary>Removes the weight override and restores the code-defined weight.</summary>
    ValueTask ResetWeightOverrideAsync(string queue, CancellationToken ct = default);

    /// <summary>Clears the cache in this process if caching is enabled.</summary>
    void InvalidateCache();
}
