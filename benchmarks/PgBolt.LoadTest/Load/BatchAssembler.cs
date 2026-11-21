namespace PgBolt.LoadTest.Load;

/// <summary>Batch key. One call to <c>EnqueueManyAsync</c> uses one queue and one delay.</summary>
internal readonly record struct BatchKey(string Queue, bool Delayed);

internal sealed record PendingBatch(BatchKey Key, IReadOnlyList<JobDraft> Jobs, long FirstAddedMicros);

/// <summary>
/// Groups jobs into batches by key. A batch is ready at <paramref name="maxSize"/> jobs or <paramref name="maxAgeMicros"/> after its first job.
/// Designed for a single generator thread.
/// </summary>
internal sealed class BatchAssembler(int maxSize = 100, long maxAgeMicros = 10_000)
{
    private readonly Dictionary<BatchKey, (List<JobDraft> Jobs, long FirstAddedMicros)> _open = [];

    /// <summary>Adds a job and returns its batch if full.</summary>
    public PendingBatch? Add(JobDraft job, long nowMicros)
    {
        var key = new BatchKey(job.Queue, job.Delayed);
        if (!_open.TryGetValue(key, out var open))
            _open[key] = open = ([], nowMicros);

        open.Jobs.Add(job);
        return open.Jobs.Count >= maxSize ? Close(key) : null;
    }

    /// <summary>Takes batches that have waited at least <c>maxAgeMicros</c>.</summary>
    public IReadOnlyList<PendingBatch> TakeAged(long nowMicros) =>
        Take(open => nowMicros - open.FirstAddedMicros >= maxAgeMicros);

    /// <summary>Takes all partially assembled batches.</summary>
    public IReadOnlyList<PendingBatch> TakeAll() => Take(_ => true);

    private List<PendingBatch> Take(Func<(List<JobDraft> Jobs, long FirstAddedMicros), bool> ready) =>
        [.. _open.Where(pair => ready(pair.Value)).Select(pair => pair.Key).ToList().Select(Close)];

    private PendingBatch Close(BatchKey key)
    {
        var (jobs, firstAddedMicros) = _open[key];
        _open.Remove(key);
        return new PendingBatch(key, jobs, firstAddedMicros);
    }
}
